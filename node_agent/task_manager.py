from __future__ import annotations

import asyncio
import heapq
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Dict, Optional

from node_agent.executor import ExecutorEngine
from node_agent.models import TaskEvent, TaskRequest, TaskRuntime, TaskStatus
from node_agent.scheduler import ResourceRequest, ResourceScheduler, ResourceUsage, ScheduleInput


@dataclass(order=True, slots=True)
class _QueuedTask:
    """等待调度的任务对象。"""

    sort_index: tuple[int, int]
    request: TaskRequest = field(compare=False)
    runtime: TaskRuntime = field(compare=False)
    on_event: Callable[[TaskEvent], None] = field(compare=False)
    result_future: asyncio.Future[TaskStatus] = field(compare=False)
    deadline: Optional[float] = field(compare=False, default=None)


class TaskManager:
    """任务管理器，负责接收任务、调度、执行、取消。"""

    TERMINAL_STATUSES = {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELED}

    def __init__(
        self,
        scheduler: ResourceScheduler,
        executor: Optional[ExecutorEngine] = None,
        max_concurrency: int = 2,
        retain_history: bool = False,
        history_limit: Optional[int] = None,
    ):
        self.scheduler = scheduler
        self.executor = executor or ExecutorEngine()
        self.max_concurrency = max_concurrency
        self.tasks: Dict[str, TaskRuntime] = {}
        self.task_history: Dict[str, list[TaskRuntime]] = {}
        self.retain_history = retain_history
        self.history_limit = history_limit

        self._running_count = 0
        self._usage = ResourceUsage()
        self._pending: list[_QueuedTask] = []
        self._queue_seq = 0
        self._cond = asyncio.Condition()
        self._dispatcher_task: Optional[asyncio.Task[None]] = None

    def _is_terminal(self, status: TaskStatus) -> bool:
        return status in self.TERMINAL_STATUSES

    def _archive_runtime(self, runtime: TaskRuntime) -> None:
        if not self.retain_history:
            return

        history = self.task_history.setdefault(runtime.request.task_id, [])
        history.append(runtime)
        if self.history_limit is not None and self.history_limit >= 0:
            self.task_history[runtime.request.task_id] = history[-self.history_limit :]

    def _ensure_dispatcher(self) -> None:
        if self._dispatcher_task and not self._dispatcher_task.done():
            return
        self._dispatcher_task = asyncio.create_task(self._dispatch_loop())

    def _to_resource_request(self, request: TaskRequest) -> ResourceRequest:
        return ResourceRequest(
            cpu_cores=max(1, int(request.resource_request.get("cpu_cores", 1))),
            memory_mb=max(1, int(request.resource_request.get("memory_mb", 256))),
            gpu_vram_mb=max(0, int(request.resource_request.get("gpu_vram_mb", 0))),
        )

    async def _dispatch_loop(self) -> None:
        while True:
            async with self._cond:
                scheduled = await self._pick_schedulable_task()
                if not scheduled:
                    if not self._pending:
                        return
                    await self._cond.wait()
                    continue

            queued, resources, device = scheduled
            result = await self._run_one(queued, resources, device)
            if not queued.result_future.done():
                queued.result_future.set_result(result)

    async def _pick_schedulable_task(self) -> tuple[_QueuedTask, ResourceRequest, str] | None:
        now = asyncio.get_running_loop().time()

        # 先处理超时，避免无法调度的任务无限等待。
        expired_indexes: list[int] = []
        for index, item in enumerate(self._pending):
            if item.deadline is not None and now >= item.deadline:
                expired_indexes.append(index)

        for index in reversed(expired_indexes):
            item = self._pending[index]
            self._pending[index] = self._pending[-1]
            self._pending.pop()
            if index < len(self._pending):
                heapq._siftup(self._pending, index)
                heapq._siftdown(self._pending, 0, index)
            item.runtime.status = TaskStatus.FAILED
            item.runtime.ended_at = datetime.now(timezone.utc)
            item.on_event(
                TaskEvent(task_id=item.request.task_id, event_type="failed", payload={"reason": "任务等待超时"})
            )
            if not item.result_future.done():
                item.result_future.set_result(TaskStatus.FAILED)

        if self._running_count >= self.max_concurrency:
            return None

        # 扫描优先队列，选取优先级最高且当前可执行的任务。
        candidates = sorted(self._pending)
        for candidate in candidates:
            request_resources = self._to_resource_request(candidate.request)
            schedule_input = ScheduleInput(
                require_gpu=candidate.request.require_gpu,
                prefer_gpu=candidate.request.prefer_gpu,
                priority=candidate.request.priority,
                timeout_sec=candidate.request.timeout_sec,
                resource_request=request_resources,
            )
            decision = self.scheduler.decide(schedule_input, self._usage)
            if not decision.accepted:
                continue

            self._pending.remove(candidate)
            heapq.heapify(self._pending)
            return candidate, request_resources, decision.device

        return None

    async def _run_one(self, queued: _QueuedTask, resources: ResourceRequest, device: str) -> TaskStatus:
        self._running_count += 1
        self._usage.used_cpu_cores += resources.cpu_cores
        self._usage.used_memory_mb += resources.memory_mb
        if device == "gpu":
            self._usage.used_gpu_vram_mb += max(self.scheduler.gpu_vram_threshold_mb, resources.gpu_vram_mb)

        queued.runtime.status = TaskStatus.RUNNING
        queued.on_event(TaskEvent(task_id=queued.request.task_id, event_type="running", payload={"device": device}))

        try:
            if queued.request.timeout_sec and queued.request.timeout_sec > 0:
                result = await asyncio.wait_for(
                    self.executor.run_task(queued.request, queued.on_event),
                    timeout=queued.request.timeout_sec,
                )
            else:
                result = await self.executor.run_task(queued.request, queued.on_event)
            queued.runtime.status = result
            return result
        except asyncio.TimeoutError:
            await self.executor.cancel(queued.request.task_id)
            queued.runtime.status = TaskStatus.FAILED
            queued.on_event(
                TaskEvent(task_id=queued.request.task_id, event_type="failed", payload={"reason": "任务执行超时"})
            )
            return TaskStatus.FAILED
        finally:
            queued.runtime.ended_at = datetime.now(timezone.utc)
            self._running_count = max(0, self._running_count - 1)
            self._usage.used_cpu_cores = max(0, self._usage.used_cpu_cores - resources.cpu_cores)
            self._usage.used_memory_mb = max(0, self._usage.used_memory_mb - resources.memory_mb)
            if device == "gpu":
                self._usage.used_gpu_vram_mb = max(
                    0,
                    self._usage.used_gpu_vram_mb - max(self.scheduler.gpu_vram_threshold_mb, resources.gpu_vram_mb),
                )

            async with self._cond:
                self._cond.notify_all()

    async def submit(self, request: TaskRequest, on_event) -> TaskStatus:
        """提交任务到优先队列，等待调度结果。"""
        existing_runtime = self.tasks.get(request.task_id)
        if existing_runtime and not self._is_terminal(existing_runtime.status):
            on_event(TaskEvent(task_id=request.task_id, event_type="rejected", payload={"reason": "task_id 已存在"}))
            return TaskStatus.FAILED

        if existing_runtime and self._is_terminal(existing_runtime.status):
            self._archive_runtime(existing_runtime)

        runtime = TaskRuntime(request=request)
        self.tasks[request.task_id] = runtime

        loop = asyncio.get_running_loop()
        future: asyncio.Future[TaskStatus] = loop.create_future()
        deadline = loop.time() + request.timeout_sec if request.timeout_sec and request.timeout_sec > 0 else None

        queued = _QueuedTask(
            sort_index=(-request.priority, self._queue_seq),
            request=request,
            runtime=runtime,
            on_event=on_event,
            result_future=future,
            deadline=deadline,
        )
        self._queue_seq += 1

        on_event(
            TaskEvent(
                task_id=request.task_id,
                event_type="queued",
                payload={"priority": request.priority, "resource_request": request.resource_request},
            )
        )

        async with self._cond:
            heapq.heappush(self._pending, queued)
            self._ensure_dispatcher()
            self._cond.notify_all()

        return await future

    async def cancel(self, task_id: str, on_event) -> bool:
        """取消任务并更新状态。"""
        runtime = self.tasks.get(task_id)
        if not runtime:
            on_event(TaskEvent(task_id=task_id, event_type="cancel_failed", payload={"reason": "任务不存在"}))
            return False

        async with self._cond:
            for index, item in enumerate(self._pending):
                if item.request.task_id != task_id:
                    continue

                self._pending[index] = self._pending[-1]
                self._pending.pop()
                if index < len(self._pending):
                    heapq._siftup(self._pending, index)
                    heapq._siftdown(self._pending, 0, index)
                runtime.status = TaskStatus.CANCELED
                runtime.ended_at = datetime.now(timezone.utc)
                item.result_future.set_result(TaskStatus.CANCELED)
                on_event(TaskEvent(task_id=task_id, event_type="canceled", payload={}))
                return True

        canceled = await self.executor.cancel(task_id)
        if canceled:
            runtime.status = TaskStatus.CANCELED
            runtime.ended_at = datetime.now(timezone.utc)
            on_event(TaskEvent(task_id=task_id, event_type="canceled", payload={}))
            return True

        on_event(TaskEvent(task_id=task_id, event_type="cancel_failed", payload={"reason": "任务未在运行"}))
        return False
