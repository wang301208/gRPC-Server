from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Dict, Optional

from node_agent.executor import ExecutorEngine
from node_agent.models import TaskEvent, TaskRequest, TaskRuntime, TaskStatus
from node_agent.scheduler import ResourceScheduler


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
        self.tasks: Dict[str, TaskRuntime] = {}
        self.task_history: Dict[str, list[TaskRuntime]] = {}
        self.retain_history = retain_history
        self.history_limit = history_limit
        self._semaphore = asyncio.Semaphore(max_concurrency)

    def _is_terminal(self, status: TaskStatus) -> bool:
        return status in self.TERMINAL_STATUSES

    def _archive_runtime(self, runtime: TaskRuntime) -> None:
        if not self.retain_history:
            return

        history = self.task_history.setdefault(runtime.request.task_id, [])
        history.append(runtime)
        if self.history_limit is not None and self.history_limit >= 0:
            self.task_history[runtime.request.task_id] = history[-self.history_limit :]

    async def submit(self, request: TaskRequest, on_event) -> TaskStatus:
        """提交并执行任务。"""
        existing_runtime = self.tasks.get(request.task_id)
        if existing_runtime and not self._is_terminal(existing_runtime.status):
            on_event(TaskEvent(task_id=request.task_id, event_type="rejected", payload={"reason": "task_id 已存在"}))
            return TaskStatus.FAILED

        if existing_runtime and self._is_terminal(existing_runtime.status):
            self._archive_runtime(existing_runtime)

        decision = self.scheduler.decide(require_gpu=request.require_gpu, prefer_gpu=request.prefer_gpu)
        if not decision.accepted:
            on_event(TaskEvent(task_id=request.task_id, event_type="rejected", payload={"reason": decision.reason}))
            return TaskStatus.FAILED

        runtime = TaskRuntime(request=request)
        self.tasks[request.task_id] = runtime
        on_event(TaskEvent(task_id=request.task_id, event_type="queued", payload={"device": decision.device}))

        async with self._semaphore:
            runtime.status = TaskStatus.RUNNING
            result = await self.executor.run_task(request, on_event)
            runtime.status = result
            runtime.ended_at = datetime.now(timezone.utc)
            return result

    async def cancel(self, task_id: str, on_event) -> bool:
        """取消任务并更新状态。"""
        runtime = self.tasks.get(task_id)
        if not runtime:
            on_event(TaskEvent(task_id=task_id, event_type="cancel_failed", payload={"reason": "任务不存在"}))
            return False

        canceled = await self.executor.cancel(task_id)
        if canceled:
            runtime.status = TaskStatus.CANCELED
            on_event(TaskEvent(task_id=task_id, event_type="canceled", payload={}))
            return True

        on_event(TaskEvent(task_id=task_id, event_type="cancel_failed", payload={"reason": "任务未在运行"}))
        return False
