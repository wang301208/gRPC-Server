from __future__ import annotations

import asyncio
from typing import Dict, Optional

from node_agent.executor import ExecutorEngine
from node_agent.models import TaskEvent, TaskRequest, TaskRuntime, TaskStatus
from node_agent.scheduler import ResourceScheduler


class TaskManager:
    """任务管理器，负责接收任务、调度、执行、取消。"""

    def __init__(self, scheduler: ResourceScheduler, executor: Optional[ExecutorEngine] = None, max_concurrency: int = 2):
        self.scheduler = scheduler
        self.executor = executor or ExecutorEngine()
        self.tasks: Dict[str, TaskRuntime] = {}
        self._semaphore = asyncio.Semaphore(max_concurrency)

    async def submit(self, request: TaskRequest, on_event) -> TaskStatus:
        """提交并执行任务。"""
        if request.task_id in self.tasks:
            on_event(TaskEvent(task_id=request.task_id, event_type="rejected", payload={"reason": "task_id 已存在"}))
            return TaskStatus.FAILED

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
