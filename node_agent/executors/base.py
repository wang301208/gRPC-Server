from __future__ import annotations

from typing import Callable, Protocol

from node_agent.models import TaskEvent, TaskRequest, TaskStatus


class TaskExecutor(Protocol):
    """任务执行器统一接口。"""

    async def run_task(self, request: TaskRequest, on_event: Callable[[TaskEvent], None]) -> TaskStatus:
        """执行任务并回传事件。"""

    async def cancel(self, task_id: str) -> bool:
        """取消指定任务。"""
