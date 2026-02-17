from __future__ import annotations

import asyncio
from typing import Callable, Optional

from node_agent.models import TaskEvent, TaskRequest, TaskStatus


class ExecutorEngine:
    """任务执行引擎，负责启动进程与流式日志。"""

    def __init__(self) -> None:
        self._processes: dict[str, asyncio.subprocess.Process] = {}

    async def run_task(self, request: TaskRequest, on_event: Callable[[TaskEvent], None]) -> TaskStatus:
        """执行单个任务并持续回传日志。"""
        proc = await asyncio.create_subprocess_exec(
            *request.command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env={**request.env},
        )
        self._processes[request.task_id] = proc
        on_event(TaskEvent(task_id=request.task_id, event_type="running", payload={"pid": proc.pid}))

        assert proc.stdout is not None
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            on_event(
                TaskEvent(
                    task_id=request.task_id,
                    event_type="log",
                    payload={"line": line.decode("utf-8", errors="ignore").rstrip()},
                )
            )

        code = await proc.wait()
        self._processes.pop(request.task_id, None)

        if code == 0:
            on_event(TaskEvent(task_id=request.task_id, event_type="completed", payload={"code": code}))
            return TaskStatus.COMPLETED
        on_event(TaskEvent(task_id=request.task_id, event_type="failed", payload={"code": code}))
        return TaskStatus.FAILED

    async def cancel(self, task_id: str, grace_sec: float = 1.5) -> bool:
        """尝试取消正在运行的任务。"""
        proc: Optional[asyncio.subprocess.Process] = self._processes.get(task_id)
        if not proc:
            return False

        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=grace_sec)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
        self._processes.pop(task_id, None)
        return True
