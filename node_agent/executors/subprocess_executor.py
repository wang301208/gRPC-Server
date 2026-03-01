from __future__ import annotations

import asyncio
import json
import os
from typing import Callable, Optional

from node_agent.models import TaskEvent, TaskRequest, TaskStatus


class SubprocessExecutor:
    """基于本地子进程的执行器。"""

    def __init__(self) -> None:
        self._processes: dict[str, asyncio.subprocess.Process] = {}

    def _parse_structured_event(self, task_id: str, line_text: str) -> TaskEvent:
        """解析约定前缀的结构化事件，不符合格式时回退为普通日志事件。"""
        stripped_line = line_text.rstrip()
        prefix_to_event = {
            "__PROGRESS__": "progress",
            "__ARTIFACT__": "artifact",
        }

        for prefix, event_type in prefix_to_event.items():
            if not stripped_line.startswith(prefix):
                continue

            payload_text = stripped_line[len(prefix) :].strip()
            if not payload_text:
                break

            try:
                payload = json.loads(payload_text)
            except json.JSONDecodeError:
                break

            if isinstance(payload, dict):
                return TaskEvent(task_id=task_id, event_type=event_type, payload=payload)
            break

        return TaskEvent(task_id=task_id, event_type="log", payload={"line": stripped_line})

    async def run_task(self, request: TaskRequest, on_event: Callable[[TaskEvent], None]) -> TaskStatus:
        """执行任务并持续回传日志。"""
        env = {**os.environ, **request.env}
        if request.assigned_gpu_indices:
            # 与调度器选择保持一致，避免任务跑到未分配 GPU。
            env["CUDA_VISIBLE_DEVICES"] = ",".join(str(index) for index in request.assigned_gpu_indices)

        try:
            proc = await asyncio.create_subprocess_exec(
                *request.command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env=env,
                cwd=request.workdir or None,
            )
        except FileNotFoundError as exc:
            on_event(
                TaskEvent(
                    task_id=request.task_id,
                    event_type="failed",
                    payload={"reason": f"命令不存在: {exc.filename}"},
                )
            )
            return TaskStatus.FAILED

        self._processes[request.task_id] = proc
        on_event(
            TaskEvent(
                task_id=request.task_id,
                event_type="running",
                payload={"pid": proc.pid, "backend": "subprocess", "gpu_indices": request.assigned_gpu_indices, "resource_request": request.resource_request},
            )
        )

        assert proc.stdout is not None
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            line_text = line.decode("utf-8", errors="ignore")
            on_event(self._parse_structured_event(request.task_id, line_text))

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
