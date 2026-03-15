from __future__ import annotations

import asyncio
import os
from typing import Callable, Optional

from node_agent.models import TaskEvent, TaskRequest, TaskStatus


class DockerExecutor:
    """基于 Docker 容器的执行器。"""

    def __init__(self, cpu_image: str = "python:3.11-slim", gpu_image: str = "nvidia/cuda:12.2.0-runtime-ubuntu22.04") -> None:
        self.cpu_image = cpu_image
        self.gpu_image = gpu_image
        self._processes: dict[str, asyncio.subprocess.Process] = {}

    def _build_command(self, request: TaskRequest) -> list[str]:
        """根据任务参数构建 docker run 命令。"""
        image = self.gpu_image if request.require_gpu else self.cpu_image
        command = ["docker", "run", "--rm"]

        cpu_cores = max(1, int(request.resource_request.get("cpu_cores", 1)))
        memory_mb = max(1, int(request.resource_request.get("memory_mb", 256)))
        command.extend(["--cpus", str(cpu_cores), "--memory", f"{memory_mb}m"])

        if request.require_gpu:
            # 优先绑定调度器指定的 GPU；旧逻辑回退到 all。
            if request.assigned_gpu_indices:
                devices = ",".join(str(index) for index in request.assigned_gpu_indices)
                command.extend(["--gpus", f"device={devices}"])
            else:
                command.extend(["--gpus", "all"])

        workdir = request.workdir or "/workspace"
        command.extend(["-w", workdir])

        # 若工作目录存在，则自动挂载到容器中，确保命令可访问本地代码。
        if request.workdir and os.path.isdir(request.workdir):
            command.extend(["-v", f"{request.workdir}:{request.workdir}"])

        for key, value in request.env.items():
            command.extend(["-e", f"{key}={value}"])

        command.append(image)
        command.extend(request.command)
        return command

    async def run_task(self, request: TaskRequest, on_event: Callable[[TaskEvent], None]) -> TaskStatus:
        """在容器中执行任务并流式回传日志。"""
        docker_cmd = self._build_command(request)
        try:
            proc = await asyncio.create_subprocess_exec(
                *docker_cmd,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except FileNotFoundError:
            on_event(
                TaskEvent(
                    task_id=request.task_id,
                    event_type="failed",
                    payload={"reason": "docker 命令不存在"},
                )
            )
            return TaskStatus.FAILED

        self._processes[request.task_id] = proc
        on_event(
            TaskEvent(
                task_id=request.task_id,
                event_type="running",
                payload={
                    "pid": proc.pid,
                    "backend": "docker",
                    "image": self.gpu_image if request.require_gpu else self.cpu_image,
                    "gpu_indices": request.assigned_gpu_indices,
                    "resource_request": request.resource_request,
                },
            )
        )

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
        """取消运行中的 docker 命令进程。"""
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
