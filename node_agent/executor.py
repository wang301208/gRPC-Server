from __future__ import annotations

import asyncio
import os
from abc import ABC, abstractmethod
from typing import Callable, Optional

from node_agent.models import TaskEvent, TaskRequest, TaskStatus


class Executor(ABC):
    """任务执行后端接口。"""

    @property
    @abstractmethod
    def backend_name(self) -> str:
        """返回执行后端名称。"""

    @abstractmethod
    async def run(self, request: TaskRequest, on_event: Callable[[TaskEvent], None]) -> TaskStatus:
        """执行任务并输出事件。"""

    @abstractmethod
    async def cancel(self, task_id: str, grace_sec: float = 1.5) -> bool:
        """取消任务。"""

    @abstractmethod
    def status(self, task_id: str) -> Optional[TaskStatus]:
        """查询任务状态。"""


class SubprocessExecutor(Executor):
    """基于本地子进程的默认执行器。"""

    def __init__(self) -> None:
        self._processes: dict[str, asyncio.subprocess.Process] = {}
        self._statuses: dict[str, TaskStatus] = {}

    @property
    def backend_name(self) -> str:
        return "subprocess"

    async def run(self, request: TaskRequest, on_event: Callable[[TaskEvent], None]) -> TaskStatus:
        """执行单个任务并持续回传日志。"""
        try:
            proc = await asyncio.create_subprocess_exec(
                *request.command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                env={**os.environ, **request.env},
            )
        except FileNotFoundError as exc:
            self._statuses[request.task_id] = TaskStatus.FAILED
            on_event(
                TaskEvent(
                    task_id=request.task_id,
                    event_type="failed",
                    payload={"reason": f"命令不存在: {exc.filename}"},
                )
            )
            return TaskStatus.FAILED

        self._processes[request.task_id] = proc
        self._statuses[request.task_id] = TaskStatus.RUNNING
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

        if self._statuses.get(request.task_id) == TaskStatus.CANCELED:
            return TaskStatus.CANCELED

        if code == 0:
            self._statuses[request.task_id] = TaskStatus.COMPLETED
            on_event(TaskEvent(task_id=request.task_id, event_type="completed", payload={"code": code}))
            return TaskStatus.COMPLETED

        self._statuses[request.task_id] = TaskStatus.FAILED
        on_event(TaskEvent(task_id=request.task_id, event_type="failed", payload={"code": code}))
        return TaskStatus.FAILED

    async def cancel(self, task_id: str, grace_sec: float = 1.5) -> bool:
        """尝试取消正在运行的任务。"""
        proc = self._processes.get(task_id)
        if not proc:
            return False

        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=grace_sec)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
        self._processes.pop(task_id, None)
        self._statuses[task_id] = TaskStatus.CANCELED
        return True

    def status(self, task_id: str) -> Optional[TaskStatus]:
        return self._statuses.get(task_id)


class DockerExecutor(Executor):
    """基于 Docker CLI 的简化执行器。"""

    def __init__(self) -> None:
        self._containers: dict[str, str] = {}
        self._statuses: dict[str, TaskStatus] = {}

    @property
    def backend_name(self) -> str:
        return "docker"

    async def run(self, request: TaskRequest, on_event: Callable[[TaskEvent], None]) -> TaskStatus:
        """启动容器、拉取日志并等待结束。"""
        if not request.command:
            self._statuses[request.task_id] = TaskStatus.FAILED
            on_event(TaskEvent(task_id=request.task_id, event_type="failed", payload={"reason": "docker 任务缺少镜像参数"}))
            return TaskStatus.FAILED

        image, *command = request.command
        container_name = f"task-{request.task_id}"
        self._containers[request.task_id] = container_name

        run_cmd = ["docker", "run", "--rm", "--name", container_name]
        for key, value in request.env.items():
            run_cmd.extend(["-e", f"{key}={value}"])
        run_cmd.append(image)
        run_cmd.extend(command)

        try:
            proc = await asyncio.create_subprocess_exec(
                *run_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except FileNotFoundError:
            self._statuses[request.task_id] = TaskStatus.FAILED
            on_event(TaskEvent(task_id=request.task_id, event_type="failed", payload={"reason": "docker 命令不存在"}))
            return TaskStatus.FAILED

        self._statuses[request.task_id] = TaskStatus.RUNNING
        on_event(
            TaskEvent(
                task_id=request.task_id,
                event_type="running",
                payload={"container": container_name, "image": image},
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
        self._containers.pop(request.task_id, None)
        if self._statuses.get(request.task_id) == TaskStatus.CANCELED:
            return TaskStatus.CANCELED

        if code == 0:
            self._statuses[request.task_id] = TaskStatus.COMPLETED
            on_event(TaskEvent(task_id=request.task_id, event_type="completed", payload={"code": code}))
            return TaskStatus.COMPLETED

        self._statuses[request.task_id] = TaskStatus.FAILED
        on_event(TaskEvent(task_id=request.task_id, event_type="failed", payload={"code": code}))
        return TaskStatus.FAILED

    async def cancel(self, task_id: str, grace_sec: float = 1.5) -> bool:
        """停止容器并返回取消结果。"""
        container = self._containers.get(task_id)
        if not container:
            return False

        try:
            stop_proc = await asyncio.create_subprocess_exec(
                "docker",
                "stop",
                container,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except FileNotFoundError:
            return False

        try:
            await asyncio.wait_for(stop_proc.wait(), timeout=grace_sec)
        except asyncio.TimeoutError:
            return False

        self._containers.pop(task_id, None)
        self._statuses[task_id] = TaskStatus.CANCELED
        return stop_proc.returncode == 0

    def status(self, task_id: str) -> Optional[TaskStatus]:
        return self._statuses.get(task_id)


# 向后兼容旧名称。
ExecutorEngine = SubprocessExecutor
