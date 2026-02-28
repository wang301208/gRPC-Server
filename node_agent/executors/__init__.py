from __future__ import annotations

from node_agent.executors.base import TaskExecutor
from node_agent.executors.docker_executor import DockerExecutor
from node_agent.executors.subprocess_executor import SubprocessExecutor


def build_executor(backend: str) -> TaskExecutor:
    """根据配置构造执行器实例。"""
    if backend == "subprocess":
        return SubprocessExecutor()
    if backend == "docker":
        return DockerExecutor()
    raise ValueError(f"不支持的执行器后端: {backend}")


__all__ = ["TaskExecutor", "SubprocessExecutor", "DockerExecutor", "build_executor"]
