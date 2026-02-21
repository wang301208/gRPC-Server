from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional


class TaskStatus(str, Enum):
    """任务状态枚举。"""

    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


@dataclass(slots=True)
class TaskRequest:
    """任务提交请求。"""

    task_id: str
    command: list[str]
    task_type: str
    require_gpu: bool = False
    prefer_gpu: bool = False
    env: Dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class TaskEvent:
    """任务事件。"""

    task_id: str
    event_type: str
    payload: Dict[str, Any]


@dataclass(slots=True)
class DeployRequest:
    """部署请求。"""

    service_name: str
    workdir: str
    deploy_type: str = "service"
    command: Optional[list[str]] = None
    require_gpu: bool = False
    env: Dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class NodeSession:
    """会话上下文。"""

    node_id: str
    api_key: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TaskRuntime:
    """任务运行时对象。"""

    request: TaskRequest
    status: TaskStatus = TaskStatus.QUEUED
    process: Optional[Any] = None
    ended_at: Optional[datetime] = None
