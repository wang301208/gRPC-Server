from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

DEFAULT_PROTOCOL_VERSION = "v1"


@dataclass(slots=True)
class TaskSubmitMessage:
    task_id: str
    command: list[str]
    task_type: str
    require_gpu: bool = False
    prefer_gpu: bool = False
    env: Dict[str, str] = field(default_factory=dict)
    protocol_version: str = DEFAULT_PROTOCOL_VERSION


@dataclass(slots=True)
class TaskCancelMessage:
    task_id: str
    protocol_version: str = DEFAULT_PROTOCOL_VERSION


@dataclass(slots=True)
class DeployRequestMessage:
    service_name: str
    workdir: str
    deploy_type: str = "service"
    command: Optional[list[str]] = None
    require_gpu: bool = False
    env: Dict[str, str] = field(default_factory=dict)
    protocol_version: str = DEFAULT_PROTOCOL_VERSION


@dataclass(slots=True)
class CloseMessage:
    protocol_version: str = DEFAULT_PROTOCOL_VERSION


@dataclass(slots=True)
class ControlRequestEnvelope:
    task_submit: Optional[TaskSubmitMessage] = None
    task_cancel: Optional[TaskCancelMessage] = None
    deploy_request: Optional[DeployRequestMessage] = None
    close: Optional[CloseMessage] = None

    def which_oneof(self) -> str:
        if self.task_submit is not None:
            return "task_submit"
        if self.task_cancel is not None:
            return "task_cancel"
        if self.deploy_request is not None:
            return "deploy_request"
        if self.close is not None:
            return "close"
        raise ValueError("ControlRequest 未设置任何 oneof 字段")


# 兼容旧字典协议输入，统一映射到 oneof 结构。
def parse_legacy_request(message: Dict[str, Any]) -> ControlRequestEnvelope:
    protocol_version = message.get("protocol_version") or DEFAULT_PROTOCOL_VERSION
    # 兼容策略：仅新增字段，旧客户端缺失新增字段时使用稳定默认值。
    msg_type = message.get("type")
    if msg_type == "task_submit":
        task = message.get("task", {})
        return ControlRequestEnvelope(
            task_submit=TaskSubmitMessage(
                task_id=task["task_id"],
                command=list(task["command"]),
                task_type=task["task_type"],
                require_gpu=task.get("require_gpu", False),
                prefer_gpu=task.get("prefer_gpu", False),
                env=dict(task.get("env") or {}),
                protocol_version=protocol_version,
            )
        )

    if msg_type == "task_cancel":
        return ControlRequestEnvelope(
            task_cancel=TaskCancelMessage(
                task_id=message["task_id"],
                protocol_version=protocol_version,
            )
        )

    if msg_type == "deploy":
        deploy = message.get("deploy", {})
        return ControlRequestEnvelope(
            deploy_request=DeployRequestMessage(
                service_name=deploy["service_name"],
                workdir=deploy["workdir"],
                deploy_type=deploy.get("deploy_type", "service"),
                command=deploy.get("command"),
                require_gpu=deploy.get("require_gpu", False),
                env=dict(deploy.get("env") or {}),
                protocol_version=protocol_version,
            )
        )

    if msg_type == "close":
        return ControlRequestEnvelope(close=CloseMessage(protocol_version=protocol_version))

    raise ValueError(f"未知消息类型: {msg_type}")
