from __future__ import annotations

from typing import TYPE_CHECKING, Any, AsyncIterator, Iterable, Mapping

import grpc
from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct

from node_agent.grpc_gen import control_stream_pb2, control_stream_pb2_grpc
from node_agent.models import NodeSession
from node_agent.protocol import DEFAULT_PROTOCOL_VERSION

if TYPE_CHECKING:
    from node_agent.server import NodeAgentServer


_NODE_ID_METADATA_KEYS = ("x-node-id", "node-id")
_API_KEY_METADATA_KEYS = ("x-api-key", "api-key")


def _struct_from_mapping(data: Mapping[str, Any] | None) -> Struct:
    msg = Struct()
    if data:
        msg.update(dict(data))
    return msg


def _struct_to_dict(data: Struct) -> dict[str, Any]:
    return MessageToDict(data, preserving_proto_field_name=True)


def _capability_to_proto(capability: Mapping[str, Any]) -> control_stream_pb2.CapabilityInfo:
    message = control_stream_pb2.CapabilityInfo(
        cpu_cores=int(capability.get("cpu_cores", 0)),
        total_memory_mb=int(capability.get("total_memory_mb", 0)),
        total_disk_gb=int(capability.get("total_disk_gb", 0)),
        gpu_available=bool(capability.get("gpu_available", False)),
        gpu_name=str(capability.get("gpu_name", "")),
        gpu_vram_mb=int(capability.get("gpu_vram_mb", 0)),
        cuda_version=str(capability.get("cuda_version", "")),
        driver_version=str(capability.get("driver_version", "")),
    )
    for gpu in capability.get("gpus") or []:
        message.gpus.add(
            index=int(gpu.get("index", 0)),
            name=str(gpu.get("name", "")),
            total_vram_mb=int(gpu.get("total_vram_mb", 0)),
        )
    return message


def grpc_request_to_legacy(message: control_stream_pb2.ControlRequest) -> dict[str, Any]:
    payload_type = message.WhichOneof("payload")
    request: dict[str, Any] = {}
    if message.request_id:
        request["request_id"] = message.request_id
    if message.trace_id:
        request["trace_id"] = message.trace_id

    if payload_type == "task_submit":
        submit = message.task_submit
        task = {
            "task_id": submit.task_id,
            "command": list(submit.command),
            "task_type": submit.task_type,
            "require_gpu": submit.require_gpu,
            "prefer_gpu": submit.prefer_gpu,
            "env": dict(submit.env),
            "priority": submit.priority,
            "timeout_sec": submit.timeout_sec,
            "workdir": submit.workdir,
        }
        if submit.HasField("resource_request"):
            task["resource_request"] = {
                "cpu_cores": submit.resource_request.cpu_cores,
                "memory_mb": submit.resource_request.memory_mb,
                "gpu_vram_mb": submit.resource_request.gpu_vram_mb,
            }
        request.update(
            {
                "type": "task_submit",
                "protocol_version": submit.protocol_version or DEFAULT_PROTOCOL_VERSION,
                "task": task,
            }
        )
        return request

    if payload_type == "task_cancel":
        cancel = message.task_cancel
        request.update(
            {
                "type": "task_cancel",
                "protocol_version": cancel.protocol_version or DEFAULT_PROTOCOL_VERSION,
                "task_id": cancel.task_id,
            }
        )
        return request

    if payload_type == "task_sync_request":
        task_sync = message.task_sync_request
        request.update(
            {
                "type": "task_sync",
                "protocol_version": task_sync.protocol_version or DEFAULT_PROTOCOL_VERSION,
            }
        )
        return request

    if payload_type == "deploy_request":
        deploy = message.deploy_request
        request.update(
            {
                "type": "deploy",
                "protocol_version": deploy.protocol_version or DEFAULT_PROTOCOL_VERSION,
                "deploy": {
                    "service_name": deploy.service_name,
                    "workdir": deploy.workdir,
                    "deploy_type": deploy.deploy_type,
                    "command": list(deploy.command),
                    "require_gpu": deploy.require_gpu,
                    "env": dict(deploy.env),
                },
            }
        )
        return request

    if payload_type == "close":
        close = message.close
        request.update(
            {
                "type": "close",
                "protocol_version": close.protocol_version or DEFAULT_PROTOCOL_VERSION,
            }
        )
        return request

    raise ValueError("ControlRequest 未设置任何 payload")


def legacy_response_to_grpc(item: Mapping[str, Any]) -> control_stream_pb2.ControlResponse:
    response = control_stream_pb2.ControlResponse(
        request_id=str(item.get("request_id") or ""),
        trace_id=str(item.get("trace_id") or ""),
    )
    msg_type = item.get("type")

    if msg_type == "node_hello":
        response.node_hello.protocol_version = str(item.get("protocol_version") or DEFAULT_PROTOCOL_VERSION)
        response.node_hello.capability.CopyFrom(_capability_to_proto(item.get("capability") or {}))
        return response

    if msg_type == "task_event":
        response.task_event.protocol_version = str(item.get("protocol_version") or DEFAULT_PROTOCOL_VERSION)
        response.task_event.task_id = str(item.get("task_id") or "")
        response.task_event.event_type = str(item.get("event_type") or "")
        response.task_event.error_code = str(item.get("error_code") or "")
        response.task_event.payload.CopyFrom(_struct_from_mapping(item.get("data") or {}))
        return response

    if msg_type == "deploy_event":
        response.deploy_event.protocol_version = str(item.get("protocol_version") or DEFAULT_PROTOCOL_VERSION)
        response.deploy_event.ok = bool(item.get("ok", False))
        response.deploy_event.message = str(item.get("message") or "")
        response.deploy_event.error_code = str(item.get("error_code") or "")
        return response

    if msg_type == "heartbeat":
        response.heartbeat.protocol_version = str(item.get("protocol_version") or DEFAULT_PROTOCOL_VERSION)
        response.heartbeat.metrics.CopyFrom(_struct_from_mapping(item.get("metrics") or {}))
        return response

    if msg_type == "task_snapshot":
        response.task_snapshot.protocol_version = str(item.get("protocol_version") or DEFAULT_PROTOCOL_VERSION)
        for task in item.get("tasks") or []:
            task_msg = response.task_snapshot.tasks.add(
                task_id=str(task.get("task_id") or ""),
                status=str(task.get("status") or ""),
                task_type=str(task.get("task_type") or ""),
                require_gpu=bool(task.get("require_gpu", False)),
                prefer_gpu=bool(task.get("prefer_gpu", False)),
                priority=int(task.get("priority", 0)),
                timeout_sec=float(task.get("timeout_sec", 0.0)),
            )
            resource_request = dict(task.get("resource_request") or {})
            task_msg.resource_request.cpu_cores = int(resource_request.get("cpu_cores", 1))
            task_msg.resource_request.memory_mb = int(resource_request.get("memory_mb", 256))
            task_msg.resource_request.gpu_vram_mb = int(resource_request.get("gpu_vram_mb", 0))
            task_msg.assigned_gpu_indices.extend(int(item) for item in task.get("assigned_gpu_indices") or [])
        return response

    if msg_type == "auth_failed":
        response.auth_failed.protocol_version = str(item.get("protocol_version") or DEFAULT_PROTOCOL_VERSION)
        response.auth_failed.reason = str(item.get("reason") or item.get("error_message") or "")
        response.auth_failed.error_code = str(item.get("error_code") or "")
        return response

    raise ValueError(f"未知响应类型: {msg_type}")


def _extract_api_key(metadata: Mapping[str, str]) -> str:
    for key in _API_KEY_METADATA_KEYS:
        value = metadata.get(key)
        if value:
            return value

    authorization = metadata.get("authorization", "")
    if not authorization:
        return ""
    if authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return authorization


def session_from_metadata(metadata_items: Iterable[tuple[str, str]]) -> NodeSession:
    metadata = {key.lower(): value for key, value in metadata_items}
    node_id = ""
    for key in _NODE_ID_METADATA_KEYS:
        value = metadata.get(key)
        if value:
            node_id = value
            break

    return NodeSession(
        node_id=node_id,
        api_key=_extract_api_key(metadata),
        metadata=dict(metadata),
    )


class ControlStreamServicer(control_stream_pb2_grpc.ControlStreamServicer):
    def __init__(self, app: NodeAgentServer):
        self.app = app

    async def Stream(
        self,
        request_iterator: AsyncIterator[control_stream_pb2.ControlRequest],
        context: grpc.aio.ServicerContext,
    ) -> AsyncIterator[control_stream_pb2.ControlResponse]:
        session = session_from_metadata(context.invocation_metadata())

        async def _incoming() -> AsyncIterator[dict[str, Any]]:
            async for message in request_iterator:
                yield grpc_request_to_legacy(message)

        async for item in self.app.control_stream(session, _incoming()):
            yield legacy_response_to_grpc(item)


def add_servicer_to_server(server: grpc.aio.Server, app: NodeAgentServer) -> None:
    control_stream_pb2_grpc.add_ControlStreamServicer_to_server(ControlStreamServicer(app), server)


__all__ = [
    "ControlStreamServicer",
    "add_servicer_to_server",
    "control_stream_pb2",
    "control_stream_pb2_grpc",
    "grpc_request_to_legacy",
    "legacy_response_to_grpc",
    "session_from_metadata",
    "_struct_to_dict",
]
