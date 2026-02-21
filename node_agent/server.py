from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Dict

from node_agent.audit import AuditLogger
from node_agent.auth import AuthManager
from node_agent.capability import detect_capability
from node_agent.deploy_manager import DeployManager, DeployResult
from node_agent.metrics import MetricsCollector
from node_agent.models import DeployRequest, NodeSession, TaskEvent, TaskRequest
from node_agent.protocol import DEFAULT_PROTOCOL_VERSION, parse_legacy_request
from node_agent.scheduler import ResourceScheduler
from node_agent.task_manager import TaskManager


class NodeAgentServer:
    """NodeAgent 服务主入口，提供控制流接口。"""

    def __init__(self, api_keys: Dict[str, str]):
        self.capability = detect_capability()
        self.auth = AuthManager(api_keys=api_keys)
        self.scheduler = ResourceScheduler(self.capability)
        self.task_manager = TaskManager(self.scheduler)
        self.deploy_manager = DeployManager()
        self.audit = AuditLogger()
        self.metrics = MetricsCollector(interval_sec=1.0)

    async def control_stream(self, session: NodeSession, incoming: AsyncIterator[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """模拟 gRPC 双向流：消费客户端消息并异步产出服务端事件。"""
        if not self.auth.authenticate(session.node_id, session.api_key):
            yield {"type": "auth_failed", "protocol_version": DEFAULT_PROTOCOL_VERSION}
            return

        out_queue: asyncio.Queue[dict] = asyncio.Queue()
        active_tasks: set[asyncio.Task[None]] = set()

        def push_event(event: TaskEvent) -> None:
            out_queue.put_nowait(
                {
                    "type": "task_event",
                    "protocol_version": DEFAULT_PROTOCOL_VERSION,
                    "data": event.payload,
                    "event_type": event.event_type,
                    "task_id": event.task_id,
                }
            )

        # 启动时先发送节点能力。
        yield {
            "type": "node_hello",
            "protocol_version": DEFAULT_PROTOCOL_VERSION,
            "capability": self.capability.to_dict(),
        }

        # 后台心跳指标推送。
        self.metrics.start(
            lambda m: out_queue.put_nowait(
                {
                    "type": "heartbeat",
                    "protocol_version": DEFAULT_PROTOCOL_VERSION,
                    "metrics": m.to_dict(),
                }
            )
        )

        async def consume() -> None:
            async for msg in incoming:
                self.audit.write("incoming", msg)
                envelope = parse_legacy_request(msg)
                request_kind = envelope.which_oneof()
                if request_kind == "task_submit":
                    submit = envelope.task_submit
                    assert submit is not None
                    req = TaskRequest(
                        task_id=submit.task_id,
                        command=submit.command,
                        task_type=submit.task_type,
                        require_gpu=submit.require_gpu,
                        prefer_gpu=submit.prefer_gpu,
                        env=submit.env,
                    )

                    async def _submit() -> None:
                        await self.task_manager.submit(req, push_event)

                    task = asyncio.create_task(_submit())
                    active_tasks.add(task)
                    task.add_done_callback(active_tasks.discard)
                elif request_kind == "task_cancel":
                    cancel = envelope.task_cancel
                    assert cancel is not None

                    async def _cancel() -> None:
                        await self.task_manager.cancel(cancel.task_id, push_event)

                    task = asyncio.create_task(_cancel())
                    active_tasks.add(task)
                    task.add_done_callback(active_tasks.discard)
                elif request_kind == "deploy_request":
                    deploy = envelope.deploy_request
                    assert deploy is not None
                    req = DeployRequest(
                        service_name=deploy.service_name,
                        workdir=deploy.workdir,
                        deploy_type=deploy.deploy_type,
                        command=deploy.command,
                        require_gpu=deploy.require_gpu,
                        env=deploy.env,
                    )
                    try:
                        result = await self.deploy_manager.deploy(req)
                    except Exception as exc:
                        message = (
                            "部署流程发生未捕获异常: "
                            f"error_type={type(exc).__name__}, detail={exc}"
                        )
                        result = DeployResult(ok=False, message=message)
                    out_queue.put_nowait(
                        {
                            "type": "deploy_event",
                            "protocol_version": DEFAULT_PROTOCOL_VERSION,
                            "ok": result.ok,
                            "message": result.message,
                        }
                    )
                elif request_kind == "close":
                    break

        consumer = asyncio.create_task(consume())

        while not consumer.done() or active_tasks or not out_queue.empty():
            try:
                item = await asyncio.wait_for(out_queue.get(), timeout=0.5)
                self.audit.write("outgoing", item)
                yield item
            except asyncio.TimeoutError:
                continue

        self.metrics.stop()
