from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Dict

from node_agent.audit import AuditLogger
from node_agent.auth import AuthManager
from node_agent.capability import detect_capability
from node_agent.deploy_manager import DeployManager
from node_agent.metrics import MetricsCollector
from node_agent.models import DeployRequest, NodeSession, TaskEvent, TaskRequest
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
            yield {"type": "auth_failed"}
            return

        out_queue: asyncio.Queue[dict] = asyncio.Queue()
        active_tasks: set[asyncio.Task[None]] = set()

        def push_event(event: TaskEvent) -> None:
            out_queue.put_nowait({"type": "task_event", "data": event.payload, "event_type": event.event_type, "task_id": event.task_id})

        # 启动时先发送节点能力。
        yield {"type": "node_hello", "capability": self.capability.to_dict()}

        # 后台心跳指标推送。
        self.metrics.start(lambda m: out_queue.put_nowait({"type": "heartbeat", "metrics": m.to_dict()}))

        async def consume() -> None:
            async for msg in incoming:
                msg_type = msg.get("type")
                self.audit.write("incoming", msg)
                if msg_type == "task_submit":
                    req = TaskRequest(**msg["task"])
                    async def _submit() -> None:
                        await self.task_manager.submit(req, push_event)

                    task = asyncio.create_task(_submit())
                    active_tasks.add(task)
                    task.add_done_callback(active_tasks.discard)
                elif msg_type == "task_cancel":
                    async def _cancel() -> None:
                        await self.task_manager.cancel(msg["task_id"], push_event)

                    task = asyncio.create_task(_cancel())
                    active_tasks.add(task)
                    task.add_done_callback(active_tasks.discard)
                elif msg_type == "deploy":
                    req = DeployRequest(**msg["deploy"])
                    result = await self.deploy_manager.deploy(req)
                    out_queue.put_nowait({"type": "deploy_event", "ok": result.ok, "message": result.message})
                elif msg_type == "close":
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
