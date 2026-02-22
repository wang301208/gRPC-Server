from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import Any, AsyncIterator, Dict

from node_agent.auth import AuthContext
from node_agent.deploy_manager import DeployManager, DeployResult
from node_agent.metrics import MetricsCollector, TaskMetricsCollector
from node_agent.capability import Capability
from node_agent.models import DeployRequest, NodeSession, TaskEvent, TaskRequest
from node_agent.protocol import DEFAULT_PROTOCOL_VERSION, parse_legacy_request
from node_agent.task_manager import TaskManager


@dataclass
class ServerComponents:
    """控制流依赖容器，用于解耦服务主入口与处理逻辑。"""

    capability: Capability
    auth: Any
    task_manager: TaskManager
    deploy_manager: DeployManager
    audit: Any
    metrics: MetricsCollector
    task_metrics: TaskMetricsCollector


class ControlStreamEngine:
    """控制流处理器：负责消息消费、鉴权、任务调度与事件输出。"""

    def __init__(self, components: ServerComponents):
        self.components = components
        self.out_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.active_tasks: set[asyncio.Task[None]] = set()
        self.task_context: dict[str, dict[str, Any]] = {}
        self.auth_context: AuthContext | None = None

    async def run(self, session: NodeSession, incoming: AsyncIterator[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """运行控制流并持续输出服务端事件。"""
        consumer: asyncio.Task[None] | None = None
        try:
            self.auth_context = self.components.auth.authenticate_with_context(session.node_id, session.api_key)
            if self.auth_context is None:
                yield {
                    "type": "auth_failed",
                    "protocol_version": DEFAULT_PROTOCOL_VERSION,
                    "error_code": "AUTH_FAILED",
                    "error_message": "节点鉴权失败",
                }
                return

            yield {
                "type": "node_hello",
                "protocol_version": DEFAULT_PROTOCOL_VERSION,
                "capability": self.components.capability.to_dict(),
            }

            # 后台启动指标推送。
            self.components.metrics.start(
                lambda m: self.out_queue.put_nowait(
                    {
                        "type": "heartbeat",
                        "protocol_version": DEFAULT_PROTOCOL_VERSION,
                        "metrics": {**m.to_dict(), **self.components.task_metrics.snapshot().to_dict()},
                    }
                )
            )

            consumer = asyncio.create_task(self._consume(incoming))
            while not consumer.done() or self.active_tasks or not self.out_queue.empty():
                if consumer.done() and consumer.cancelled():
                    break
                if consumer.done() and consumer.exception() is not None:
                    break
                try:
                    item = await asyncio.wait_for(self.out_queue.get(), timeout=0.5)
                    self.components.audit.write(
                        "outgoing", item, trace_id=item.get("trace_id"), request_id=item.get("request_id")
                    )
                    yield item
                except asyncio.TimeoutError:
                    continue
        finally:
            await self._cleanup(consumer)

    async def _consume(self, incoming: AsyncIterator[Dict[str, Any]]) -> None:
        async for msg in incoming:
            self.components.audit.write("incoming", msg, trace_id=msg.get("trace_id"), request_id=msg.get("request_id"))
            envelope = parse_legacy_request(msg)
            request_kind = envelope.which_oneof()
            if request_kind == "task_submit":
                await self._handle_submit(envelope.task_submit, msg)
            elif request_kind == "task_cancel":
                await self._handle_cancel(envelope.task_cancel, msg)
            elif request_kind == "deploy_request":
                await self._handle_deploy(envelope.deploy_request, msg)
            elif request_kind == "close":
                break

    async def _handle_submit(self, submit, msg: Dict[str, Any]) -> None:
        assert submit is not None
        request_id = msg.get("request_id") or submit.task_id
        trace_id = msg.get("trace_id") or request_id or uuid.uuid4().hex
        self.task_context[submit.task_id] = {
            "request_id": request_id,
            "trace_id": trace_id,
            "queued_at": time.monotonic(),
        }
        req = TaskRequest(
            task_id=submit.task_id,
            command=submit.command,
            task_type=submit.task_type,
            require_gpu=submit.require_gpu,
            prefer_gpu=submit.prefer_gpu,
            env=submit.env,
        )

        async def _submit() -> None:
            await self.components.task_manager.submit(req, self._push_task_event)

        self._register_task(asyncio.create_task(_submit()))

    async def _handle_cancel(self, cancel, msg: Dict[str, Any]) -> None:
        assert cancel is not None
        if not self.components.auth.authorize(
            self.auth_context,
            required_scope="task.cancel",
            allowed_roles={"admin", "operator"},
        ):
            self.out_queue.put_nowait(
                {
                    "type": "task_event",
                    "protocol_version": DEFAULT_PROTOCOL_VERSION,
                    "event_type": "rejected",
                    "task_id": cancel.task_id,
                    "data": {"reason": "权限不足，拒绝取消任务"},
                    "error_code": "AUTH_FAILED",
                    "trace_id": msg.get("trace_id"),
                    "request_id": msg.get("request_id"),
                }
            )
            return

        async def _cancel() -> None:
            await self.components.task_manager.cancel(cancel.task_id, self._push_task_event)

        self._register_task(asyncio.create_task(_cancel()))

    async def _handle_deploy(self, deploy, msg: Dict[str, Any]) -> None:
        assert deploy is not None
        if not self.components.auth.authorize(self.auth_context, required_scope="deploy", allowed_roles={"admin", "operator"}):
            self.out_queue.put_nowait(
                {
                    "type": "deploy_event",
                    "protocol_version": DEFAULT_PROTOCOL_VERSION,
                    "ok": False,
                    "message": "权限不足，拒绝部署请求",
                    "error_code": "AUTH_FAILED",
                    "trace_id": msg.get("trace_id"),
                    "request_id": msg.get("request_id"),
                }
            )
            return

        req = DeployRequest(
            service_name=deploy.service_name,
            workdir=deploy.workdir,
            deploy_type=deploy.deploy_type,
            command=deploy.command,
            require_gpu=deploy.require_gpu,
            env=deploy.env,
        )
        try:
            result = await self.components.deploy_manager.deploy(req)
        except Exception as exc:
            message = f"部署流程发生未捕获异常: error_type={type(exc).__name__}, detail={exc}"
            result = DeployResult(ok=False, message=message)

        error_code = None
        if not result.ok:
            error_code = "DEPLOY_CMD_NOT_FOUND" if "FileNotFoundError" in result.message else "DEPLOY_FAILED"

        self.out_queue.put_nowait(
            {
                "type": "deploy_event",
                "protocol_version": DEFAULT_PROTOCOL_VERSION,
                "ok": result.ok,
                "message": result.message,
                "error_code": error_code,
                "trace_id": msg.get("trace_id"),
                "request_id": msg.get("request_id"),
            }
        )

    def _register_task(self, task: asyncio.Task[None]) -> None:
        self.active_tasks.add(task)
        task.add_done_callback(self.active_tasks.discard)

    def _push_task_event(self, event: TaskEvent) -> None:
        context = self.task_context.get(event.task_id, {})
        now = time.monotonic()
        if event.event_type == "running" and context.get("running_started_at") is None:
            context["running_started_at"] = now
            queued_at = context.get("queued_at")
            if queued_at is not None:
                self.components.task_metrics.observe_queue_wait(now - float(queued_at))
        if event.event_type in {"completed", "failed", "canceled"}:
            started_at = context.get("running_started_at")
            if started_at is not None:
                self.components.task_metrics.observe_execution(now - float(started_at))
            self.components.task_metrics.observe_terminal(event.event_type)

        self.out_queue.put_nowait(
            {
                "type": "task_event",
                "protocol_version": DEFAULT_PROTOCOL_VERSION,
                "data": event.payload,
                "event_type": event.event_type,
                "task_id": event.task_id,
                "trace_id": context.get("trace_id"),
                "request_id": context.get("request_id"),
            }
        )

    async def _cleanup(self, consumer: asyncio.Task[None] | None) -> None:
        # 无论控制流如何结束，都确保后台资源被安全回收。
        self.components.metrics.stop()
        if consumer is not None and not consumer.done():
            consumer.cancel()
            try:
                await consumer
            except asyncio.CancelledError:
                pass

        if self.active_tasks:
            pending_tasks = [task for task in self.active_tasks if not task.done()]
            for task in pending_tasks:
                task.cancel()
            if pending_tasks:
                await asyncio.gather(*pending_tasks, return_exceptions=True)
