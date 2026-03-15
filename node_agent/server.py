from __future__ import annotations

from typing import Any, AsyncIterator, Dict

from node_agent.audit import AuditLogger
from node_agent.auth import AuthManager
from node_agent.capability import Capability, detect_capability
from node_agent.control_stream import ControlStreamEngine, ServerComponents
from node_agent.deploy_manager import DeployManager
from node_agent.executors import build_executor
from node_agent.metrics import MetricsCollector, TaskMetricsCollector
from node_agent.models import NodeSession
from node_agent.scheduler import ResourceScheduler
from node_agent.security import ServerTLSConfig
from node_agent.task_manager import TaskManager


class NodeAgentServer:
    """NodeAgent 服务主入口，负责组装模块并暴露控制流接口。"""

    def __init__(
        self,
        api_keys: Dict[str, Any],
        executor_backend: str = "subprocess",
        capability: Capability | None = None,
        metrics_interval_sec: float = 1.0,
    ):
        self.capability = capability or detect_capability()
        self.auth = AuthManager(api_keys=api_keys)
        self.scheduler = ResourceScheduler(self.capability)
        self.task_manager = TaskManager(self.scheduler, executor=build_executor(executor_backend))
        self.deploy_manager = DeployManager()
        self.audit = AuditLogger()
        self.metrics = MetricsCollector(interval_sec=metrics_interval_sec)
        self.task_metrics = TaskMetricsCollector()

    def health_status(self) -> Dict[str, Any]:
        """返回服务健康状态，用于 liveness/readiness 探活。

        说明：
        - alive 代表进程内核心组件已初始化。
        - ready 代表服务具备接收请求能力（当前至少存在可用的鉴权配置）。
        """

        checks = {
            "capability_loaded": self.capability is not None,
            "auth_manager_ready": self.auth is not None,
            "scheduler_ready": self.scheduler is not None,
            "task_manager_ready": self.task_manager is not None,
            "deploy_manager_ready": self.deploy_manager is not None,
            "audit_ready": self.audit is not None,
            "metrics_ready": self.metrics is not None,
        }
        alive = all(checks.values())

        # ready 需要满足基础活性，同时至少配置了一个节点密钥。
        has_api_key = bool(getattr(self.auth, "api_keys", {}))
        ready = alive and has_api_key

        return {
            "alive": alive,
            "ready": ready,
            "checks": checks,
        }

    async def control_stream(self, session: NodeSession, incoming: AsyncIterator[Dict[str, Any]]) -> AsyncIterator[Dict[str, Any]]:
        """模拟 gRPC 双向流：将消息处理交给独立控制流引擎。"""
        components = ServerComponents(
            capability=self.capability,
            auth=self.auth,
            task_manager=self.task_manager,
            deploy_manager=self.deploy_manager,
            audit=self.audit,
            metrics=self.metrics,
            task_metrics=self.task_metrics,
        )
        engine = ControlStreamEngine(components)
        async for item in engine.run(session, incoming):
            yield item

    async def create_grpc_server(
        self,
        bind_address: str = "127.0.0.1:50051",
        tls_config: ServerTLSConfig | None = None,
    ):
        """创建可直接启动的 gRPC aio server。"""
        import grpc

        from node_agent.grpc_service import add_servicer_to_server

        server = grpc.aio.server()
        add_servicer_to_server(server, self)
        if tls_config is not None:
            port = server.add_secure_port(bind_address, tls_config.build_server_credentials())
        else:
            port = server.add_insecure_port(bind_address)
        if port == 0:
            raise RuntimeError(f"gRPC 端口绑定失败: {bind_address}")
        return server, port

    async def serve(
        self,
        bind_address: str = "127.0.0.1:50051",
        tls_config: ServerTLSConfig | None = None,
    ):
        """启动 gRPC 服务并返回 `(server, port)`。"""
        server, port = await self.create_grpc_server(bind_address, tls_config=tls_config)
        await server.start()
        return server, port
