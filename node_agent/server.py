from __future__ import annotations

from typing import Any, AsyncIterator, Dict

from node_agent.audit import AuditLogger
from node_agent.auth import AuthManager
from node_agent.capability import detect_capability
from node_agent.control_stream import ControlStreamEngine, ServerComponents
from node_agent.deploy_manager import DeployManager
from node_agent.metrics import MetricsCollector, TaskMetricsCollector
from node_agent.models import NodeSession
from node_agent.scheduler import ResourceScheduler
from node_agent.task_manager import TaskManager


class NodeAgentServer:
    """NodeAgent 服务主入口，负责组装模块并暴露控制流接口。"""

    def __init__(self, api_keys: Dict[str, str]):
        self.capability = detect_capability()
        self.auth = AuthManager(api_keys=api_keys)
        self.scheduler = ResourceScheduler(self.capability)
        self.task_manager = TaskManager(self.scheduler)
        self.deploy_manager = DeployManager()
        self.audit = AuditLogger()
        self.metrics = MetricsCollector(interval_sec=1.0)
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
