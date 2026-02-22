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
