import asyncio
import sys

from node_agent.models import NodeSession
from node_agent.server import NodeAgentServer


async def _incoming(messages):
    for item in messages:
        await asyncio.sleep(0.05)
        yield item


def test_control_stream_task_flow():
    async def _run():
        server = NodeAgentServer(api_keys={"n1": "k1"})
        session = NodeSession(node_id="n1", api_key="k1")

        messages = [
            {
                "type": "task_submit",
                "task": {
                    "task_id": "s1",
                    "command": [sys.executable, "-c", "print('hello')"],
                    "task_type": "inference",
                    "require_gpu": False,
                    "prefer_gpu": False,
                    "env": {},
                },
            },
            {"type": "close"},
        ]

        results = []
        async for msg in server.control_stream(session, _incoming(messages)):
            results.append(msg)

        assert any(m.get("type") == "node_hello" for m in results)
        assert any(m.get("protocol_version") == "v1" for m in results)
        assert any(m.get("event_type") == "completed" for m in results)

    asyncio.run(_run())


def test_control_stream_waits_running_tasks_on_close():
    async def _run():
        server = NodeAgentServer(api_keys={"n1": "k1"})
        session = NodeSession(node_id="n1", api_key="k1")

        messages = [
            {
                "type": "task_submit",
                "task": {
                    "task_id": "s2",
                    "command": [sys.executable, "-c", "import time; print('start'); time.sleep(0.2); print('end')"],
                    "task_type": "inference",
                    "require_gpu": False,
                    "prefer_gpu": False,
                    "env": {},
                },
            },
            {"type": "close"},
        ]

        results = []
        async for msg in server.control_stream(session, _incoming(messages)):
            results.append(msg)

        assert any(m.get("event_type") == "completed" and m.get("task_id") == "s2" for m in results)

    asyncio.run(_run())


def test_control_stream_deploy_error_still_returns_deploy_event(monkeypatch):
    async def _run():
        server = NodeAgentServer(api_keys={"n1": "k1"})
        session = NodeSession(node_id="n1", api_key="k1")

        async def fake_deploy(_):
            raise RuntimeError("boom")

        monkeypatch.setattr(server.deploy_manager, "deploy", fake_deploy)

        messages = [
            {
                "type": "deploy",
                "deploy": {
                    "service_name": "web",
                    "workdir": ".",
                    "deploy_type": "website",
                    "require_gpu": False,
                    "env": {},
                },
            },
            {"type": "close"},
        ]

        results = []
        async for msg in server.control_stream(session, _incoming(messages)):
            results.append(msg)

        deploy_events = [m for m in results if m.get("type") == "deploy_event"]
        assert deploy_events
        assert deploy_events[0]["ok"] is False
        assert "RuntimeError" in deploy_events[0]["message"]

    asyncio.run(_run())


def test_control_stream_protocol_compat_missing_optional_fields():
    async def _run():
        server = NodeAgentServer(api_keys={"n1": "k1"})
        session = NodeSession(node_id="n1", api_key="k1")

        # 模拟旧客户端：缺失 protocol_version 和 prefer_gpu。
        messages = [
            {
                "type": "task_submit",
                "task": {
                    "task_id": "compat-1",
                    "command": [sys.executable, "-c", "print('compat')"],
                    "task_type": "inference",
                    "require_gpu": False,
                    "env": {},
                },
            },
            {"type": "close"},
        ]

        results = []
        async for msg in server.control_stream(session, _incoming(messages)):
            results.append(msg)

        # 服务端在旧字段缺失时应回落到稳定默认值并正常完成任务。
        assert any(m.get("type") == "node_hello" and m.get("protocol_version") == "v1" for m in results)
        assert any(m.get("type") == "task_event" and m.get("event_type") == "completed" for m in results)

    asyncio.run(_run())
