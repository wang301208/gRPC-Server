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
        metrics_stop_count = 0

        def fake_stop():
            nonlocal metrics_stop_count
            metrics_stop_count += 1

        monkeypatch.setattr(server.metrics, "stop", fake_stop)

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
        # deploy 抛异常后流正常结束，指标线程应被回收。
        assert metrics_stop_count == 1

    asyncio.run(_run())


def test_control_stream_protocol_compat_missing_optional_fields():
    async def _run():
        server = NodeAgentServer(api_keys={"n1": "k1"})
        session = NodeSession(node_id="n1", api_key="k1")

        # 模拟旧客户端：缺失 protocol_version、prefer_gpu 以及新增调度参数。
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
        queued_event = next(m for m in results if m.get("type") == "task_event" and m.get("event_type") == "queued")
        assert queued_event["data"]["priority"] == 0
        assert queued_event["data"]["resource_request"] == {"cpu_cores": 1, "memory_mb": 256, "gpu_vram_mb": 0}
        assert any(m.get("type") == "task_event" and m.get("event_type") == "completed" for m in results)

    asyncio.run(_run())



def test_control_stream_new_fields_passthrough_affects_queue_and_timeout():
    async def _run():
        priority_server = NodeAgentServer(api_keys={"n1": "k1"})
        priority_server.task_manager.max_concurrency = 1
        session = NodeSession(node_id="n1", api_key="k1")

        priority_messages = [
            {
                "type": "task_submit",
                "task": {
                    "task_id": "busy-1",
                    "command": [sys.executable, "-c", "import time; time.sleep(0.6)"],
                    "task_type": "inference",
                    "priority": 1,
                    "timeout_sec": 2,
                    "resource_request": {"cpu_cores": 1, "memory_mb": 256, "gpu_vram_mb": 0},
                },
            },
            {
                "type": "task_submit",
                "task": {
                    "task_id": "low-priority",
                    "command": [sys.executable, "-c", "print('low')"],
                    "task_type": "inference",
                    "priority": 1,
                    "timeout_sec": 2,
                    "resource_request": {"cpu_cores": 1, "memory_mb": 256, "gpu_vram_mb": 0},
                },
            },
            {
                "type": "task_submit",
                "task": {
                    "task_id": "high-priority",
                    "command": [sys.executable, "-c", "print('high')"],
                    "task_type": "inference",
                    "priority": 9,
                    "timeout_sec": 2,
                    "resource_request": {"cpu_cores": 1, "memory_mb": 256, "gpu_vram_mb": 0},
                },
            },
            {"type": "close"},
        ]

        priority_results = []
        async for msg in priority_server.control_stream(session, _incoming(priority_messages)):
            priority_results.append(msg)

        running_events = [
            m
            for m in priority_results
            if m.get("type") == "task_event" and m.get("event_type") == "running"
        ]
        started_after_busy = [m["task_id"] for m in running_events if m["task_id"] != "busy-1"]
        assert started_after_busy[0] == "high-priority"

        timeout_server = NodeAgentServer(api_keys={"n1": "k1"})
        timeout_messages = [
            {
                "type": "task_submit",
                "task": {
                    "task_id": "timeout-task",
                    "command": [sys.executable, "-c", "import time; time.sleep(0.2)"],
                    "task_type": "inference",
                    "priority": 0,
                    "timeout_sec": 0.05,
                    "resource_request": {"cpu_cores": 1, "memory_mb": 256, "gpu_vram_mb": 0},
                },
            },
            {"type": "close"},
        ]

        timeout_results = []
        async for msg in timeout_server.control_stream(session, _incoming(timeout_messages)):
            timeout_results.append(msg)

        timeout_failed = [
            m
            for m in timeout_results
            if m.get("type") == "task_event"
            and m.get("task_id") == "timeout-task"
            and m.get("event_type") == "failed"
        ]
        assert timeout_failed
        assert timeout_failed[0]["data"]["reason"] == "任务执行超时"

    asyncio.run(_run())


def test_control_stream_deploy_rejected_without_scope():
    async def _run():
        server = NodeAgentServer(
            api_keys={
                "n1": {
                    "key": "k1",
                    "scopes": ["task.submit"],
                    "role": "operator",
                }
            }
        )
        session = NodeSession(node_id="n1", api_key="k1")

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
        assert "权限不足" in deploy_events[0]["message"]

    asyncio.run(_run())


def test_control_stream_cancel_rejected_without_scope():
    async def _run():
        server = NodeAgentServer(
            api_keys={
                "n1": {
                    "key": "k1",
                    "scopes": ["task.submit"],
                    "role": "operator",
                }
            }
        )
        session = NodeSession(node_id="n1", api_key="k1")

        messages = [
            {"type": "task_cancel", "task_id": "s1"},
            {"type": "close"},
        ]

        results = []
        async for msg in server.control_stream(session, _incoming(messages)):
            results.append(msg)

        rejected_events = [m for m in results if m.get("type") == "task_event" and m.get("event_type") == "rejected"]
        assert rejected_events
        assert rejected_events[0]["task_id"] == "s1"
        assert "权限不足" in rejected_events[0]["data"]["reason"]

    asyncio.run(_run())


def test_control_stream_task_trace_id_consistent_across_events():
    async def _run():
        server = NodeAgentServer(api_keys={"n1": "k1"})
        session = NodeSession(node_id="n1", api_key="k1")

        messages = [
            {
                "type": "task_submit",
                "request_id": "req-trace-1",
                "trace_id": "trace-xyz",
                "task": {
                    "task_id": "trace-task-1",
                    "command": [sys.executable, "-c", "print('trace')"],
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

        task_events = [m for m in results if m.get("type") == "task_event" and m.get("task_id") == "trace-task-1"]
        assert task_events
        assert {m.get("trace_id") for m in task_events} == {"trace-xyz"}
        assert {m.get("request_id") for m in task_events} == {"req-trace-1"}

    asyncio.run(_run())


def test_control_stream_error_codes_for_auth_and_deploy_missing_command(monkeypatch):
    async def _run():
        denied_server = NodeAgentServer(api_keys={"n1": "k1"})
        denied_session = NodeSession(node_id="n1", api_key="bad-key")

        denied_results = []
        async for msg in denied_server.control_stream(denied_session, _incoming([{"type": "close"}])):
            denied_results.append(msg)

        assert denied_results
        assert denied_results[0]["error_code"] == "AUTH_FAILED"

        deploy_server = NodeAgentServer(api_keys={"n1": "k1"})
        deploy_session = NodeSession(node_id="n1", api_key="k1")

        async def fake_deploy(_):
            return type("R", (), {"ok": False, "message": "部署失败: error_type=FileNotFoundError"})()

        monkeypatch.setattr(deploy_server.deploy_manager, "deploy", fake_deploy)

        deploy_messages = [
            {
                "type": "deploy",
                "request_id": "req-deploy-1",
                "trace_id": "trace-deploy-1",
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

        deploy_results = []
        async for msg in deploy_server.control_stream(deploy_session, _incoming(deploy_messages)):
            deploy_results.append(msg)

        deploy_events = [m for m in deploy_results if m.get("type") == "deploy_event"]
        assert deploy_events
        assert deploy_events[0]["error_code"] == "DEPLOY_CMD_NOT_FOUND"

    asyncio.run(_run())


def test_control_stream_cleanup_when_incoming_raises(monkeypatch):
    async def _run():
        server = NodeAgentServer(api_keys={"n1": "k1"})
        session = NodeSession(node_id="n1", api_key="k1")

        metrics_stop_count = 0
        cancelled_count = 0
        submit_started = asyncio.Event()

        def fake_stop():
            nonlocal metrics_stop_count
            metrics_stop_count += 1

        monkeypatch.setattr(server.metrics, "stop", fake_stop)

        async def fake_submit(_, __):
            nonlocal cancelled_count
            submit_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled_count += 1
                raise

        monkeypatch.setattr(server.task_manager, "submit", fake_submit)

        async def broken_incoming():
            yield {
                "type": "task_submit",
                "task": {
                    "task_id": "cleanup-1",
                    "command": [sys.executable, "-c", "print('cleanup')"],
                    "task_type": "inference",
                    "require_gpu": False,
                    "prefer_gpu": False,
                    "env": {},
                },
            }
            await submit_started.wait()
            raise RuntimeError("incoming boom")

        results = []
        async for msg in server.control_stream(session, broken_incoming()):
            results.append(msg)

        # 流在异常后应当完成收尾，不遗留任务和指标线程。
        assert any(m.get("type") == "node_hello" for m in results)
        assert metrics_stop_count == 1
        assert cancelled_count == 1

    asyncio.run(_run())


def test_health_status_ready_with_api_keys():
    server = NodeAgentServer(api_keys={"n1": "k1"})

    status = server.health_status()

    assert status["alive"] is True
    assert status["ready"] is True
    assert all(status["checks"].values())


def test_health_status_not_ready_without_api_keys():
    server = NodeAgentServer(api_keys={})

    status = server.health_status()

    assert status["alive"] is True
    assert status["ready"] is False
