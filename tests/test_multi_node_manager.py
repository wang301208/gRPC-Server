import asyncio
import json
import sys
import time

import pytest

from node_agent.capability import Capability, GpuDevice
from node_agent.grpc_gen import control_stream_pb2
from node_agent.multi_node_manager import FleetEvent, MultiNodeManager, NodeTarget
from node_agent.persistence import SQLiteFleetRepository
from node_agent.security import ClientTLSConfig, ServerTLSConfig
from node_agent.server import NodeAgentServer
from node_agent.shared_storage import FileSystemSharedTaskStorage


@pytest.mark.asyncio
async def test_multi_node_manager_discovers_multiple_nodes():
    cpu_server = NodeAgentServer(
        api_keys={"cpu-node": "cpu-key"},
        capability=Capability(4, 1024, 20, gpu_available=False),
        metrics_interval_sec=0.05,
    )
    gpu_server = NodeAgentServer(
        api_keys={"gpu-node": "gpu-key"},
        capability=Capability(
            8,
            2048,
            20,
            gpu_available=True,
            gpus=[GpuDevice(index=0, name="GPU-0", total_vram_mb=8192)],
            gpu_name="GPU-0",
            gpu_vram_mb=8192,
        ),
        metrics_interval_sec=0.05,
    )

    cpu_grpc, cpu_port = await cpu_server.serve("127.0.0.1:0")
    gpu_grpc, gpu_port = await gpu_server.serve("127.0.0.1:0")

    manager = MultiNodeManager()
    manager.add_node(NodeTarget(node_id="cpu-node", address=f"127.0.0.1:{cpu_port}", api_key="cpu-key"))
    manager.add_node(NodeTarget(node_id="gpu-node", address=f"127.0.0.1:{gpu_port}", api_key="gpu-key"))

    try:
        results = await manager.connect_all()
        assert [item.node_id for item in results] == ["cpu-node", "gpu-node"]
        assert all(item.ok for item in results)

        listed = manager.list_nodes()
        assert [item.node_id for item in listed] == ["cpu-node", "gpu-node"]
        assert listed[0].capability["gpu_available"] is False
        assert listed[1].capability["gpu_available"] is True
        assert listed[1].capability["gpus"][0]["total_vram_mb"] == 8192
    finally:
        await manager.close()
        await cpu_grpc.stop(None)
        await gpu_grpc.stop(None)


@pytest.mark.asyncio
async def test_wait_for_task_terminal_does_not_consume_subscription_events(tmp_path):
    server = NodeAgentServer(
        api_keys={"n1": "k1"},
        capability=Capability(4, 1024, 20, gpu_available=False),
        metrics_interval_sec=0.05,
    )
    grpc_server, port = await server.serve("127.0.0.1:0")

    manager = MultiNodeManager()
    manager.add_node(NodeTarget(node_id="n1", address=f"127.0.0.1:{port}", api_key="k1"))

    subscription = manager.subscribe(kinds=["task_event"], max_queue_size=16)
    try:
        result = await manager.connect_node("n1")
        assert result.ok

        submit = control_stream_pb2.TaskSubmit(
            protocol_version="v1",
            task_id="sub-task-1",
            command=[sys.executable, "-c", "print('sub-event')"],
            task_type="inference",
            workdir=str(tmp_path),
        )
        await manager.submit_task(submit)
        terminal = await manager.wait_for_task_terminal("sub-task-1")
        assert terminal.data["event_type"] == "completed"

        seen = []
        while True:
            event = await subscription.next_event(timeout_sec=2.0)
            if event.data.get("task_id") != "sub-task-1":
                continue
            seen.append(event.data["event_type"])
            if event.data["event_type"] == "completed":
                break

        assert "completed" in seen
    finally:
        await subscription.close()
        await manager.close()
        await grpc_server.stop(None)


@pytest.mark.asyncio
async def test_terminal_waiters_share_terminal_cache_and_do_not_compete():
    manager = MultiNodeManager()
    waiter1 = asyncio.create_task(manager.wait_for_task_terminal("task-1", timeout_sec=1.0))
    waiter2 = asyncio.create_task(manager.wait_for_task_terminal("task-1", timeout_sec=1.0))

    event = FleetEvent(
        node_id="n1",
        kind="task_event",
        request_id="req-1",
        trace_id="trace-1",
        data={"task_id": "task-1", "event_type": "completed", "payload": {"code": 0}, "error_code": ""},
    )
    await manager._publish_event(event)

    resolved1, resolved2 = await asyncio.gather(waiter1, waiter2)
    cached = await manager.wait_for_task_terminal("task-1", timeout_sec=0.1)

    assert resolved1 == event
    assert resolved2 == event
    assert cached == event

    await manager.close()


@pytest.mark.asyncio
async def test_subscription_prefers_dropping_heartbeats_when_queue_full():
    manager = MultiNodeManager(event_queue_size=2)
    subscription = manager.subscribe(max_queue_size=2)

    try:
        await manager._publish_event(
            FleetEvent(node_id="n1", kind="heartbeat", request_id="", trace_id="", data={"metrics": {"cpu": 1}})
        )
        await manager._publish_event(
            FleetEvent(
                node_id="n1",
                kind="task_event",
                request_id="req-1",
                trace_id="trace-1",
                data={"task_id": "task-1", "event_type": "queued", "payload": {}, "error_code": ""},
            )
        )
        await manager._publish_event(
            FleetEvent(
                node_id="n1",
                kind="task_event",
                request_id="req-2",
                trace_id="trace-2",
                data={"task_id": "task-2", "event_type": "running", "payload": {}, "error_code": ""},
            )
        )

        first = await subscription.next_event(timeout_sec=0.1)
        second = await subscription.next_event(timeout_sec=0.1)

        assert first.kind == "task_event"
        assert second.kind == "task_event"
        assert [first.data["task_id"], second.data["task_id"]] == ["task-1", "task-2"]
        assert subscription.dropped_count == 1
    finally:
        await subscription.close()
        await manager.close()


def test_select_node_prefers_low_runtime_load_and_excludes_stale_nodes():
    manager = MultiNodeManager(heartbeat_timeout_sec=1.0)
    manager.add_node(NodeTarget(node_id="n1", address="127.0.0.1:1", api_key="k1"))
    manager.add_node(NodeTarget(node_id="n2", address="127.0.0.1:2", api_key="k2"))
    manager.add_node(NodeTarget(node_id="n3", address="127.0.0.1:3", api_key="k3"))

    now = time.time()
    for node_id in ("n1", "n2", "n3"):
        node = manager._nodes[node_id]
        node.connected = True
        node.authenticated = True
        node.capability = {"gpu_available": False, "gpus": []}
        node.last_seen_at = now
        node.last_heartbeat_at = now

    manager._nodes["n1"].last_heartbeat = {"system_cpu_percent": 95, "system_memory_percent": 80}
    manager._nodes["n2"].last_heartbeat = {"system_cpu_percent": 10, "system_memory_percent": 20}
    manager._nodes["n3"].last_heartbeat = {"system_cpu_percent": 1, "system_memory_percent": 10}
    manager._nodes["n3"].last_heartbeat_at = now - 10

    submit = control_stream_pb2.TaskSubmit(protocol_version="v1", task_id="load-task", command=["echo"], task_type="inference")
    assert manager._select_node_for_task(submit) == "n2"

    manager.disable_node("n2", True)
    assert manager._select_node_for_task(submit) == "n1"

    manager.drain_node("n1", True)
    with pytest.raises(RuntimeError, match="no eligible nodes"):
        manager._select_node_for_task(submit)


@pytest.mark.asyncio
async def test_connect_all_allows_partial_success():
    server = NodeAgentServer(
        api_keys={"good-node": "good-key"},
        capability=Capability(4, 1024, 20, gpu_available=False),
        metrics_interval_sec=0.05,
    )
    grpc_server, port = await server.serve("127.0.0.1:0")

    manager = MultiNodeManager(reconnect_backoff_initial_sec=0.01, reconnect_backoff_max_sec=0.02)
    manager.add_node(NodeTarget(node_id="good-node", address=f"127.0.0.1:{port}", api_key="good-key"))
    manager.add_node(NodeTarget(node_id="bad-node", address=f"127.0.0.1:{port}", api_key="bad-key"))

    try:
        results = await manager.connect_all(timeout_sec=1.0)
        by_node = {item.node_id: item for item in results}
        assert by_node["good-node"].ok is True
        assert by_node["bad-node"].ok is False
        assert by_node["bad-node"].snapshot.authenticated is False
    finally:
        await manager.close()
        await grpc_server.stop(None)


@pytest.mark.asyncio
async def test_node_reconnects_with_backoff(monkeypatch):
    manager = MultiNodeManager(reconnect_backoff_initial_sec=0.01, reconnect_backoff_max_sec=0.02)
    manager.add_node(NodeTarget(node_id="n1", address="127.0.0.1:1", api_key="k1"))

    node = manager._nodes["n1"]
    attempts = {"count": 0}
    sleep_calls = []
    original_sleep = asyncio.sleep

    async def fake_run_single_stream(_node):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise RuntimeError("boom")
        _node.connected = True
        _node.authenticated = True
        _node.capability = {"gpu_available": False, "gpus": []}
        _node.last_seen_at = time.monotonic()
        _node.last_heartbeat_at = _node.last_seen_at
        _node.ready_event.set()
        _node.stop_requested = True

    async def fake_sleep(delay):
        sleep_calls.append(delay)
        await original_sleep(0)

    monkeypatch.setattr(manager, "_run_single_stream", fake_run_single_stream)
    monkeypatch.setattr("asyncio.sleep", fake_sleep)

    result = await manager.connect_node("n1", timeout_sec=1.0)
    assert result.ok is True
    assert attempts["count"] == 2
    assert 0.01 in sleep_calls

    await manager.close()


def test_open_channel_uses_secure_channel_when_tls_configured(monkeypatch):
    manager = MultiNodeManager()
    tls = ClientTLSConfig(root_certificates=b"ca", server_name_override="node.local")
    manager.add_node(NodeTarget(node_id="n1", address="127.0.0.1:1234", api_key="k1", tls=tls))

    captured = {}

    def fake_secure_channel(address, credentials, options=None):
        captured["address"] = address
        captured["credentials"] = credentials
        captured["options"] = options
        return "secure-channel"

    monkeypatch.setattr("grpc.aio.secure_channel", fake_secure_channel)
    monkeypatch.setattr(ClientTLSConfig, "build_channel_credentials", lambda self: "creds")

    channel = manager._open_channel(manager._nodes["n1"])
    assert channel == "secure-channel"
    assert captured["address"] == "127.0.0.1:1234"
    assert captured["credentials"] == "creds"
    assert captured["options"] == [("grpc.ssl_target_name_override", "node.local")]


@pytest.mark.asyncio
async def test_create_grpc_server_uses_secure_port_when_tls_configured(monkeypatch):
    app = NodeAgentServer(api_keys={"n1": "k1"})
    tls = ServerTLSConfig(certificate_chain=b"cert", private_key=b"key", root_certificates=b"ca", require_client_auth=True)
    calls = {}

    class FakeServer:
        def add_secure_port(self, address, credentials):
            calls["address"] = address
            calls["credentials"] = credentials
            return 4321

        def add_insecure_port(self, address):
            raise AssertionError("expected secure port")

    fake_server = FakeServer()

    monkeypatch.setattr("grpc.aio.server", lambda: fake_server)
    monkeypatch.setattr("node_agent.grpc_service.add_servicer_to_server", lambda server, _: calls.setdefault("servicer", server))
    monkeypatch.setattr(ServerTLSConfig, "build_server_credentials", lambda self: "server-creds")

    server, port = await app.create_grpc_server("127.0.0.1:0", tls_config=tls)

    assert server is fake_server
    assert port == 4321
    assert calls["address"] == "127.0.0.1:0"
    assert calls["credentials"] == "server-creds"
    assert calls["servicer"] is fake_server


@pytest.mark.asyncio
async def test_multi_node_manager_routes_tasks_to_matching_nodes(tmp_path):
    cpu_server = NodeAgentServer(
        api_keys={"cpu-node": "cpu-key"},
        capability=Capability(4, 1024, 20, gpu_available=False),
        metrics_interval_sec=0.05,
    )
    gpu_server = NodeAgentServer(
        api_keys={"gpu-node": "gpu-key"},
        capability=Capability(
            8,
            2048,
            20,
            gpu_available=True,
            gpus=[GpuDevice(index=0, name="GPU-0", total_vram_mb=8192)],
            gpu_name="GPU-0",
            gpu_vram_mb=8192,
        ),
        metrics_interval_sec=0.05,
    )

    cpu_grpc, cpu_port = await cpu_server.serve("127.0.0.1:0")
    gpu_grpc, gpu_port = await gpu_server.serve("127.0.0.1:0")

    manager = MultiNodeManager()
    manager.add_node(NodeTarget(node_id="cpu-node", address=f"127.0.0.1:{cpu_port}", api_key="cpu-key"))
    manager.add_node(NodeTarget(node_id="gpu-node", address=f"127.0.0.1:{gpu_port}", api_key="gpu-key"))

    try:
        await manager.connect_all()

        cpu_submit = control_stream_pb2.TaskSubmit(
            protocol_version="v1",
            task_id="cpu-task-1",
            command=[sys.executable, "-c", "print('cpu-task')"],
            task_type="inference",
            workdir=str(tmp_path),
        )
        assigned_cpu_node, _, _ = await manager.submit_task(cpu_submit)
        assert assigned_cpu_node == "cpu-node"
        cpu_terminal = await manager.wait_for_task_terminal("cpu-task-1")
        assert cpu_terminal.node_id == "cpu-node"
        assert cpu_terminal.data["event_type"] == "completed"

        gpu_submit = control_stream_pb2.TaskSubmit(
            protocol_version="v1",
            task_id="gpu-task-1",
            command=[sys.executable, "-c", "print('gpu-task')"],
            task_type="train",
            require_gpu=True,
            workdir=str(tmp_path),
        )
        assigned_gpu_node, _, _ = await manager.submit_task(gpu_submit)
        assert assigned_gpu_node == "gpu-node"
        gpu_terminal = await manager.wait_for_task_terminal("gpu-task-1")
        assert gpu_terminal.node_id == "gpu-node"
        assert gpu_terminal.data["event_type"] == "completed"

        explicit_submit = control_stream_pb2.TaskSubmit(
            protocol_version="v1",
            task_id="cpu-task-2",
            command=[sys.executable, "-c", "print('cpu-explicit')"],
            task_type="inference",
            workdir=str(tmp_path),
        )
        explicit_node, _, _ = await manager.submit_task(explicit_submit, node_id="cpu-node")
        assert explicit_node == "cpu-node"
        explicit_terminal = await manager.wait_for_task_terminal("cpu-task-2")
        assert explicit_terminal.node_id == "cpu-node"
        assert explicit_terminal.data["event_type"] == "completed"
    finally:
        await manager.close()
        await cpu_grpc.stop(None)
        await gpu_grpc.stop(None)


@pytest.mark.asyncio
async def test_task_state_persists_across_manager_restart(tmp_path):
    db_path = tmp_path / "fleet.sqlite3"
    repo1 = SQLiteFleetRepository(str(db_path))
    repo2 = SQLiteFleetRepository(str(db_path))

    server = NodeAgentServer(
        api_keys={"n1": "k1"},
        capability=Capability(4, 1024, 20, gpu_available=False),
        metrics_interval_sec=0.05,
    )
    grpc_server, port = await server.serve("127.0.0.1:0")

    manager1 = MultiNodeManager(store=repo1, controller_id="c1")
    manager1.add_node(NodeTarget(node_id="n1", address=f"127.0.0.1:{port}", api_key="k1"))

    try:
        result = await manager1.connect_node("n1")
        assert result.ok is True

        submit = control_stream_pb2.TaskSubmit(
            protocol_version="v1",
            task_id="persist-task-1",
            command=[sys.executable, "-c", "print('persisted')"],
            task_type="inference",
            workdir=str(tmp_path),
        )
        assigned_node, _, _ = await manager1.submit_task(submit)
        assert assigned_node == "n1"
        terminal = await manager1.wait_for_task_terminal("persist-task-1")
        assert terminal.data["event_type"] == "completed"

        await manager1.close()

        manager2 = MultiNodeManager(store=repo2, controller_id="c2")
        try:
            listed = manager2.list_nodes()
            assert [item.node_id for item in listed] == ["n1"]
            task = manager2.get_task("persist-task-1")
            assert task.status == "completed"
            restored = await manager2.wait_for_task_terminal("persist-task-1", timeout_sec=0.1)
            assert restored.data["event_type"] == "completed"
        finally:
            await manager2.close()
    finally:
        await grpc_server.stop(None)
        repo1.close()
        repo2.close()


@pytest.mark.asyncio
async def test_shared_storage_persists_logs_and_artifacts(tmp_path):
    db_path = tmp_path / "shared.sqlite3"
    shared_root = tmp_path / "shared"
    repo = SQLiteFleetRepository(str(db_path))
    storage = FileSystemSharedTaskStorage(shared_root)

    server = NodeAgentServer(
        api_keys={"n1": "k1"},
        capability=Capability(4, 1024, 20, gpu_available=False),
        metrics_interval_sec=0.05,
    )
    grpc_server, port = await server.serve("127.0.0.1:0")

    manager = MultiNodeManager(store=repo, shared_storage=storage, controller_id="c1")
    manager.add_node(NodeTarget(node_id="n1", address=f"127.0.0.1:{port}", api_key="k1"))

    artifact_path = tmp_path / "artifact.txt"
    artifact_payload = json.dumps({"name": "artifact.txt", "path": str(artifact_path)})
    command = [
        sys.executable,
        "-c",
        (
            "from pathlib import Path; "
            f"p = Path(r'{artifact_path}'); "
            "p.write_text('artifact-data', encoding='utf-8'); "
            "print('hello-log'); "
            f"print('__ARTIFACT__ ' + {artifact_payload!r})"
        ),
    ]

    try:
        result = await manager.connect_node("n1")
        assert result.ok is True

        submit = control_stream_pb2.TaskSubmit(
            protocol_version="v1",
            task_id="artifact-task-1",
            command=command,
            task_type="inference",
            workdir=str(tmp_path),
        )
        assigned_node, _, _ = await manager.submit_task(submit)
        assert assigned_node == "n1"
        terminal = await manager.wait_for_task_terminal("artifact-task-1")
        assert terminal.data["event_type"] == "completed"

        log_path = shared_root / "artifact-task-1" / "logs" / "stdout.log"
        copied_artifact = shared_root / "artifact-task-1" / "artifacts" / "artifact.txt"
        assert "hello-log" in log_path.read_text(encoding="utf-8")
        assert copied_artifact.read_text(encoding="utf-8") == "artifact-data"

        artifact_events = [
            item
            for item in manager.list_task_events("artifact-task-1")
            if item.kind == "task_event" and item.data.get("event_type") == "artifact"
        ]
        assert artifact_events
        assert artifact_events[0].data["payload"]["shared_artifact_path"] == str(copied_artifact)
    finally:
        await manager.close()
        await grpc_server.stop(None)
        repo.close()


@pytest.mark.asyncio
async def test_leader_failover_dispatches_persisted_tasks(tmp_path):
    db_path = tmp_path / "failover.sqlite3"
    repo1 = SQLiteFleetRepository(str(db_path))
    repo2 = SQLiteFleetRepository(str(db_path))

    server = NodeAgentServer(
        api_keys={"n1": "k1"},
        capability=Capability(4, 1024, 20, gpu_available=False),
        metrics_interval_sec=0.05,
    )
    grpc_server, port = await server.serve("127.0.0.1:0")

    manager1 = MultiNodeManager(store=repo1, controller_id="leader-a", assignment_timeout_sec=2.0)
    manager1.add_node(NodeTarget(node_id="n1", address=f"127.0.0.1:{port}", api_key="k1"))

    try:
        await manager1.connect_all()
        assert await manager1.wait_until_leader(timeout_sec=2.0) is True

        manager2 = MultiNodeManager(store=repo2, controller_id="leader-b", assignment_timeout_sec=2.0)
        try:
            await manager2.start()
            await asyncio.sleep(0.2)
            assert manager2.is_leader is False

            submit1 = control_stream_pb2.TaskSubmit(
                protocol_version="v1",
                task_id="handoff-task-1",
                command=[sys.executable, "-c", "print('first-controller')"],
                task_type="inference",
                workdir=str(tmp_path),
            )
            assigned1, _, _ = await manager2.submit_task(submit1)
            assert assigned1 == "n1"
            terminal1 = await manager2.wait_for_task_terminal("handoff-task-1", timeout_sec=5.0)
            assert terminal1.data["event_type"] == "completed"

            await manager1.close()
            assert await manager2.wait_until_leader(timeout_sec=5.0) is True
            deadline = time.time() + 5.0
            while time.time() < deadline:
                node = manager2.get_node("n1")
                if node.connected and node.authenticated:
                    break
                await asyncio.sleep(0.1)
            node = manager2.get_node("n1")
            assert node.connected is True
            assert node.authenticated is True

            submit2 = control_stream_pb2.TaskSubmit(
                protocol_version="v1",
                task_id="handoff-task-2",
                command=[sys.executable, "-c", "print('second-controller')"],
                task_type="inference",
                workdir=str(tmp_path),
            )
            assigned2, _, _ = await manager2.submit_task(submit2)
            assert assigned2 == "n1"
            terminal2 = await manager2.wait_for_task_terminal("handoff-task-2", timeout_sec=5.0)
            assert terminal2.data["event_type"] == "completed"
        finally:
            await manager2.close()
    finally:
        await grpc_server.stop(None)
        repo1.close()
        repo2.close()


@pytest.mark.asyncio
async def test_stale_dispatching_task_is_requeued_after_failover(tmp_path):
    db_path = tmp_path / "requeue.sqlite3"
    repo1 = SQLiteFleetRepository(str(db_path))
    repo2 = SQLiteFleetRepository(str(db_path))

    server = NodeAgentServer(
        api_keys={"n1": "k1"},
        capability=Capability(4, 1024, 20, gpu_available=False),
        metrics_interval_sec=0.05,
    )
    grpc_server, port = await server.serve("127.0.0.1:0")

    manager1 = MultiNodeManager(
        store=repo1,
        controller_id="leader-a",
        assignment_timeout_sec=2.0,
        dispatch_claim_timeout_sec=0.2,
    )
    manager1.add_node(NodeTarget(node_id="n1", address=f"127.0.0.1:{port}", api_key="k1"))

    try:
        await manager1.connect_all()
        assert await manager1.wait_until_leader(timeout_sec=2.0) is True

        stale_submit = control_stream_pb2.TaskSubmit(
            protocol_version="v1",
            task_id="stale-dispatch-1",
            command=[sys.executable, "-c", "print('requeued-task')"],
            task_type="inference",
            workdir=str(tmp_path),
        )
        created_at = time.time() - 5.0
        repo1.create_task(
            task_id="stale-dispatch-1",
            request_id="req-stale-1",
            trace_id="trace-stale-1",
            submit_payload=manager1._task_submit_to_payload(stale_submit),
            requested_node_id="",
            created_at=created_at,
        )
        assert repo1.claim_task("stale-dispatch-1", node_id="n1", updated_at=time.time() - 5.0) is True

        manager2 = MultiNodeManager(
            store=repo2,
            controller_id="leader-b",
            assignment_timeout_sec=2.0,
            dispatch_claim_timeout_sec=0.2,
        )
        try:
            await manager2.start()
            await manager1.close()
            assert await manager2.wait_until_leader(timeout_sec=5.0) is True

            terminal = await manager2.wait_for_task_terminal("stale-dispatch-1", timeout_sec=5.0)
            assert terminal.data["event_type"] == "completed"

            task = manager2.get_task("stale-dispatch-1")
            assert task.status == "completed"
            assert task.dispatch_attempts >= 2

            task_events = manager2.list_task_events("stale-dispatch-1")
            assert any(item.kind == "task_requeued" for item in task_events)
        finally:
            await manager2.close()
    finally:
        await grpc_server.stop(None)
        repo1.close()
        repo2.close()
