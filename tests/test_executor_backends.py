import asyncio

from node_agent.capability import Capability
from node_agent.executors import DockerExecutor
from node_agent.models import TaskEvent, TaskRequest, TaskRuntime, TaskStatus
from node_agent.scheduler import ResourceScheduler
from node_agent.server import NodeAgentServer
from node_agent.task_manager import TaskManager


class _FakeExecutor:
    """用于验证调用链的假执行器。"""

    def __init__(self):
        self.run_calls = []
        self.cancel_calls = []

    async def run_task(self, request, on_event):
        self.run_calls.append(request.task_id)
        on_event(TaskEvent(task_id=request.task_id, event_type="completed", payload={"code": 0}))
        return TaskStatus.COMPLETED

    async def cancel(self, task_id):
        self.cancel_calls.append(task_id)
        return True


def test_node_agent_server_uses_configured_backend(monkeypatch):
    captured = {}

    def fake_build_executor(backend):
        captured["backend"] = backend
        return _FakeExecutor()

    monkeypatch.setattr("node_agent.server.build_executor", fake_build_executor)
    server = NodeAgentServer(api_keys={"n1": "k1"}, executor_backend="docker")

    assert captured["backend"] == "docker"
    assert isinstance(server.task_manager.executor, _FakeExecutor)


def test_task_manager_submit_and_cancel_call_executor():
    async def _run():
        cap = Capability(4, 1024, 20, gpu_available=False)
        fake_executor = _FakeExecutor()
        manager = TaskManager(ResourceScheduler(cap), executor=fake_executor)

        events = []
        req = TaskRequest(task_id="m1", command=["echo", "ok"], task_type="inference")
        status = await manager.submit(req, events.append)

        assert status == TaskStatus.COMPLETED
        assert fake_executor.run_calls == ["m1"]

        manager.tasks["m2"] = TaskRuntime(request=TaskRequest(task_id="m2", command=["x"], task_type="inference"))
        canceled = await manager.cancel("m2", events.append)
        assert canceled is True
        assert fake_executor.cancel_calls == ["m2"]

    asyncio.run(_run())


def test_docker_executor_build_command_gpu_and_env(tmp_path):
    workdir = tmp_path / "job"
    workdir.mkdir()
    request = TaskRequest(
        task_id="d1",
        command=["python", "-c", "print('ok')"],
        task_type="train",
        require_gpu=True,
        env={"A": "B"},
        workdir=str(workdir),
    )

    executor = DockerExecutor(cpu_image="cpu:latest", gpu_image="gpu:latest")
    cmd = executor._build_command(request)

    assert cmd[:3] == ["docker", "run", "--rm"]
    assert "--gpus" in cmd
    assert "gpu:latest" in cmd
    assert "-e" in cmd
    assert f"A=B" in cmd
    assert "-w" in cmd
    assert str(workdir) in cmd
