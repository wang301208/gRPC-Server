import asyncio
import sys

from node_agent.capability import Capability
from node_agent.executor import Executor
from node_agent.models import TaskEvent, TaskRequest, TaskStatus
from node_agent.scheduler import ResourceScheduler
from node_agent.task_manager import TaskManager


class StubExecutor(Executor):
    """测试用执行器，模拟运行/取消行为。"""

    def __init__(self, backend_name: str):
        self._backend_name = backend_name
        self._statuses = {}
        self.canceled = []

    @property
    def backend_name(self) -> str:
        return self._backend_name

    async def run(self, request: TaskRequest, on_event):
        self._statuses[request.task_id] = TaskStatus.RUNNING
        on_event(TaskEvent(task_id=request.task_id, event_type="running", payload={"source": self._backend_name}))

        for item in request.command:
            if item.startswith("sleep:"):
                await asyncio.sleep(float(item.split(":", 1)[1]))
            if self._statuses.get(request.task_id) == TaskStatus.CANCELED:
                return TaskStatus.CANCELED

        on_event(TaskEvent(task_id=request.task_id, event_type="log", payload={"line": f"{self._backend_name}-log"}))
        self._statuses[request.task_id] = TaskStatus.COMPLETED
        on_event(TaskEvent(task_id=request.task_id, event_type="completed", payload={"code": 0}))
        return TaskStatus.COMPLETED

    async def cancel(self, task_id: str, grace_sec: float = 1.5) -> bool:
        self.canceled.append((task_id, grace_sec))
        if self._statuses.get(task_id) != TaskStatus.RUNNING:
            return False
        self._statuses[task_id] = TaskStatus.CANCELED
        return True

    def status(self, task_id: str):
        return self._statuses.get(task_id)


def _manager_with_stubs(backend_by_task_type=None):
    cap = Capability(4, 1024, 20, gpu_available=False)
    subprocess_stub = StubExecutor("subprocess")
    docker_stub = StubExecutor("docker")
    manager = TaskManager(
        ResourceScheduler(cap),
        executors={"subprocess": subprocess_stub, "docker": docker_stub},
        backend_by_task_type=backend_by_task_type or {},
    )
    return manager, subprocess_stub, docker_stub


def test_submit_task_success():
    async def _run():
        cap = Capability(4, 1024, 20, gpu_available=False)
        manager = TaskManager(ResourceScheduler(cap))
        events = []

        req = TaskRequest(
            task_id="t1",
            command=[sys.executable, "-c", "print('ok')"],
            task_type="inference",
        )
        status = await manager.submit(req, events.append)
        assert status.value == "completed"
        assert any(e.event_type == "log" for e in events)

    asyncio.run(_run())


def test_cancel_task():
    async def _run():
        cap = Capability(4, 1024, 20, gpu_available=False)
        manager = TaskManager(ResourceScheduler(cap))
        events = []

        req = TaskRequest(
            task_id="t2",
            command=[sys.executable, "-c", "import time; time.sleep(5)"],
            task_type="train",
        )

        run_task = asyncio.create_task(manager.submit(req, events.append))
        await asyncio.sleep(0.5)
        canceled = await manager.cancel("t2", events.append)
        assert canceled
        await run_task
        assert any(e.event_type == "canceled" for e in events)

    asyncio.run(_run())


def test_submit_task_keeps_system_env():
    async def _run():
        cap = Capability(4, 1024, 20, gpu_available=False)
        manager = TaskManager(ResourceScheduler(cap))
        events = []

        req = TaskRequest(
            task_id="t3",
            command=[sys.executable, "-c", "import os; print('custom=' + os.getenv('CUSTOM_ENV', '')); print('has_path=' + str(bool(os.getenv('PATH'))))"],
            task_type="inference",
            env={"CUSTOM_ENV": "yes"},
        )
        status = await manager.submit(req, events.append)
        assert status.value == "completed"

        logs = [e.payload.get("line", "") for e in events if e.event_type == "log"]
        assert any("custom=yes" in line for line in logs)
        assert any("has_path=True" in line for line in logs)

    asyncio.run(_run())


def test_submit_task_command_not_found():
    async def _run():
        cap = Capability(4, 1024, 20, gpu_available=False)
        manager = TaskManager(ResourceScheduler(cap))
        events = []

        req = TaskRequest(
            task_id="t4",
            command=["__definitely_not_existing_command__"],
            task_type="inference",
        )
        status = await manager.submit(req, events.append)
        assert status.value == "failed"

        failed_events = [e for e in events if e.event_type == "failed"]
        assert failed_events
        assert failed_events[0].payload["reason"].startswith("命令不存在")

    asyncio.run(_run())


def test_submit_task_allow_resubmit_after_completed():
    async def _run():
        cap = Capability(4, 1024, 20, gpu_available=False)
        manager = TaskManager(ResourceScheduler(cap))
        events = []

        first_req = TaskRequest(
            task_id="t5",
            command=[sys.executable, "-c", "print('first')"],
            task_type="inference",
        )
        first_status = await manager.submit(first_req, events.append)
        assert first_status.value == "completed"

        second_req = TaskRequest(
            task_id="t5",
            command=[sys.executable, "-c", "print('second')"],
            task_type="inference",
        )
        second_status = await manager.submit(second_req, events.append)
        assert second_status.value == "completed"

        logs = [e.payload.get("line", "") for e in events if e.event_type == "log"]
        assert any("first" in line for line in logs)
        assert any("second" in line for line in logs)

    asyncio.run(_run())


def test_submit_task_history_limit_works():
    async def _run():
        cap = Capability(4, 1024, 20, gpu_available=False)
        manager = TaskManager(ResourceScheduler(cap), retain_history=True, history_limit=2)
        events = []

        for index in range(4):
            req = TaskRequest(
                task_id="t6",
                command=[sys.executable, "-c", f"print('run-{index}')"],
                task_type="inference",
            )
            status = await manager.submit(req, events.append)
            assert status.value == "completed"

        history = manager.task_history["t6"]
        assert len(history) == 2
        assert [task.request.command[-1] for task in history] == ["print('run-1')", "print('run-2')"]
        assert manager.tasks["t6"].request.command[-1] == "print('run-3')"

    asyncio.run(_run())


def test_backend_selection_by_task_type_and_env_override():
    async def _run():
        manager, _, _ = _manager_with_stubs(backend_by_task_type={"container": "docker"})
        events = []

        docker_req = TaskRequest(task_id="b1", command=["echo"], task_type="container")
        status = await manager.submit(docker_req, events.append)
        assert status == TaskStatus.COMPLETED
        assert manager.task_backends["b1"] == "docker"

        override_req = TaskRequest(
            task_id="b2",
            command=["echo"],
            task_type="container",
            env={"TASK_BACKEND": "subprocess"},
        )
        status = await manager.submit(override_req, events.append)
        assert status == TaskStatus.COMPLETED
        assert manager.task_backends["b2"] == "subprocess"

    asyncio.run(_run())


def test_cancel_behavior_consistent_across_backends():
    async def _run():
        manager, subprocess_stub, docker_stub = _manager_with_stubs(backend_by_task_type={"container": "docker"})

        task_sub = TaskRequest(task_id="c1", command=["sleep:2"], task_type="inference")
        task_docker = TaskRequest(task_id="c2", command=["sleep:2"], task_type="container")

        sub_events = []
        docker_events = []

        sub_job = asyncio.create_task(manager.submit(task_sub, sub_events.append))
        docker_job = asyncio.create_task(manager.submit(task_docker, docker_events.append))
        await asyncio.sleep(0.2)

        assert await manager.cancel("c1", sub_events.append)
        assert await manager.cancel("c2", docker_events.append)
        await asyncio.gather(sub_job, docker_job)

        assert subprocess_stub.status("c1") == TaskStatus.CANCELED
        assert docker_stub.status("c2") == TaskStatus.CANCELED
        assert any(e.event_type == "canceled" for e in sub_events)
        assert any(e.event_type == "canceled" for e in docker_events)

    asyncio.run(_run())


def test_log_event_format_consistent_with_backend_field():
    async def _run():
        manager, _, _ = _manager_with_stubs(backend_by_task_type={"container": "docker"})
        events = []

        req = TaskRequest(task_id="f1", command=["echo"], task_type="container")
        await manager.submit(req, events.append)

        log_events = [e for e in events if e.event_type == "log"]
        assert log_events
        for event in events:
            assert "backend" in event.payload
        assert set(event.payload["backend"] for event in log_events) == {"docker"}

    asyncio.run(_run())
