import asyncio
import sys

from node_agent.capability import Capability
from node_agent.models import TaskRequest
from node_agent.scheduler import ResourceScheduler
from node_agent.task_manager import TaskManager


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
