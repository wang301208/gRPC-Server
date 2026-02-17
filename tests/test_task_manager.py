import asyncio
import os
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
