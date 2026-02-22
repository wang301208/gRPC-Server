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


def test_high_priority_task_can_jump_queue():
    async def _run():
        cap = Capability(4, 1024, 20, gpu_available=False)
        manager = TaskManager(ResourceScheduler(cap), max_concurrency=1)
        events = []

        low_1 = TaskRequest(
            task_id="p-low-1",
            command=[sys.executable, "-c", "import time; time.sleep(0.2); print('low-1')"],
            task_type="inference",
            priority=1,
        )
        low_2 = TaskRequest(
            task_id="p-low-2",
            command=[sys.executable, "-c", "print('low-2')"],
            task_type="inference",
            priority=1,
        )
        high = TaskRequest(
            task_id="p-high",
            command=[sys.executable, "-c", "print('high')"],
            task_type="inference",
            priority=10,
        )

        first = asyncio.create_task(manager.submit(low_1, events.append))
        second = asyncio.create_task(manager.submit(low_2, events.append))
        await asyncio.sleep(0.05)
        third = asyncio.create_task(manager.submit(high, events.append))

        assert (await first).value == "completed"
        assert (await second).value == "completed"
        assert (await third).value == "completed"

        completed_order = [e.task_id for e in events if e.event_type == "completed"]
        assert completed_order == ["p-low-1", "p-high", "p-low-2"]

    asyncio.run(_run())


def test_gpu_insufficient_queue_then_timeout_fail():
    async def _run():
        cap = Capability(8, 4096, 20, gpu_available=True, gpu_vram_mb=4096)
        manager = TaskManager(ResourceScheduler(cap, gpu_vram_threshold_mb=2048), max_concurrency=2)
        events = []

        task_a = TaskRequest(
            task_id="gpu-a",
            command=[sys.executable, "-c", "import time; time.sleep(0.8); print('a')"],
            task_type="train",
            require_gpu=True,
            timeout_sec=2,
            resource_request={"cpu_cores": 1, "memory_mb": 128, "gpu_vram_mb": 2048},
        )
        task_b = TaskRequest(
            task_id="gpu-b",
            command=[sys.executable, "-c", "import time; time.sleep(0.8); print('b')"],
            task_type="train",
            require_gpu=True,
            timeout_sec=2,
            resource_request={"cpu_cores": 1, "memory_mb": 128, "gpu_vram_mb": 2048},
        )
        task_c = TaskRequest(
            task_id="gpu-c",
            command=[sys.executable, "-c", "print('c')"],
            task_type="train",
            require_gpu=True,
            timeout_sec=0.2,
            resource_request={"cpu_cores": 1, "memory_mb": 128, "gpu_vram_mb": 2048},
        )

        run_a = asyncio.create_task(manager.submit(task_a, events.append))
        run_b = asyncio.create_task(manager.submit(task_b, events.append))
        await asyncio.sleep(0.05)
        status_c = await manager.submit(task_c, events.append)

        assert status_c.value == "failed"
        assert any(e.task_id == "gpu-c" and e.event_type == "failed" and e.payload.get("reason") == "任务等待超时" for e in events)

        assert (await run_a).value == "completed"
        assert (await run_b).value == "completed"

    asyncio.run(_run())
