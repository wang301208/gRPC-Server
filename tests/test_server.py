import asyncio
import sys
from unittest.mock import Mock

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


def test_control_stream_cleanup_when_incoming_raises():
    async def _run():
        server = NodeAgentServer(api_keys={"n1": "k1"})
        session = NodeSession(node_id="n1", api_key="k1")

        stop_mock = Mock()
        server.metrics.stop = stop_mock

        submit_cancelled = asyncio.Event()

        submit_started = asyncio.Event()

        async def _failing_incoming():
            yield {
                "type": "task_submit",
                "task": {
                    "task_id": "s3",
                    "command": [sys.executable, "-c", "print('hello')"],
                    "task_type": "inference",
                    "require_gpu": False,
                    "prefer_gpu": False,
                    "env": {},
                },
            }
            await submit_started.wait()
            raise RuntimeError("incoming failed")

        async def _blocked_submit(req, push_event):
            submit_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                submit_cancelled.set()
                raise

        server.task_manager.submit = _blocked_submit

        with_raised = False
        try:
            async for _ in server.control_stream(session, _failing_incoming()):
                pass
        except RuntimeError as exc:
            with_raised = True
            assert str(exc) == "incoming failed"

        assert with_raised
        assert stop_mock.call_count == 1
        assert submit_cancelled.is_set()

    asyncio.run(_run())
