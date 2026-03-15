import sys

import grpc
import pytest
from google.protobuf.json_format import MessageToDict

from node_agent.capability import Capability
from node_agent.grpc_service import control_stream_pb2, control_stream_pb2_grpc
from node_agent.server import NodeAgentServer


@pytest.mark.asyncio
async def test_grpc_stream_task_flow_and_trace_passthrough(tmp_path):
    app = NodeAgentServer(
        api_keys={"n1": "k1"},
        capability=Capability(4, 1024, 20, gpu_available=False),
        metrics_interval_sec=0.05,
    )
    server, port = await app.serve("127.0.0.1:0")

    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            await channel.channel_ready()
            stub = control_stream_pb2_grpc.ControlStreamStub(channel)

            async def _requests():
                yield control_stream_pb2.ControlRequest(
                    request_id="req-grpc-1",
                    trace_id="trace-grpc-1",
                    task_submit=control_stream_pb2.TaskSubmit(
                        protocol_version="v1",
                        task_id="grpc-task-1",
                        command=[sys.executable, "-c", "print('grpc-hello')"],
                        task_type="inference",
                        workdir=str(tmp_path),
                    ),
                )
                yield control_stream_pb2.ControlRequest(
                    close=control_stream_pb2.Close(protocol_version="v1"),
                )

            responses = [item async for item in stub.Stream(_requests(), metadata=(("x-node-id", "n1"), ("x-api-key", "k1")))]

        assert responses
        assert responses[0].WhichOneof("payload") == "node_hello"
        assert responses[0].node_hello.capability.cpu_cores == 4

        task_events = [item.task_event for item in responses if item.WhichOneof("payload") == "task_event"]
        assert task_events
        assert {item.task_id for item in task_events} == {"grpc-task-1"}
        assert {item.request_id for item in responses if item.WhichOneof("payload") == "task_event"} == {"req-grpc-1"}
        assert {item.trace_id for item in responses if item.WhichOneof("payload") == "task_event"} == {"trace-grpc-1"}

        completed = [item for item in task_events if item.event_type == "completed"]
        assert completed

        logs = [
            MessageToDict(item.payload, preserving_proto_field_name=True).get("line")
            for item in task_events
            if item.event_type == "log"
        ]
        assert "grpc-hello" in logs
    finally:
        await server.stop(None)


@pytest.mark.asyncio
async def test_grpc_stream_auth_failed_via_authorization_metadata():
    app = NodeAgentServer(
        api_keys={"n1": "k1"},
        capability=Capability(4, 1024, 20, gpu_available=False),
    )
    server, port = await app.serve("127.0.0.1:0")

    try:
        async with grpc.aio.insecure_channel(f"127.0.0.1:{port}") as channel:
            await channel.channel_ready()
            stub = control_stream_pb2_grpc.ControlStreamStub(channel)

            async def _requests():
                yield control_stream_pb2.ControlRequest(
                    close=control_stream_pb2.Close(protocol_version="v1"),
                )

            responses = [
                item
                async for item in stub.Stream(
                    _requests(),
                    metadata=(("x-node-id", "n1"), ("authorization", "Bearer bad-key")),
                )
            ]

        assert responses
        assert responses[0].WhichOneof("payload") == "auth_failed"
        assert responses[0].auth_failed.error_code == "AUTH_FAILED"
        assert responses[0].auth_failed.reason == "节点鉴权失败"
    finally:
        await server.stop(None)
