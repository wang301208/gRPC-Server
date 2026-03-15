from __future__ import annotations

import asyncio
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from typing import Any, AsyncIterator, Mapping, Sequence

import grpc
from google.protobuf.json_format import MessageToDict

from node_agent.grpc_gen import control_stream_pb2, control_stream_pb2_grpc
from node_agent.persistence import (
    FleetRepository,
    SQLiteFleetRepository,
    StoredEvent,
    StoredNode,
    StoredTask,
    decode_tls_config,
    encode_tls_config,
    monotonic_now,
)
from node_agent.security import ClientTLSConfig
from node_agent.shared_storage import NullSharedTaskStorage, SharedTaskStorage


@dataclass(slots=True)
class NodeTarget:
    node_id: str
    address: str
    api_key: str
    metadata: dict[str, str] = field(default_factory=dict)
    tls: ClientTLSConfig | None = None


@dataclass(slots=True)
class FleetEvent:
    node_id: str
    kind: str
    request_id: str
    trace_id: str
    data: dict[str, Any]


@dataclass(slots=True)
class NodeSnapshot:
    node_id: str
    address: str
    connected: bool
    authenticated: bool
    capability: dict[str, Any]
    last_heartbeat: dict[str, Any]
    active_tasks: list[str]
    last_error: str
    last_seen_at: float
    last_heartbeat_at: float
    reconnect_attempt: int
    disabled: bool
    draining: bool
    stale: bool
    eligible: bool
    controller_id: str


@dataclass(slots=True)
class TaskSnapshot:
    task_id: str
    status: str
    request_id: str
    trace_id: str
    requested_node_id: str
    assigned_node_id: str
    submit_payload: dict[str, Any]
    last_event_type: str
    terminal_payload: dict[str, Any]
    dispatch_attempts: int
    created_at: float
    updated_at: float


@dataclass(slots=True)
class ConnectResult:
    node_id: str
    ok: bool
    snapshot: NodeSnapshot
    error: str = ""


@dataclass(slots=True)
class _ManagedNode:
    target: NodeTarget
    send_queue: asyncio.Queue[control_stream_pb2.ControlRequest | None] = field(default_factory=asyncio.Queue)
    ready_event: asyncio.Event = field(default_factory=asyncio.Event)
    stream_task: asyncio.Task[None] | None = None
    channel: grpc.aio.Channel | None = None
    connected: bool = False
    authenticated: bool = False
    capability: dict[str, Any] = field(default_factory=dict)
    last_heartbeat: dict[str, Any] = field(default_factory=dict)
    active_tasks: dict[str, str] = field(default_factory=dict)
    last_error: str = ""
    last_seen_at: float = 0.0
    last_heartbeat_at: float = 0.0
    reconnect_attempt: int = 0
    stop_requested: bool = False
    auth_failed: bool = False
    draining: bool = False
    disabled: bool = False


class EventSubscription:
    def __init__(
        self,
        manager: MultiNodeManager,
        *,
        node_id: str | None = None,
        kinds: Sequence[str] | None = None,
        max_queue_size: int = 256,
    ) -> None:
        self._manager = manager
        self.node_id = node_id
        self.kinds = set(kinds) if kinds is not None else None
        self.max_queue_size = max(1, max_queue_size)
        self.dropped_count = 0
        self._buffer: list[FleetEvent] = []
        self._condition = asyncio.Condition()
        self._closed = False

    def accepts(self, event: FleetEvent) -> bool:
        if self._closed:
            return False
        if self.node_id is not None and event.node_id != self.node_id:
            return False
        if self.kinds is not None and event.kind not in self.kinds:
            return False
        return True

    async def publish(self, event: FleetEvent) -> None:
        if not self.accepts(event):
            return
        async with self._condition:
            self._enqueue(event)
            self._condition.notify(1)

    async def next_event(self, timeout_sec: float | None = None) -> FleetEvent:
        if timeout_sec is None:
            return await self._next_event()
        return await asyncio.wait_for(self._next_event(), timeout=timeout_sec)

    async def close(self) -> None:
        async with self._condition:
            if self._closed:
                return
            self._closed = True
            self._condition.notify_all()
        self._manager._unsubscribe(self)

    async def _next_event(self) -> FleetEvent:
        async with self._condition:
            while not self._buffer:
                if self._closed:
                    raise RuntimeError("subscription closed")
                await self._condition.wait()
            return self._buffer.pop(0)

    def _enqueue(self, event: FleetEvent) -> None:
        if len(self._buffer) >= self.max_queue_size:
            if event.kind == "heartbeat":
                self.dropped_count += 1
                return
            self._drop_oldest()
        self._buffer.append(event)

    def _drop_oldest(self) -> None:
        for index, existing in enumerate(self._buffer):
            if existing.kind == "heartbeat":
                self._buffer.pop(index)
                self.dropped_count += 1
                return
        if self._buffer:
            self._buffer.pop(0)
            self.dropped_count += 1


class MultiNodeManager:
    TERMINAL_TASK_EVENTS = {"completed", "failed", "canceled", "rejected", "cancel_failed"}

    def __init__(
        self,
        *,
        store: FleetRepository | None = None,
        shared_storage: SharedTaskStorage | None = None,
        controller_id: str | None = None,
        lease_name: str = "scheduler",
        lease_ttl_sec: float = 3.0,
        dispatch_interval_sec: float = 0.1,
        heartbeat_timeout_sec: float = 5.0,
        reconnect_backoff_initial_sec: float = 0.5,
        reconnect_backoff_max_sec: float = 5.0,
        event_queue_size: int = 256,
        terminal_cache_size: int = 1024,
        assignment_timeout_sec: float = 5.0,
        dispatch_claim_timeout_sec: float | None = None,
        auto_connect_on_leadership: bool = True,
    ) -> None:
        self.store = store or SQLiteFleetRepository()
        self.shared_storage = shared_storage or NullSharedTaskStorage()
        self.controller_id = controller_id or uuid.uuid4().hex
        self.lease_name = lease_name
        self.lease_ttl_sec = lease_ttl_sec
        self.dispatch_interval_sec = dispatch_interval_sec
        self.heartbeat_timeout_sec = heartbeat_timeout_sec
        self.reconnect_backoff_initial_sec = reconnect_backoff_initial_sec
        self.reconnect_backoff_max_sec = reconnect_backoff_max_sec
        self.terminal_cache_size = terminal_cache_size
        self.assignment_timeout_sec = assignment_timeout_sec
        self.dispatch_claim_timeout_sec = dispatch_claim_timeout_sec or max(assignment_timeout_sec, lease_ttl_sec * 2.0)
        self.auto_connect_on_leadership = auto_connect_on_leadership

        self._owns_store = store is None
        self._owns_shared_storage = shared_storage is None
        self._nodes: dict[str, _ManagedNode] = {}
        self._task_to_node: dict[str, str] = {}
        self._subscriptions: list[EventSubscription] = []
        self._task_waiters: dict[str, list[asyncio.Future[FleetEvent]]] = {}
        self._assignment_waiters: dict[str, list[asyncio.Future[str]]] = {}
        self._task_terminal_cache: OrderedDict[str, FleetEvent] = OrderedDict()
        self._default_subscription = self.subscribe(max_queue_size=event_queue_size)
        self._dispatch_wakeup = asyncio.Event()
        self._lease_ready = asyncio.Event()
        self._coordination_started = False
        self._closing = False
        self._is_leader = False
        self._leadership_task: asyncio.Task[None] | None = None
        self._scheduler_task: asyncio.Task[None] | None = None

        self._restore_nodes_from_store()

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    async def start(self) -> None:
        if self._coordination_started:
            return
        self._coordination_started = True
        self._leadership_task = asyncio.create_task(self._leadership_loop())
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        await self._lease_ready.wait()

    async def wait_until_leader(self, timeout_sec: float = 5.0) -> bool:
        await self.start()
        deadline = monotonic_now() + timeout_sec
        while monotonic_now() < deadline:
            if self._is_leader:
                return True
            await asyncio.sleep(min(self.dispatch_interval_sec, 0.1))
        return self._is_leader

    def add_node(self, target: NodeTarget) -> None:
        if target.node_id in self._nodes:
            raise ValueError(f"node already exists: {target.node_id}")
        node = _ManagedNode(target=target)
        self._nodes[target.node_id] = node
        self._persist_node_state(node)
        self._dispatch_wakeup.set()

    def remove_node(self, node_id: str) -> None:
        node = self._require_node(node_id)
        if node.stream_task is not None and not node.stream_task.done():
            raise RuntimeError(f"node still connected: {node_id}")
        self._nodes.pop(node_id, None)
        self.store.delete_node(node_id)

    def subscribe(
        self,
        *,
        node_id: str | None = None,
        kinds: Sequence[str] | None = None,
        max_queue_size: int = 256,
    ) -> EventSubscription:
        subscription = EventSubscription(self, node_id=node_id, kinds=kinds, max_queue_size=max_queue_size)
        self._subscriptions.append(subscription)
        return subscription

    def list_nodes(self) -> list[NodeSnapshot]:
        return [self._snapshot_from_record(item) for item in self.store.list_nodes()]

    def get_node(self, node_id: str) -> NodeSnapshot:
        record = self.store.get_node(node_id)
        if record is None:
            raise KeyError(f"node not found: {node_id}")
        return self._snapshot_from_record(record)

    def list_tasks(self, statuses: Sequence[str] | None = None) -> list[TaskSnapshot]:
        return [self._task_snapshot_from_record(item) for item in self.store.list_tasks(statuses=statuses)]

    def get_task(self, task_id: str) -> TaskSnapshot:
        record = self.store.get_task(task_id)
        if record is None:
            raise KeyError(f"task not found: {task_id}")
        return self._task_snapshot_from_record(record)

    def list_task_events(self, task_id: str) -> list[FleetEvent]:
        return [self._fleet_event_from_stored(item) for item in self.store.list_task_events(task_id)]

    async def connect_node(self, node_id: str, timeout_sec: float = 5.0) -> ConnectResult:
        await self.start()
        node = self._require_node(node_id)
        self._ensure_node_stream_started(node)

        error = ""
        try:
            await asyncio.wait_for(node.ready_event.wait(), timeout=timeout_sec)
        except asyncio.TimeoutError:
            error = f"connect timeout: {node_id}"
            node.last_error = error
            self._persist_node_state(node)

        snapshot = self._snapshot_from_managed(node)
        if error:
            return ConnectResult(node_id=node_id, ok=False, snapshot=snapshot, error=error)
        if not snapshot.authenticated:
            return ConnectResult(
                node_id=node_id,
                ok=False,
                snapshot=snapshot,
                error=snapshot.last_error or f"node authentication failed: {node_id}",
            )
        return ConnectResult(node_id=node_id, ok=True, snapshot=snapshot)

    async def connect_all(self, timeout_sec: float = 5.0) -> list[ConnectResult]:
        await self.start()
        node_ids = sorted(self._nodes)
        results = await asyncio.gather(
            *(self.connect_node(node_id, timeout_sec=timeout_sec) for node_id in node_ids),
            return_exceptions=True,
        )

        final: list[ConnectResult] = []
        for node_id, result in zip(node_ids, results):
            if isinstance(result, Exception):
                snapshot = self.get_node(node_id)
                final.append(
                    ConnectResult(
                        node_id=node_id,
                        ok=False,
                        snapshot=snapshot,
                        error=snapshot.last_error or f"{type(result).__name__}: {result}",
                    )
                )
                continue
            final.append(result)
        return final

    def _ensure_node_stream_started(self, node: _ManagedNode) -> None:
        if node.stream_task is None or node.stream_task.done():
            node.send_queue = asyncio.Queue()
            node.ready_event = asyncio.Event()
            node.stop_requested = False
            node.auth_failed = False
            node.last_error = ""
            node.stream_task = asyncio.create_task(self._run_node_lifecycle(node))
            return
        if not (node.connected and node.authenticated):
            node.ready_event = asyncio.Event()

    async def disconnect_node(self, node_id: str) -> None:
        node = self._require_node(node_id)
        if node.stream_task is None:
            node.connected = False
            node.authenticated = False
            self._persist_node_state(node)
            return

        node.stop_requested = True
        if not node.stream_task.done():
            await node.send_queue.put(
                control_stream_pb2.ControlRequest(
                    request_id=f"close-{node_id}",
                    trace_id=f"close-{node_id}",
                    close=control_stream_pb2.Close(protocol_version="v1"),
                )
            )
            await node.send_queue.put(None)
            await asyncio.gather(node.stream_task, return_exceptions=True)

        node.stream_task = None
        node.connected = False
        node.authenticated = False
        node.channel = None
        self._persist_node_state(node)
        self._dispatch_wakeup.set()

    def drain_node(self, node_id: str, draining: bool = True) -> NodeSnapshot:
        node = self._require_node(node_id)
        node.draining = draining
        self._persist_node_state(node)
        self._dispatch_wakeup.set()
        return self._snapshot_from_managed(node)

    def disable_node(self, node_id: str, disabled: bool = True) -> NodeSnapshot:
        node = self._require_node(node_id)
        node.disabled = disabled
        self._persist_node_state(node)
        self._dispatch_wakeup.set()
        return self._snapshot_from_managed(node)

    async def submit_task(
        self,
        submit: control_stream_pb2.TaskSubmit,
        *,
        node_id: str | None = None,
        request_id: str | None = None,
        trace_id: str | None = None,
    ) -> tuple[str, str, str]:
        await self.start()
        request_id = request_id or submit.task_id or uuid.uuid4().hex
        trace_id = trace_id or request_id
        requested_node_id = node_id or ""

        try:
            self.store.create_task(
                task_id=submit.task_id,
                request_id=request_id,
                trace_id=trace_id,
                submit_payload=self._task_submit_to_payload(submit),
                requested_node_id=requested_node_id,
                created_at=monotonic_now(),
            )
        except ValueError as exc:
            raise RuntimeError(str(exc)) from exc

        self._task_to_node.pop(submit.task_id, None)
        self._task_terminal_cache.pop(submit.task_id, None)
        await self._publish_event(
            FleetEvent(
                node_id=requested_node_id,
                kind="task_submitted",
                request_id=request_id,
                trace_id=trace_id,
                data={"task_id": submit.task_id, "requested_node_id": requested_node_id},
            )
        )
        self._dispatch_wakeup.set()

        assigned_node_id = await self._wait_for_assignment(submit.task_id, timeout_sec=self.assignment_timeout_sec)
        return assigned_node_id, request_id, trace_id

    async def cancel_task(
        self,
        task_id: str,
        *,
        node_id: str | None = None,
        request_id: str | None = None,
        trace_id: str | None = None,
        protocol_version: str = "v1",
    ) -> tuple[str, str, str]:
        task = self.store.get_task(task_id)
        if task is None:
            raise KeyError(f"unknown task: {task_id}")

        target_node = node_id or task.assigned_node_id or task.requested_node_id or self._task_to_node.get(task_id, "")
        request_id = request_id or task.request_id or task_id
        trace_id = trace_id or task.trace_id or request_id

        if task.status == "pending":
            await self._publish_event(
                FleetEvent(
                    node_id=target_node,
                    kind="task_event",
                    request_id=request_id,
                    trace_id=trace_id,
                    data={"task_id": task_id, "event_type": "canceled", "payload": {}, "error_code": ""},
                )
            )
            return target_node, request_id, trace_id

        if not target_node:
            raise RuntimeError(f"task is not assigned yet: {task_id}")

        await self._send(
            target_node,
            control_stream_pb2.ControlRequest(
                request_id=request_id,
                trace_id=trace_id,
                task_cancel=control_stream_pb2.TaskCancel(protocol_version=protocol_version, task_id=task_id),
            ),
        )
        return target_node, request_id, trace_id

    async def deploy(
        self,
        deploy_request: control_stream_pb2.DeployRequest,
        *,
        node_id: str,
        request_id: str | None = None,
        trace_id: str | None = None,
    ) -> tuple[str, str, str]:
        await self.start()
        node = self._require_node(node_id)
        self._assert_node_accepting_work(node, action="deploy")

        request_id = request_id or uuid.uuid4().hex
        trace_id = trace_id or request_id
        await self._send(
            node_id,
            control_stream_pb2.ControlRequest(
                request_id=request_id,
                trace_id=trace_id,
                deploy_request=deploy_request,
            ),
        )
        return node_id, request_id, trace_id

    async def next_event(self, timeout_sec: float | None = None) -> FleetEvent:
        return await self._default_subscription.next_event(timeout_sec=timeout_sec)

    async def wait_for_task_terminal(self, task_id: str, timeout_sec: float = 10.0) -> FleetEvent:
        cached = self._task_terminal_cache.get(task_id)
        if cached is not None:
            return cached

        stored = self.store.get_terminal_task_event(task_id, self.TERMINAL_TASK_EVENTS)
        if stored is not None:
            event = self._fleet_event_from_stored(stored)
            self._remember_terminal_event(task_id, event)
            return event

        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[FleetEvent] = loop.create_future()
        self._task_waiters.setdefault(task_id, []).append(waiter)
        try:
            return await asyncio.wait_for(waiter, timeout=timeout_sec)
        finally:
            waiters = self._task_waiters.get(task_id, [])
            if waiter in waiters:
                waiters.remove(waiter)
            if not waiters:
                self._task_waiters.pop(task_id, None)

    async def close(self) -> None:
        self._closing = True

        for node in self._nodes.values():
            node.stop_requested = True

        close_requests = []
        for node in self._nodes.values():
            if node.stream_task is None or node.stream_task.done():
                continue
            close_requests.append(
                self._send(
                    node.target.node_id,
                    control_stream_pb2.ControlRequest(
                        request_id=f"close-{node.target.node_id}",
                        trace_id=f"close-{node.target.node_id}",
                        close=control_stream_pb2.Close(protocol_version="v1"),
                    ),
                    enqueue_close=True,
                    allow_stopped=True,
                )
            )
        if close_requests:
            await asyncio.gather(*close_requests, return_exceptions=True)

        tasks = [node.stream_task for node in self._nodes.values() if node.stream_task is not None]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        background = [task for task in (self._leadership_task, self._scheduler_task) if task is not None]
        for task in background:
            task.cancel()
        if background:
            await asyncio.gather(*background, return_exceptions=True)

        self.store.release_lease(self.lease_name, self.controller_id)
        self._is_leader = False

        subscriptions = list(self._subscriptions)
        for subscription in subscriptions:
            await subscription.close()

        if self._owns_shared_storage:
            self.shared_storage.close()
        if self._owns_store:
            self.store.close()

    async def _send(
        self,
        node_id: str,
        request: control_stream_pb2.ControlRequest,
        *,
        enqueue_close: bool = False,
        allow_stopped: bool = False,
    ) -> None:
        node = self._require_node(node_id)
        if node.stream_task is None or node.stream_task.done() or (node.stop_requested and not allow_stopped):
            raise RuntimeError(f"node is not connected: {node_id}")
        await node.send_queue.put(request)
        if enqueue_close:
            await node.send_queue.put(None)

    async def _leadership_loop(self) -> None:
        interval = max(self.lease_ttl_sec / 3.0, 0.1)
        try:
            while not self._closing:
                was_leader = self._is_leader
                is_leader = self.store.acquire_lease(
                    self.lease_name,
                    self.controller_id,
                    self.lease_ttl_sec,
                    monotonic_now(),
                )
                if is_leader and not self._is_leader:
                    if self.auto_connect_on_leadership:
                        self._resume_managed_nodes()
                    self._dispatch_wakeup.set()
                elif was_leader and not is_leader:
                    await self._disconnect_all_nodes()
                self._is_leader = is_leader
                self._lease_ready.set()
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            raise

    async def _scheduler_loop(self) -> None:
        try:
            while not self._closing:
                if not self._is_leader:
                    await self._wait_for_dispatch_wakeup()
                    continue

                recovered_any = await self._recover_stale_dispatches()
                pending = self.store.list_pending_tasks(limit=64)
                dispatched_any = recovered_any
                for task in pending:
                    if await self._dispatch_stored_task(task):
                        dispatched_any = True

                if dispatched_any:
                    continue
                await self._wait_for_dispatch_wakeup()
        except asyncio.CancelledError:
            raise

    async def _wait_for_dispatch_wakeup(self) -> None:
        try:
            await asyncio.wait_for(self._dispatch_wakeup.wait(), timeout=self.dispatch_interval_sec)
        except asyncio.TimeoutError:
            return
        finally:
            self._dispatch_wakeup.clear()

    def _resume_managed_nodes(self) -> None:
        for node in self._nodes.values():
            if node.disabled:
                continue
            self._ensure_node_stream_started(node)

    async def _disconnect_all_nodes(self) -> None:
        disconnects = [
            self.disconnect_node(node_id)
            for node_id, node in self._nodes.items()
            if node.stream_task is not None and not node.stream_task.done()
        ]
        if disconnects:
            await asyncio.gather(*disconnects, return_exceptions=True)

    async def _recover_stale_dispatches(self) -> bool:
        recovered = self.store.requeue_stale_tasks(
            statuses=("dispatching",),
            stale_before=monotonic_now() - self.dispatch_claim_timeout_sec,
            updated_at=monotonic_now(),
        )
        for task in recovered:
            await self._publish_event(
                FleetEvent(
                    node_id=task.assigned_node_id,
                    kind="task_requeued",
                    request_id=task.request_id,
                    trace_id=task.trace_id,
                    data={"task_id": task.task_id, "reason": "dispatch_claim_timeout"},
                )
            )
        return bool(recovered)

    async def _dispatch_stored_task(self, task: StoredTask) -> bool:
        submit = self._task_submit_from_payload(task.submit_payload)
        try:
            if task.requested_node_id:
                node = self._require_node(task.requested_node_id)
                self._assert_node_accepting_work(node, action="dispatch task")
                target_node = task.requested_node_id
            else:
                target_node = self._select_node_for_task(submit)
        except (KeyError, RuntimeError):
            return False

        if not self.store.claim_task(task.task_id, node_id=target_node, updated_at=monotonic_now()):
            return False

        try:
            await self._send(
                target_node,
                control_stream_pb2.ControlRequest(
                    request_id=task.request_id,
                    trace_id=task.trace_id,
                    task_submit=submit,
                ),
            )
        except Exception as exc:
            self.store.reset_task_to_pending(task.task_id, updated_at=monotonic_now())
            await self._publish_event(
                FleetEvent(
                    node_id=target_node,
                    kind="task_dispatch_failed",
                    request_id=task.request_id,
                    trace_id=task.trace_id,
                    data={"task_id": task.task_id, "error": f"{type(exc).__name__}: {exc}"},
                )
            )
            return False

        self._task_to_node[task.task_id] = target_node
        self._resolve_assignment_waiters(task.task_id, target_node)
        await self._publish_event(
            FleetEvent(
                node_id=target_node,
                kind="task_dispatched",
                request_id=task.request_id,
                trace_id=task.trace_id,
                data={"task_id": task.task_id},
            )
        )
        return True

    async def _run_node_lifecycle(self, node: _ManagedNode) -> None:
        while not node.stop_requested:
            reconnect_delay = self._compute_backoff(node.reconnect_attempt)
            try:
                await self._run_single_stream(node)
            except Exception as exc:
                node.last_error = f"{type(exc).__name__}: {exc}"
            finally:
                was_connected = node.connected
                node.connected = False
                node.channel = None
                self._persist_node_state(node)
                if was_connected and not node.stop_requested:
                    await self._publish_event(
                        FleetEvent(
                            node_id=node.target.node_id,
                            kind="node_disconnected",
                            request_id="",
                            trace_id="",
                            data={"error": node.last_error},
                        )
                    )

            if node.stop_requested or node.auth_failed:
                break

            node.reconnect_attempt += 1
            self._persist_node_state(node)
            await self._publish_event(
                FleetEvent(
                    node_id=node.target.node_id,
                    kind="node_reconnecting",
                    request_id="",
                    trace_id="",
                    data={
                        "attempt": node.reconnect_attempt,
                        "backoff_sec": reconnect_delay,
                        "error": node.last_error,
                    },
                )
            )
            await asyncio.sleep(reconnect_delay)

    async def _run_single_stream(self, node: _ManagedNode) -> None:
        node.auth_failed = False
        metadata = self._build_metadata(node)
        channel = self._open_channel(node)
        async with channel:
            node.channel = channel
            await channel.channel_ready()
            stub = control_stream_pb2_grpc.ControlStreamStub(channel)
            call = stub.Stream(self._iter_requests(node), metadata=metadata)
            async for response in call:
                await self._handle_response(node, response)

    async def _iter_requests(self, node: _ManagedNode) -> AsyncIterator[control_stream_pb2.ControlRequest]:
        while True:
            request = await node.send_queue.get()
            if request is None:
                return
            yield request

    async def _handle_response(self, node: _ManagedNode, response: control_stream_pb2.ControlResponse) -> None:
        node.last_seen_at = monotonic_now()
        payload_kind = response.WhichOneof("payload")

        if payload_kind == "node_hello":
            node.connected = True
            node.authenticated = True
            node.last_error = ""
            node.reconnect_attempt = 0
            node.capability = self._message_to_dict(response.node_hello.capability)
            self._persist_node_state(node)
            self._dispatch_wakeup.set()
            if not node.ready_event.is_set():
                node.ready_event.set()
            await self._publish_event(
                FleetEvent(
                    node_id=node.target.node_id,
                    kind="node_hello",
                    request_id=response.request_id,
                    trace_id=response.trace_id,
                    data={
                        "protocol_version": response.node_hello.protocol_version,
                        "capability": dict(node.capability),
                    },
                )
            )
            await node.send_queue.put(
                control_stream_pb2.ControlRequest(
                    request_id=f"sync-{node.target.node_id}-{uuid.uuid4().hex[:8]}",
                    trace_id=f"sync-{node.target.node_id}",
                    task_sync_request=control_stream_pb2.TaskSyncRequest(protocol_version="v1"),
                )
            )
            return

        if payload_kind == "heartbeat":
            node.last_heartbeat = self._message_to_dict(response.heartbeat.metrics)
            node.last_heartbeat_at = monotonic_now()
            self._persist_node_state(node)
            self._dispatch_wakeup.set()
            await self._publish_event(
                FleetEvent(
                    node_id=node.target.node_id,
                    kind="heartbeat",
                    request_id=response.request_id,
                    trace_id=response.trace_id,
                    data={
                        "protocol_version": response.heartbeat.protocol_version,
                        "metrics": dict(node.last_heartbeat),
                    },
                )
            )
            return

        if payload_kind == "auth_failed":
            node.connected = False
            node.authenticated = False
            node.auth_failed = True
            node.last_error = response.auth_failed.reason or response.auth_failed.error_code or "authentication failed"
            self._persist_node_state(node)
            self._dispatch_wakeup.set()
            if not node.ready_event.is_set():
                node.ready_event.set()
            await self._publish_event(
                FleetEvent(
                    node_id=node.target.node_id,
                    kind="auth_failed",
                    request_id=response.request_id,
                    trace_id=response.trace_id,
                    data={
                        "reason": response.auth_failed.reason,
                        "error_code": response.auth_failed.error_code,
                    },
                )
            )
            return

        if payload_kind == "task_event":
            payload = self._message_to_dict(response.task_event.payload)
            task_id = response.task_event.task_id
            event_type = response.task_event.event_type
            if event_type in self.TERMINAL_TASK_EVENTS:
                node.active_tasks.pop(task_id, None)
            else:
                node.active_tasks[task_id] = event_type
                self._task_to_node[task_id] = node.target.node_id
                self._resolve_assignment_waiters(task_id, node.target.node_id)
            self._persist_node_state(node)

            await self._publish_event(
                FleetEvent(
                    node_id=node.target.node_id,
                    kind="task_event",
                    request_id=response.request_id,
                    trace_id=response.trace_id,
                    data={
                        "task_id": task_id,
                        "event_type": event_type,
                        "payload": payload,
                        "error_code": response.task_event.error_code,
                    },
                )
            )
            return

        if payload_kind == "task_snapshot":
            snapshot_ids: set[str] = set()
            for task in response.task_snapshot.tasks:
                snapshot_ids.add(task.task_id)
                node.active_tasks[task.task_id] = task.status
                self._task_to_node[task.task_id] = node.target.node_id
                self._resolve_assignment_waiters(task.task_id, node.target.node_id)
                await self._publish_event(
                    FleetEvent(
                        node_id=node.target.node_id,
                        kind="task_event",
                        request_id=response.request_id,
                        trace_id=response.trace_id,
                        data={
                            "task_id": task.task_id,
                            "event_type": task.status,
                            "payload": {
                                "reconciled": True,
                                "task_type": task.task_type,
                                "require_gpu": task.require_gpu,
                                "prefer_gpu": task.prefer_gpu,
                                "priority": task.priority,
                                "timeout_sec": task.timeout_sec,
                                "resource_request": {
                                    "cpu_cores": task.resource_request.cpu_cores,
                                    "memory_mb": task.resource_request.memory_mb,
                                    "gpu_vram_mb": task.resource_request.gpu_vram_mb,
                                },
                                "assigned_gpu_indices": list(task.assigned_gpu_indices),
                            },
                            "error_code": "",
                        },
                    )
                )

            for task_id in list(node.active_tasks):
                if task_id not in snapshot_ids:
                    node.active_tasks.pop(task_id, None)
            self._persist_node_state(node)
            await self._publish_event(
                FleetEvent(
                    node_id=node.target.node_id,
                    kind="task_snapshot",
                    request_id=response.request_id,
                    trace_id=response.trace_id,
                    data={"task_count": len(snapshot_ids)},
                )
            )
            return

        if payload_kind == "deploy_event":
            await self._publish_event(
                FleetEvent(
                    node_id=node.target.node_id,
                    kind="deploy_event",
                    request_id=response.request_id,
                    trace_id=response.trace_id,
                    data={
                        "ok": response.deploy_event.ok,
                        "message": response.deploy_event.message,
                        "error_code": response.deploy_event.error_code,
                    },
                )
            )

    async def _publish_event(self, event: FleetEvent) -> None:
        persisted = self._persist_event(event)
        if persisted.kind == "task_event":
            task_id = str(persisted.data.get("task_id") or "")
            event_type = str(persisted.data.get("event_type") or "")
            if task_id:
                if persisted.node_id:
                    self._task_to_node[task_id] = persisted.node_id
                    self._resolve_assignment_waiters(task_id, persisted.node_id)
                if event_type in self.TERMINAL_TASK_EVENTS:
                    self._remember_terminal_event(task_id, persisted)
                    waiters = self._task_waiters.pop(task_id, [])
                    for waiter in waiters:
                        if not waiter.done():
                            waiter.set_result(persisted)

        subscriptions = list(self._subscriptions)
        for subscription in subscriptions:
            await subscription.publish(persisted)

    def _persist_event(self, event: FleetEvent) -> FleetEvent:
        created_at = monotonic_now()
        task_id = str(event.data.get("task_id") or "")
        event_type = str(event.data.get("event_type") or "")
        payload = dict(event.data.get("payload") or {})

        if event.kind == "task_event" and task_id:
            payload = self.shared_storage.persist_task_payload(
                task_id=task_id,
                node_id=event.node_id,
                event_type=event_type,
                request_id=event.request_id,
                trace_id=event.trace_id,
                payload=payload,
            )
            event = replace(event, data={**event.data, "payload": payload})
            self.store.apply_task_event(
                StoredEvent(
                    event_id=0,
                    node_id=event.node_id,
                    kind=event.kind,
                    request_id=event.request_id,
                    trace_id=event.trace_id,
                    task_id=task_id,
                    event_type=event_type,
                    data=dict(event.data),
                    created_at=created_at,
                )
            )
            return event

        stored_event_type = event_type or event.kind
        self.store.append_event(
            StoredEvent(
                event_id=0,
                node_id=event.node_id,
                kind=event.kind,
                request_id=event.request_id,
                trace_id=event.trace_id,
                task_id=task_id,
                event_type=stored_event_type,
                data=dict(event.data),
                created_at=created_at,
            )
        )
        return event

    def _remember_terminal_event(self, task_id: str, event: FleetEvent) -> None:
        self._task_terminal_cache[task_id] = event
        self._task_terminal_cache.move_to_end(task_id)
        while len(self._task_terminal_cache) > self.terminal_cache_size:
            self._task_terminal_cache.popitem(last=False)

    def _select_node_for_task(self, submit: control_stream_pb2.TaskSubmit) -> str:
        candidates = [node for node in self._nodes.values() if self._is_node_eligible(node)]
        if not candidates:
            raise RuntimeError("no eligible nodes")

        if submit.require_gpu:
            gpu_nodes = [node for node in candidates if bool(node.capability.get("gpu_available", False))]
            if not gpu_nodes:
                raise RuntimeError("no GPU-capable node available")
            candidates = gpu_nodes
        elif submit.prefer_gpu:
            gpu_nodes = [node for node in candidates if bool(node.capability.get("gpu_available", False))]
            if gpu_nodes:
                candidates = gpu_nodes
        else:
            cpu_first = [node for node in candidates if not bool(node.capability.get("gpu_available", False))]
            if cpu_first:
                candidates = cpu_first

        candidates.sort(key=lambda item: self._selection_key(item, submit))
        return candidates[0].target.node_id

    def _selection_key(self, node: _ManagedNode, submit: control_stream_pb2.TaskSubmit) -> tuple[Any, ...]:
        heartbeat = node.last_heartbeat
        cpu = self._metric_value(heartbeat, "system_cpu_percent", default=1000.0)
        memory = self._metric_value(heartbeat, "system_memory_percent", default=1000.0)
        disk = self._metric_value(heartbeat, "system_disk_percent", default=1000.0)
        queue_wait = self._metric_value(heartbeat, "task_queue_wait_seconds", default=0.0)
        total_tasks = self._metric_value(heartbeat, "task_total_count", default=0.0)
        gpu_count = len(node.capability.get("gpus") or [])
        gpu_bonus = -gpu_count if submit.require_gpu or submit.prefer_gpu else 0
        return (
            len(node.active_tasks),
            cpu,
            memory,
            disk,
            queue_wait,
            total_tasks,
            gpu_bonus,
            node.target.node_id,
        )

    def _assert_node_accepting_work(self, node: _ManagedNode, *, action: str) -> None:
        if node.disabled:
            raise RuntimeError(f"node is disabled and cannot {action}: {node.target.node_id}")
        if node.draining:
            raise RuntimeError(f"node is draining and cannot {action}: {node.target.node_id}")
        if not node.authenticated or not node.connected:
            raise RuntimeError(f"node is not ready and cannot {action}: {node.target.node_id}")
        if self._is_node_stale(node):
            raise RuntimeError(f"node heartbeat is stale and cannot {action}: {node.target.node_id}")

    def _is_node_eligible(self, node: _ManagedNode) -> bool:
        return (
            node.connected
            and node.authenticated
            and not node.disabled
            and not node.draining
            and not self._is_node_stale(node)
            and bool(node.capability)
        )

    def _is_node_stale(self, node: _ManagedNode) -> bool:
        reference_time = node.last_heartbeat_at or node.last_seen_at
        if reference_time <= 0:
            return True
        return (monotonic_now() - reference_time) > self.heartbeat_timeout_sec

    def _snapshot_from_managed(self, node: _ManagedNode) -> NodeSnapshot:
        stale = self._is_node_stale(node)
        return NodeSnapshot(
            node_id=node.target.node_id,
            address=node.target.address,
            connected=node.connected,
            authenticated=node.authenticated,
            capability=dict(node.capability),
            last_heartbeat=dict(node.last_heartbeat),
            active_tasks=sorted(node.active_tasks),
            last_error=node.last_error,
            last_seen_at=node.last_seen_at,
            last_heartbeat_at=node.last_heartbeat_at,
            reconnect_attempt=node.reconnect_attempt,
            disabled=node.disabled,
            draining=node.draining,
            stale=stale,
            eligible=self._is_node_eligible(node),
            controller_id=self.controller_id,
        )

    def _snapshot_from_record(self, record: StoredNode) -> NodeSnapshot:
        local = self._nodes.get(record.node_id)
        stale = True
        eligible = False
        if local is not None:
            stale = self._is_node_stale(local)
            eligible = self._is_node_eligible(local)
        elif record.last_heartbeat_at > 0:
            stale = (monotonic_now() - record.last_heartbeat_at) > self.heartbeat_timeout_sec

        return NodeSnapshot(
            node_id=record.node_id,
            address=record.address,
            connected=record.connected,
            authenticated=record.authenticated,
            capability=dict(record.capability),
            last_heartbeat=dict(record.last_heartbeat),
            active_tasks=list(record.active_tasks),
            last_error=record.last_error,
            last_seen_at=record.last_seen_at,
            last_heartbeat_at=record.last_heartbeat_at,
            reconnect_attempt=record.reconnect_attempt,
            disabled=record.disabled,
            draining=record.draining,
            stale=stale,
            eligible=eligible,
            controller_id=record.controller_id,
        )

    def _task_snapshot_from_record(self, record: StoredTask) -> TaskSnapshot:
        return TaskSnapshot(
            task_id=record.task_id,
            status=record.status,
            request_id=record.request_id,
            trace_id=record.trace_id,
            requested_node_id=record.requested_node_id,
            assigned_node_id=record.assigned_node_id,
            submit_payload=dict(record.submit_payload),
            last_event_type=record.last_event_type,
            terminal_payload=dict(record.terminal_payload),
            dispatch_attempts=record.dispatch_attempts,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )

    def _build_metadata(self, node: _ManagedNode) -> list[tuple[str, str]]:
        metadata = [
            ("x-node-id", node.target.node_id),
            ("x-api-key", node.target.api_key),
        ]
        metadata.extend(sorted(node.target.metadata.items()))
        return metadata

    def _open_channel(self, node: _ManagedNode):
        if node.target.tls is None:
            return grpc.aio.insecure_channel(node.target.address)
        return grpc.aio.secure_channel(
            node.target.address,
            node.target.tls.build_channel_credentials(),
            options=node.target.tls.build_channel_options(),
        )

    def _compute_backoff(self, reconnect_attempt: int) -> float:
        return min(self.reconnect_backoff_max_sec, self.reconnect_backoff_initial_sec * (2 ** reconnect_attempt))

    def _unsubscribe(self, subscription: EventSubscription) -> None:
        if subscription in self._subscriptions:
            self._subscriptions.remove(subscription)

    def _require_node(self, node_id: str) -> _ManagedNode:
        node = self._nodes.get(node_id)
        if node is None:
            raise KeyError(f"node does not exist: {node_id}")
        return node

    def _persist_node_state(self, node: _ManagedNode) -> None:
        self.store.upsert_node(
            StoredNode(
                node_id=node.target.node_id,
                address=node.target.address,
                api_key=node.target.api_key,
                metadata=dict(node.target.metadata),
                tls_config=self._serialize_tls(node.target.tls),
                connected=node.connected,
                authenticated=node.authenticated,
                capability=dict(node.capability),
                last_heartbeat=dict(node.last_heartbeat),
                active_tasks=sorted(node.active_tasks),
                last_error=node.last_error,
                last_seen_at=node.last_seen_at,
                last_heartbeat_at=node.last_heartbeat_at,
                reconnect_attempt=node.reconnect_attempt,
                disabled=node.disabled,
                draining=node.draining,
                controller_id=self.controller_id,
                updated_at=monotonic_now(),
            )
        )

    def _restore_nodes_from_store(self) -> None:
        for record in self.store.list_nodes():
            if record.node_id in self._nodes:
                continue
            node = _ManagedNode(
                target=NodeTarget(
                    node_id=record.node_id,
                    address=record.address,
                    api_key=record.api_key,
                    metadata=dict(record.metadata),
                    tls=self._deserialize_tls(record.tls_config),
                )
            )
            node.capability = dict(record.capability)
            node.last_heartbeat = dict(record.last_heartbeat)
            node.last_seen_at = record.last_seen_at
            node.last_heartbeat_at = record.last_heartbeat_at
            node.last_error = record.last_error
            node.reconnect_attempt = record.reconnect_attempt
            node.disabled = record.disabled
            node.draining = record.draining
            self._nodes[record.node_id] = node
            self._persist_node_state(node)

    async def _wait_for_assignment(self, task_id: str, *, timeout_sec: float) -> str:
        existing = self._task_to_node.get(task_id)
        if existing:
            return existing

        task = self.store.get_task(task_id)
        if task is not None and task.assigned_node_id:
            self._task_to_node[task_id] = task.assigned_node_id
            return task.assigned_node_id

        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[str] = loop.create_future()
        self._assignment_waiters.setdefault(task_id, []).append(waiter)
        try:
            return await asyncio.wait_for(waiter, timeout=timeout_sec)
        except asyncio.TimeoutError:
            current = self.store.get_task(task_id)
            if current is None:
                raise
            return current.assigned_node_id
        finally:
            waiters = self._assignment_waiters.get(task_id, [])
            if waiter in waiters:
                waiters.remove(waiter)
            if not waiters:
                self._assignment_waiters.pop(task_id, None)

    def _resolve_assignment_waiters(self, task_id: str, node_id: str) -> None:
        if not node_id:
            return
        self._task_to_node[task_id] = node_id
        waiters = self._assignment_waiters.pop(task_id, [])
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(node_id)

    def _fleet_event_from_stored(self, event: StoredEvent) -> FleetEvent:
        data = dict(event.data)
        if event.kind == "task_event":
            data.setdefault("task_id", event.task_id)
            data.setdefault("event_type", event.event_type)
        return FleetEvent(
            node_id=event.node_id,
            kind=event.kind,
            request_id=event.request_id,
            trace_id=event.trace_id,
            data=data,
        )

    @staticmethod
    def _message_to_dict(message) -> dict[str, Any]:
        return MessageToDict(
            message,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
        )

    @staticmethod
    def _metric_value(metrics: Mapping[str, Any], key: str, *, default: float) -> float:
        value = metrics.get(key)
        if value in (None, ""):
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _task_submit_to_payload(submit: control_stream_pb2.TaskSubmit) -> dict[str, Any]:
        return {
            "protocol_version": submit.protocol_version,
            "task_id": submit.task_id,
            "command": list(submit.command),
            "task_type": submit.task_type,
            "require_gpu": submit.require_gpu,
            "prefer_gpu": submit.prefer_gpu,
            "env": dict(submit.env),
            "priority": submit.priority,
            "timeout_sec": submit.timeout_sec,
            "workdir": submit.workdir,
            "resource_request": {
                "cpu_cores": submit.resource_request.cpu_cores,
                "memory_mb": submit.resource_request.memory_mb,
                "gpu_vram_mb": submit.resource_request.gpu_vram_mb,
            },
        }

    @staticmethod
    def _task_submit_from_payload(payload: Mapping[str, Any]) -> control_stream_pb2.TaskSubmit:
        submit = control_stream_pb2.TaskSubmit(
            protocol_version=str(payload.get("protocol_version") or "v1"),
            task_id=str(payload.get("task_id") or ""),
            command=list(payload.get("command") or []),
            task_type=str(payload.get("task_type") or ""),
            require_gpu=bool(payload.get("require_gpu", False)),
            prefer_gpu=bool(payload.get("prefer_gpu", False)),
            env={str(key): str(value) for key, value in dict(payload.get("env") or {}).items()},
            priority=int(payload.get("priority", 0)),
            timeout_sec=float(payload.get("timeout_sec", 0.0)),
            workdir=str(payload.get("workdir") or ""),
        )
        resource_request = dict(payload.get("resource_request") or {})
        submit.resource_request.cpu_cores = int(resource_request.get("cpu_cores", 1))
        submit.resource_request.memory_mb = int(resource_request.get("memory_mb", 256))
        submit.resource_request.gpu_vram_mb = int(resource_request.get("gpu_vram_mb", 0))
        return submit

    @staticmethod
    def _serialize_tls(tls: ClientTLSConfig | None) -> dict[str, Any]:
        if tls is None:
            return {}
        return encode_tls_config(
            root_certificates=tls.root_certificates,
            private_key=tls.private_key,
            certificate_chain=tls.certificate_chain,
            server_name_override=tls.server_name_override,
        )

    @staticmethod
    def _deserialize_tls(payload: Mapping[str, Any]) -> ClientTLSConfig | None:
        if not payload:
            return None
        data = decode_tls_config(payload)
        if not any((data["root_certificates"], data["private_key"], data["certificate_chain"], data["server_name_override"])):
            return None
        return ClientTLSConfig(
            root_certificates=data["root_certificates"],
            private_key=data["private_key"],
            certificate_chain=data["certificate_chain"],
            server_name_override=data["server_name_override"],
        )


__all__ = [
    "ConnectResult",
    "EventSubscription",
    "FleetEvent",
    "MultiNodeManager",
    "NodeSnapshot",
    "NodeTarget",
    "TaskSnapshot",
]
