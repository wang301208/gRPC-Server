from __future__ import annotations

import base64
import json
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Protocol, Sequence
from urllib.parse import urlparse


def _json_dumps(data: Mapping[str, Any] | Sequence[Any] | None) -> str:
    return json.dumps(data or {}, sort_keys=True)


def _json_loads(payload: str) -> Any:
    if not payload:
        return {}
    return json.loads(payload)


def _encode_bytes(data: bytes | None) -> str:
    if not data:
        return ""
    return base64.b64encode(data).decode("ascii")


def _decode_bytes(data: str) -> bytes | None:
    if not data:
        return None
    return base64.b64decode(data.encode("ascii"))


@dataclass(slots=True)
class StoredNode:
    node_id: str
    address: str
    api_key: str
    metadata: dict[str, str]
    tls_config: dict[str, Any]
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
    controller_id: str
    updated_at: float


@dataclass(slots=True)
class StoredTask:
    task_id: str
    status: str
    request_id: str
    trace_id: str
    submit_payload: dict[str, Any]
    requested_node_id: str
    assigned_node_id: str
    created_at: float
    updated_at: float
    last_event_type: str
    terminal_payload: dict[str, Any]
    dispatch_attempts: int


@dataclass(slots=True)
class StoredEvent:
    event_id: int
    node_id: str
    kind: str
    request_id: str
    trace_id: str
    task_id: str
    event_type: str
    data: dict[str, Any]
    created_at: float


@dataclass(slots=True)
class LeaseRecord:
    lease_name: str
    owner_id: str
    expires_at: float
    updated_at: float


class FleetRepository(Protocol):
    def close(self) -> None: ...

    def upsert_node(self, node: StoredNode) -> None: ...

    def delete_node(self, node_id: str) -> None: ...

    def get_node(self, node_id: str) -> StoredNode | None: ...

    def list_nodes(self) -> list[StoredNode]: ...

    def create_task(
        self,
        *,
        task_id: str,
        request_id: str,
        trace_id: str,
        submit_payload: dict[str, Any],
        requested_node_id: str,
        created_at: float,
    ) -> StoredTask: ...

    def get_task(self, task_id: str) -> StoredTask | None: ...

    def list_tasks(self, statuses: Sequence[str] | None = None) -> list[StoredTask]: ...

    def list_pending_tasks(self, limit: int = 64) -> list[StoredTask]: ...

    def requeue_stale_tasks(
        self,
        *,
        statuses: Sequence[str],
        stale_before: float,
        updated_at: float,
    ) -> list[StoredTask]: ...

    def claim_task(self, task_id: str, *, node_id: str, updated_at: float) -> bool: ...

    def reset_task_to_pending(self, task_id: str, *, updated_at: float) -> None: ...

    def apply_task_event(self, event: StoredEvent) -> StoredTask | None: ...

    def append_event(self, event: StoredEvent) -> StoredEvent: ...

    def list_task_events(self, task_id: str) -> list[StoredEvent]: ...

    def get_terminal_task_event(self, task_id: str, terminal_event_types: Iterable[str]) -> StoredEvent | None: ...

    def acquire_lease(self, lease_name: str, owner_id: str, ttl_sec: float, now: float) -> bool: ...

    def release_lease(self, lease_name: str, owner_id: str) -> None: ...

    def get_lease(self, lease_name: str) -> LeaseRecord | None: ...


class SQLiteFleetRepository:
    TERMINAL_STATUSES = {"completed", "failed", "canceled", "rejected", "cancel_failed"}

    def __init__(self, path: str = ":memory:", *, uri: bool = False) -> None:
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False, uri=uri)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._initialize()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def upsert_node(self, node: StoredNode) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO nodes (
                    node_id, address, api_key, metadata_json, tls_config_json, connected, authenticated,
                    capability_json, last_heartbeat_json, active_tasks_json, last_error, last_seen_at,
                    last_heartbeat_at, reconnect_attempt, disabled, draining, controller_id, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(node_id) DO UPDATE SET
                    address = excluded.address,
                    api_key = excluded.api_key,
                    metadata_json = excluded.metadata_json,
                    tls_config_json = excluded.tls_config_json,
                    connected = excluded.connected,
                    authenticated = excluded.authenticated,
                    capability_json = excluded.capability_json,
                    last_heartbeat_json = excluded.last_heartbeat_json,
                    active_tasks_json = excluded.active_tasks_json,
                    last_error = excluded.last_error,
                    last_seen_at = excluded.last_seen_at,
                    last_heartbeat_at = excluded.last_heartbeat_at,
                    reconnect_attempt = excluded.reconnect_attempt,
                    disabled = excluded.disabled,
                    draining = excluded.draining,
                    controller_id = excluded.controller_id,
                    updated_at = excluded.updated_at
                """,
                (
                    node.node_id,
                    node.address,
                    node.api_key,
                    _json_dumps(node.metadata),
                    _json_dumps(node.tls_config),
                    int(node.connected),
                    int(node.authenticated),
                    _json_dumps(node.capability),
                    _json_dumps(node.last_heartbeat),
                    _json_dumps(node.active_tasks),
                    node.last_error,
                    node.last_seen_at,
                    node.last_heartbeat_at,
                    node.reconnect_attempt,
                    int(node.disabled),
                    int(node.draining),
                    node.controller_id,
                    node.updated_at,
                ),
            )
            self._conn.commit()

    def delete_node(self, node_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM nodes WHERE node_id = ?", (node_id,))
            self._conn.commit()

    def get_node(self, node_id: str) -> StoredNode | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM nodes WHERE node_id = ?", (node_id,)).fetchone()
        return self._row_to_node(row) if row is not None else None

    def list_nodes(self) -> list[StoredNode]:
        with self._lock:
            rows = self._conn.execute("SELECT * FROM nodes ORDER BY node_id").fetchall()
        return [self._row_to_node(row) for row in rows]

    def create_task(
        self,
        *,
        task_id: str,
        request_id: str,
        trace_id: str,
        submit_payload: dict[str, Any],
        requested_node_id: str,
        created_at: float,
    ) -> StoredTask:
        with self._lock:
            existing = self._conn.execute("SELECT status FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
            if existing is not None:
                raise ValueError(f"task_id already exists: {task_id}")

            self._conn.execute(
                """
                INSERT INTO tasks (
                    task_id, status, request_id, trace_id, submit_payload_json, requested_node_id, assigned_node_id,
                    created_at, updated_at, last_event_type, terminal_payload_json, dispatch_attempts, priority
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    "pending",
                    request_id,
                    trace_id,
                    _json_dumps(submit_payload),
                    requested_node_id,
                    "",
                    created_at,
                    created_at,
                    "",
                    _json_dumps({}),
                    0,
                    int(submit_payload.get("priority", 0)),
                ),
            )
            self._conn.commit()
            row = self._conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
        assert row is not None
        return self._row_to_task(row)

    def get_task(self, task_id: str) -> StoredTask | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
        return self._row_to_task(row) if row is not None else None

    def list_tasks(self, statuses: Sequence[str] | None = None) -> list[StoredTask]:
        with self._lock:
            if statuses:
                placeholders = ", ".join("?" for _ in statuses)
                rows = self._conn.execute(
                    f"SELECT * FROM tasks WHERE status IN ({placeholders}) ORDER BY created_at, task_id",
                    tuple(statuses),
                ).fetchall()
            else:
                rows = self._conn.execute("SELECT * FROM tasks ORDER BY created_at, task_id").fetchall()
        return [self._row_to_task(row) for row in rows]

    def list_pending_tasks(self, limit: int = 64) -> list[StoredTask]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM tasks
                WHERE status = 'pending'
                ORDER BY priority DESC, created_at ASC, task_id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_task(row) for row in rows]

    def requeue_stale_tasks(
        self,
        *,
        statuses: Sequence[str],
        stale_before: float,
        updated_at: float,
    ) -> list[StoredTask]:
        if not statuses:
            return []

        placeholders = ", ".join("?" for _ in statuses)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT * FROM tasks
                WHERE status IN ({placeholders}) AND updated_at <= ?
                ORDER BY updated_at ASC, task_id ASC
                """,
                (*statuses, stale_before),
            ).fetchall()
            if not rows:
                return []

            task_ids = [str(row["task_id"]) for row in rows]
            id_placeholders = ", ".join("?" for _ in task_ids)
            self._conn.execute(
                f"""
                UPDATE tasks
                SET status = 'pending',
                    assigned_node_id = '',
                    updated_at = ?,
                    last_event_type = 'requeued'
                WHERE task_id IN ({id_placeholders})
                """,
                (updated_at, *task_ids),
            )
            self._conn.commit()
        return [self._row_to_task(row) for row in rows]

    def claim_task(self, task_id: str, *, node_id: str, updated_at: float) -> bool:
        with self._lock:
            cursor = self._conn.execute(
                """
                UPDATE tasks
                SET status = 'dispatching',
                    assigned_node_id = ?,
                    updated_at = ?,
                    dispatch_attempts = dispatch_attempts + 1
                WHERE task_id = ? AND status = 'pending'
                """,
                (node_id, updated_at, task_id),
            )
            self._conn.commit()
        return cursor.rowcount > 0

    def reset_task_to_pending(self, task_id: str, *, updated_at: float) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE tasks
                SET status = 'pending',
                    assigned_node_id = '',
                    updated_at = ?
                WHERE task_id = ?
                """,
                (updated_at, task_id),
            )
            self._conn.commit()

    def apply_task_event(self, event: StoredEvent) -> StoredTask | None:
        task_id = event.task_id
        if not task_id:
            return None

        with self._lock:
            self._insert_event_locked(event)
            row = self._conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
            if row is None:
                self._conn.commit()
                return None

            current = self._row_to_task(row)
            status = current.status
            if event.event_type in self.TERMINAL_STATUSES:
                status = event.event_type
            elif event.event_type == "running":
                status = "running"
            elif event.event_type == "queued":
                status = "queued"

            assigned_node_id = current.assigned_node_id or event.node_id
            terminal_payload = current.terminal_payload
            if event.event_type in self.TERMINAL_STATUSES:
                terminal_payload = dict(event.data)

            self._conn.execute(
                """
                UPDATE tasks
                SET status = ?,
                    assigned_node_id = ?,
                    updated_at = ?,
                    last_event_type = ?,
                    terminal_payload_json = ?
                WHERE task_id = ?
                """,
                (
                    status,
                    assigned_node_id,
                    event.created_at,
                    event.event_type,
                    _json_dumps(terminal_payload),
                    task_id,
                ),
            )
            self._conn.commit()
            next_row = self._conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
        assert next_row is not None
        return self._row_to_task(next_row)

    def append_event(self, event: StoredEvent) -> StoredEvent:
        with self._lock:
            event_id = self._insert_event_locked(event)
            self._conn.commit()
        return StoredEvent(
            event_id=event_id,
            node_id=event.node_id,
            kind=event.kind,
            request_id=event.request_id,
            trace_id=event.trace_id,
            task_id=event.task_id,
            event_type=event.event_type,
            data=dict(event.data),
            created_at=event.created_at,
        )

    def list_task_events(self, task_id: str) -> list[StoredEvent]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE task_id = ? ORDER BY event_id",
                (task_id,),
            ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def get_terminal_task_event(self, task_id: str, terminal_event_types: Iterable[str]) -> StoredEvent | None:
        values = tuple(terminal_event_types)
        if not values:
            return None
        placeholders = ", ".join("?" for _ in values)
        with self._lock:
            row = self._conn.execute(
                f"""
                SELECT * FROM events
                WHERE task_id = ? AND event_type IN ({placeholders})
                ORDER BY event_id DESC
                LIMIT 1
                """,
                (task_id, *values),
            ).fetchone()
        return self._row_to_event(row) if row is not None else None

    def acquire_lease(self, lease_name: str, owner_id: str, ttl_sec: float, now: float) -> bool:
        expires_at = now + ttl_sec
        with self._lock:
            row = self._conn.execute(
                "SELECT owner_id, expires_at FROM leases WHERE lease_name = ?",
                (lease_name,),
            ).fetchone()
            if row is not None and row["owner_id"] != owner_id and row["expires_at"] > now:
                return False

            self._conn.execute(
                """
                INSERT INTO leases (lease_name, owner_id, expires_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(lease_name) DO UPDATE SET
                    owner_id = excluded.owner_id,
                    expires_at = excluded.expires_at,
                    updated_at = excluded.updated_at
                """,
                (lease_name, owner_id, expires_at, now),
            )
            self._conn.commit()
        return True

    def release_lease(self, lease_name: str, owner_id: str) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM leases WHERE lease_name = ? AND owner_id = ?",
                (lease_name, owner_id),
            )
            self._conn.commit()

    def get_lease(self, lease_name: str) -> LeaseRecord | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM leases WHERE lease_name = ?", (lease_name,)).fetchone()
        if row is None:
            return None
        return LeaseRecord(
            lease_name=row["lease_name"],
            owner_id=row["owner_id"],
            expires_at=float(row["expires_at"]),
            updated_at=float(row["updated_at"]),
        )

    def _initialize(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS nodes (
                    node_id TEXT PRIMARY KEY,
                    address TEXT NOT NULL,
                    api_key TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    tls_config_json TEXT NOT NULL,
                    connected INTEGER NOT NULL,
                    authenticated INTEGER NOT NULL,
                    capability_json TEXT NOT NULL,
                    last_heartbeat_json TEXT NOT NULL,
                    active_tasks_json TEXT NOT NULL,
                    last_error TEXT NOT NULL,
                    last_seen_at REAL NOT NULL,
                    last_heartbeat_at REAL NOT NULL,
                    reconnect_attempt INTEGER NOT NULL,
                    disabled INTEGER NOT NULL,
                    draining INTEGER NOT NULL,
                    controller_id TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    trace_id TEXT NOT NULL,
                    submit_payload_json TEXT NOT NULL,
                    requested_node_id TEXT NOT NULL,
                    assigned_node_id TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    last_event_type TEXT NOT NULL,
                    terminal_payload_json TEXT NOT NULL,
                    dispatch_attempts INTEGER NOT NULL,
                    priority INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_tasks_pending ON tasks(status, priority DESC, created_at ASC);
                CREATE TABLE IF NOT EXISTS events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    request_id TEXT NOT NULL,
                    trace_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_events_task_id ON events(task_id, event_id);
                CREATE TABLE IF NOT EXISTS leases (
                    lease_name TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                """
            )
            self._conn.commit()

    def _insert_event_locked(self, event: StoredEvent) -> int:
        cursor = self._conn.execute(
            """
            INSERT INTO events (
                node_id, kind, request_id, trace_id, task_id, event_type, data_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.node_id,
                event.kind,
                event.request_id,
                event.trace_id,
                event.task_id,
                event.event_type,
                _json_dumps(event.data),
                event.created_at,
            ),
        )
        return int(cursor.lastrowid)

    def _row_to_node(self, row: sqlite3.Row) -> StoredNode:
        return StoredNode(
            node_id=str(row["node_id"]),
            address=str(row["address"]),
            api_key=str(row["api_key"]),
            metadata=dict(_json_loads(str(row["metadata_json"]))),
            tls_config=dict(_json_loads(str(row["tls_config_json"]))),
            connected=bool(row["connected"]),
            authenticated=bool(row["authenticated"]),
            capability=dict(_json_loads(str(row["capability_json"]))),
            last_heartbeat=dict(_json_loads(str(row["last_heartbeat_json"]))),
            active_tasks=list(_json_loads(str(row["active_tasks_json"]))),
            last_error=str(row["last_error"]),
            last_seen_at=float(row["last_seen_at"]),
            last_heartbeat_at=float(row["last_heartbeat_at"]),
            reconnect_attempt=int(row["reconnect_attempt"]),
            disabled=bool(row["disabled"]),
            draining=bool(row["draining"]),
            controller_id=str(row["controller_id"]),
            updated_at=float(row["updated_at"]),
        )

    def _row_to_task(self, row: sqlite3.Row) -> StoredTask:
        return StoredTask(
            task_id=str(row["task_id"]),
            status=str(row["status"]),
            request_id=str(row["request_id"]),
            trace_id=str(row["trace_id"]),
            submit_payload=dict(_json_loads(str(row["submit_payload_json"]))),
            requested_node_id=str(row["requested_node_id"]),
            assigned_node_id=str(row["assigned_node_id"]),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
            last_event_type=str(row["last_event_type"]),
            terminal_payload=dict(_json_loads(str(row["terminal_payload_json"]))),
            dispatch_attempts=int(row["dispatch_attempts"]),
        )

    def _row_to_event(self, row: sqlite3.Row) -> StoredEvent:
        return StoredEvent(
            event_id=int(row["event_id"]),
            node_id=str(row["node_id"]),
            kind=str(row["kind"]),
            request_id=str(row["request_id"]),
            trace_id=str(row["trace_id"]),
            task_id=str(row["task_id"]),
            event_type=str(row["event_type"]),
            data=dict(_json_loads(str(row["data_json"]))),
            created_at=float(row["created_at"]),
        )


def encode_tls_config(
    *,
    root_certificates: bytes | None = None,
    private_key: bytes | None = None,
    certificate_chain: bytes | None = None,
    server_name_override: str | None = None,
) -> dict[str, Any]:
    return {
        "root_certificates": _encode_bytes(root_certificates),
        "private_key": _encode_bytes(private_key),
        "certificate_chain": _encode_bytes(certificate_chain),
        "server_name_override": server_name_override or "",
    }


def decode_tls_config(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "root_certificates": _decode_bytes(str(payload.get("root_certificates") or "")),
        "private_key": _decode_bytes(str(payload.get("private_key") or "")),
        "certificate_chain": _decode_bytes(str(payload.get("certificate_chain") or "")),
        "server_name_override": str(payload.get("server_name_override") or "") or None,
    }


def monotonic_now() -> float:
    return time.time()


def build_fleet_repository(
    target: str | None = None,
    *,
    postgres_schema: str = "public",
    sqlite_uri: bool = False,
) -> FleetRepository:
    if not target:
        return SQLiteFleetRepository(uri=sqlite_uri)

    lowered = target.lower()
    if lowered.startswith("postgresql://") or lowered.startswith("postgres://"):
        from node_agent.postgres_repository import PostgresFleetRepository

        return PostgresFleetRepository(target, schema=postgres_schema)

    if lowered.startswith("sqlite://"):
        parsed = urlparse(target)
        path = parsed.path or ""
        if path in ("", "/"):
            sqlite_target = ":memory:"
        elif len(path) >= 3 and path[0] == "/" and path[2] == ":":
            sqlite_target = path[1:]
        else:
            sqlite_target = path
        return SQLiteFleetRepository(sqlite_target, uri=sqlite_uri)

    return SQLiteFleetRepository(target, uri=sqlite_uri)


__all__ = [
    "FleetRepository",
    "LeaseRecord",
    "SQLiteFleetRepository",
    "StoredEvent",
    "StoredNode",
    "StoredTask",
    "build_fleet_repository",
    "decode_tls_config",
    "encode_tls_config",
    "monotonic_now",
]
