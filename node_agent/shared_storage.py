from __future__ import annotations

import json
import shutil
import time
from pathlib import Path
from typing import Any, Mapping, Protocol


class SharedTaskStorage(Protocol):
    def persist_task_payload(
        self,
        *,
        task_id: str,
        node_id: str,
        event_type: str,
        request_id: str,
        trace_id: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]: ...

    def close(self) -> None: ...


class NullSharedTaskStorage:
    def persist_task_payload(
        self,
        *,
        task_id: str,
        node_id: str,
        event_type: str,
        request_id: str,
        trace_id: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        return dict(payload)

    def close(self) -> None:
        return


class FileSystemSharedTaskStorage:
    def __init__(self, root_dir: str | Path) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def persist_task_payload(
        self,
        *,
        task_id: str,
        node_id: str,
        event_type: str,
        request_id: str,
        trace_id: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        task_dir = self.root_dir / task_id
        task_dir.mkdir(parents=True, exist_ok=True)
        event_path = task_dir / "events.jsonl"
        stored_payload = dict(payload)

        if event_type == "log":
            logs_dir = task_dir / "logs"
            logs_dir.mkdir(parents=True, exist_ok=True)
            log_path = logs_dir / "stdout.log"
            line = str(payload.get("line") or "")
            with log_path.open("a", encoding="utf-8") as handle:
                handle.write(line)
                handle.write("\n")
            stored_payload.setdefault("shared_log_path", str(log_path))
        elif event_type == "artifact":
            stored_payload.update(self._copy_artifact(task_dir=task_dir, payload=payload))

        event_record = {
            "node_id": node_id,
            "event_type": event_type,
            "request_id": request_id,
            "trace_id": trace_id,
            "payload": stored_payload,
            "created_at": time.time(),
        }
        with event_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event_record, sort_keys=True))
            handle.write("\n")

        stored_payload.setdefault("shared_event_path", str(event_path))
        return stored_payload

    def close(self) -> None:
        return

    def _copy_artifact(self, *, task_dir: Path, payload: Mapping[str, Any]) -> dict[str, Any]:
        source_path = Path(str(payload.get("path") or "")).expanduser()
        if not source_path.is_file():
            return {"shared_artifact_path": "", "artifact_missing": True}

        artifact_dir = task_dir / "artifacts"
        artifact_dir.mkdir(parents=True, exist_ok=True)
        artifact_name = str(payload.get("name") or source_path.name or "artifact")
        destination = artifact_dir / artifact_name
        shutil.copy2(source_path, destination)
        return {"shared_artifact_path": str(destination), "artifact_missing": False}


__all__ = [
    "FileSystemSharedTaskStorage",
    "NullSharedTaskStorage",
    "SharedTaskStorage",
]
