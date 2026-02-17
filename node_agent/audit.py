from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict


class AuditLogger:
    """审计日志落盘。"""

    def __init__(self, path: str = "audit.log"):
        self.path = Path(path)

    def write(self, action: str, payload: Dict[str, Any]) -> None:
        """写入一行 JSON 日志。"""
        record = {"ts": int(time.time()), "action": action, "payload": payload}
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
