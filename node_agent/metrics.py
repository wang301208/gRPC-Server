from __future__ import annotations

import threading
import time
from dataclasses import dataclass, asdict
from typing import Callable, Optional

import os


@dataclass(slots=True)
class MetricSnapshot:
    """指标快照。"""

    cpu_percent: float
    memory_percent: float
    disk_percent: float

    def to_dict(self) -> dict:
        return asdict(self)


class MetricsCollector:
    """后台指标采集器。"""

    def __init__(self, interval_sec: float = 2.0):
        self.interval_sec = interval_sec
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @staticmethod
    def collect_once() -> MetricSnapshot:
        """采集一次系统指标。"""
        load1, _, _ = os.getloadavg()
        cpu_count = os.cpu_count() or 1
        cpu_percent = min(100.0, (load1 / cpu_count) * 100)

        mem_total = 1
        mem_available = 0
        if os.path.exists("/proc/meminfo"):
            info = {}
            with open("/proc/meminfo", "r", encoding="utf-8") as f:
                for line in f:
                    key, value = line.split(":", 1)
                    info[key] = int(value.strip().split()[0])
            mem_total = info.get("MemTotal", 1)
            mem_available = info.get("MemAvailable", 0)
        memory_percent = (1 - mem_available / mem_total) * 100

        st = os.statvfs("/")
        total = st.f_blocks * st.f_frsize
        free = st.f_bavail * st.f_frsize
        disk_percent = (1 - free / total) * 100 if total else 0
        return MetricSnapshot(cpu_percent=cpu_percent, memory_percent=memory_percent, disk_percent=disk_percent)

    def start(self, callback: Callable[[MetricSnapshot], None]) -> None:
        """启动后台线程并回调输出。"""

        def _run() -> None:
            while not self._stop.is_set():
                callback(self.collect_once())
                time.sleep(self.interval_sec)

        if self._thread is None or not self._thread.is_alive():
            self._stop.clear()
            self._thread = threading.Thread(target=_run, daemon=True)
            self._thread.start()

    def stop(self) -> None:
        """停止采集。"""
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._thread = None
