from __future__ import annotations

import threading
import time
from dataclasses import dataclass, asdict
import importlib
import importlib.util
from typing import Callable, Optional

import os


_HAS_PSUTIL = importlib.util.find_spec("psutil") is not None
_PSUTIL = importlib.import_module("psutil") if _HAS_PSUTIL else None


@dataclass(slots=True)
class MetricSnapshot:
    """指标快照。

    字段语义：
    - cpu_percent: CPU 使用率（0~100），不可用时为 None。
    - memory_percent: 内存使用率（0~100），不可用时为 None。
    - disk_percent: 根分区磁盘使用率（0~100），不可用时为 None。
    """

    system_cpu_percent: Optional[float]
    system_memory_percent: Optional[float]
    system_disk_percent: Optional[float]

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
        cpu_percent: Optional[float] = None
        memory_percent: Optional[float] = None
        disk_percent: Optional[float] = None

        if _PSUTIL is not None:
            # 优先使用可选依赖 psutil 获取指标。
            cpu_percent = float(_PSUTIL.cpu_percent(interval=None))
            memory_percent = float(_PSUTIL.virtual_memory().percent)
            disk_percent = float(_PSUTIL.disk_usage("/").percent)
            return MetricSnapshot(
                system_cpu_percent=cpu_percent,
                system_memory_percent=memory_percent,
                system_disk_percent=disk_percent,
            )

        # 回退到当前逻辑，并对各采集点做异常保护。
        try:
            load1, _, _ = os.getloadavg()
            cpu_count = os.cpu_count() or 1
            cpu_percent = min(100.0, (load1 / cpu_count) * 100)
        except OSError:
            cpu_percent = None

        try:
            if os.path.exists("/proc/meminfo"):
                info = {}
                with open("/proc/meminfo", "r", encoding="utf-8") as f:
                    for line in f:
                        key, value = line.split(":", 1)
                        info[key] = int(value.strip().split()[0])
                mem_total = info.get("MemTotal")
                mem_available = info.get("MemAvailable")
                if mem_total and mem_available is not None:
                    memory_percent = (1 - mem_available / mem_total) * 100
            else:
                memory_percent = None
        except (OSError, ValueError):
            memory_percent = None

        try:
            st = os.statvfs("/")
            total = st.f_blocks * st.f_frsize
            free = st.f_bavail * st.f_frsize
            disk_percent = (1 - free / total) * 100 if total else None
        except OSError:
            disk_percent = None

        return MetricSnapshot(
            system_cpu_percent=cpu_percent,
            system_memory_percent=memory_percent,
            system_disk_percent=disk_percent,
        )

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


@dataclass(slots=True)
class TaskMetricSnapshot:
    """任务维度指标快照。"""

    task_queue_wait_seconds: float
    task_execution_seconds: float
    task_failure_rate: float
    task_cancel_rate: float
    task_total_count: int
    task_failed_count: int
    task_canceled_count: int

    def to_dict(self) -> dict:
        return asdict(self)


class TaskMetricsCollector:
    """任务指标聚合器。"""

    def __init__(self) -> None:
        self.total_count = 0
        self.failed_count = 0
        self.canceled_count = 0
        self.total_queue_wait_seconds = 0.0
        self.total_execution_seconds = 0.0

    def observe_queue_wait(self, wait_seconds: float) -> None:
        # 保护指标值，避免异常时间戳导致负数污染。
        self.total_queue_wait_seconds += max(0.0, wait_seconds)

    def observe_execution(self, execution_seconds: float) -> None:
        # 保护指标值，避免异常时间戳导致负数污染。
        self.total_execution_seconds += max(0.0, execution_seconds)

    def observe_terminal(self, event_type: str) -> None:
        self.total_count += 1
        if event_type == "failed":
            self.failed_count += 1
        if event_type == "canceled":
            self.canceled_count += 1

    def snapshot(self) -> TaskMetricSnapshot:
        base = self.total_count if self.total_count > 0 else 1
        return TaskMetricSnapshot(
            task_queue_wait_seconds=self.total_queue_wait_seconds,
            task_execution_seconds=self.total_execution_seconds,
            task_failure_rate=self.failed_count / base,
            task_cancel_rate=self.canceled_count / base,
            task_total_count=self.total_count,
            task_failed_count=self.failed_count,
            task_canceled_count=self.canceled_count,
        )
