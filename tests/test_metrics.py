import time
from types import SimpleNamespace
from unittest.mock import mock_open

from node_agent.metrics import MetricSnapshot, MetricsCollector


def test_metrics_collector_can_restart():
    collector = MetricsCollector(interval_sec=0.05)
    events = []

    collector.start(lambda m: events.append(m.to_dict()))
    time.sleep(0.12)
    collector.stop()

    first_count = len(events)
    assert first_count > 0

    collector.start(lambda m: events.append(m.to_dict()))
    time.sleep(0.12)
    collector.stop()

    assert len(events) > first_count


def test_collect_once_when_getloadavg_raises(monkeypatch):
    # 强制走无 psutil 的回退路径，验证 os.getloadavg 异常时依然返回结构化结果。
    monkeypatch.setattr("node_agent.metrics._PSUTIL", None)
    monkeypatch.setattr("node_agent.metrics.os.getloadavg", lambda: (_ for _ in ()).throw(OSError("boom")))
    monkeypatch.setattr("node_agent.metrics.os.path.exists", lambda p: True)
    monkeypatch.setattr(
        "builtins.open",
        mock_open(read_data="MemTotal: 1000 kB\nMemAvailable: 200 kB\n"),
    )
    monkeypatch.setattr(
        "node_agent.metrics.os.statvfs",
        lambda _: SimpleNamespace(f_blocks=100, f_frsize=1, f_bavail=25),
    )

    snapshot = MetricsCollector.collect_once()

    assert isinstance(snapshot, MetricSnapshot)
    assert snapshot.cpu_percent is None
    assert snapshot.memory_percent == 80.0
    assert snapshot.disk_percent == 75.0


def test_collect_once_without_proc_meminfo(monkeypatch):
    # 强制走无 psutil 的回退路径，模拟 /proc/meminfo 缺失场景。
    monkeypatch.setattr("node_agent.metrics._PSUTIL", None)
    monkeypatch.setattr("node_agent.metrics.os.getloadavg", lambda: (1.0, 0.5, 0.2))
    monkeypatch.setattr("node_agent.metrics.os.cpu_count", lambda: 2)
    monkeypatch.setattr("node_agent.metrics.os.path.exists", lambda p: False)
    monkeypatch.setattr(
        "node_agent.metrics.os.statvfs",
        lambda _: SimpleNamespace(f_blocks=200, f_frsize=1, f_bavail=100),
    )

    snapshot = MetricsCollector.collect_once()

    assert isinstance(snapshot.to_dict(), dict)
    assert snapshot.cpu_percent == 50.0
    assert snapshot.memory_percent is None
    assert snapshot.disk_percent == 50.0
