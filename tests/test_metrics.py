import time

from node_agent.metrics import MetricsCollector


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
