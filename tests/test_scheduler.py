from node_agent.capability import Capability
from node_agent.scheduler import ResourceScheduler


def test_require_gpu_rejected_when_unavailable():
    cap = Capability(4, 1024, 20, gpu_available=False)
    scheduler = ResourceScheduler(cap)
    decision = scheduler.decide(require_gpu=True, prefer_gpu=False)
    assert not decision.accepted


def test_prefer_gpu_falls_back_cpu():
    cap = Capability(4, 1024, 20, gpu_available=False)
    scheduler = ResourceScheduler(cap)
    decision = scheduler.decide(require_gpu=False, prefer_gpu=True)
    assert decision.accepted
    assert decision.device == "cpu"
