from node_agent.capability import Capability
from node_agent.scheduler import ResourceRequest, ResourceScheduler, ResourceUsage, ScheduleInput


def test_require_gpu_rejected_when_unavailable():
    cap = Capability(4, 1024, 20, gpu_available=False)
    scheduler = ResourceScheduler(cap)
    decision = scheduler.decide(ScheduleInput(require_gpu=True, resource_request=ResourceRequest(gpu_vram_mb=1024)))
    assert not decision.accepted


def test_prefer_gpu_falls_back_cpu():
    cap = Capability(4, 1024, 20, gpu_available=False)
    scheduler = ResourceScheduler(cap)
    decision = scheduler.decide(ScheduleInput(prefer_gpu=True))
    assert decision.accepted
    assert decision.device == "cpu"


def test_reject_when_memory_insufficient():
    cap = Capability(4, 1024, 20, gpu_available=False)
    scheduler = ResourceScheduler(cap)
    decision = scheduler.decide(
        ScheduleInput(resource_request=ResourceRequest(memory_mb=512)),
        usage=ResourceUsage(used_memory_mb=700),
    )
    assert not decision.accepted
    assert decision.reason == "内存资源不足"
