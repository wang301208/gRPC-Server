from node_agent.capability import Capability, GpuDevice
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


def test_multi_gpu_tasks_use_different_cards():
    cap = Capability(
        8,
        4096,
        20,
        gpu_available=True,
        gpus=[
            GpuDevice(index=0, name="GPU-0", total_vram_mb=4096),
            GpuDevice(index=1, name="GPU-1", total_vram_mb=4096),
        ],
    )
    scheduler = ResourceScheduler(cap, gpu_vram_threshold_mb=2048)
    usage = ResourceUsage()

    first = scheduler.decide(ScheduleInput(require_gpu=True, resource_request=ResourceRequest(gpu_vram_mb=2048)), usage)
    assert first.accepted
    assert first.device == "gpu"
    assert len(first.gpu_indices) == 1

    usage.used_gpu_vram_by_index[first.gpu_indices[0]] = 2048
    second = scheduler.decide(ScheduleInput(require_gpu=True, resource_request=ResourceRequest(gpu_vram_mb=2048)), usage)
    assert second.accepted
    assert second.device == "gpu"
    assert len(second.gpu_indices) == 1
    assert second.gpu_indices[0] != first.gpu_indices[0]


def test_multi_gpu_fallback_to_other_or_reject():
    cap = Capability(
        8,
        4096,
        20,
        gpu_available=True,
        gpus=[
            GpuDevice(index=0, name="GPU-0", total_vram_mb=4096),
            GpuDevice(index=1, name="GPU-1", total_vram_mb=4096),
        ],
    )
    scheduler = ResourceScheduler(cap, gpu_vram_threshold_mb=2048)

    # 0 号卡显存不足时，应该自动改派到 1 号卡。
    usage = ResourceUsage(used_gpu_vram_by_index={0: 3072})
    decision = scheduler.decide(ScheduleInput(require_gpu=True, resource_request=ResourceRequest(gpu_vram_mb=2048)), usage)
    assert decision.accepted
    assert decision.gpu_indices == [1]

    # 两张卡都不足时，必须拒绝 require_gpu 任务。
    usage = ResourceUsage(used_gpu_vram_by_index={0: 3072, 1: 3072})
    rejected = scheduler.decide(
        ScheduleInput(require_gpu=True, resource_request=ResourceRequest(gpu_vram_mb=2048)),
        usage,
    )
    assert not rejected.accepted
    assert rejected.reason == "GPU 不可用或显存不足"
