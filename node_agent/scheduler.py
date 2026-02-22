from __future__ import annotations

from dataclasses import dataclass, field

from node_agent.capability import Capability


@dataclass(slots=True)
class ScheduleDecision:
    """调度决策。"""

    accepted: bool
    device: str
    reason: str = ""


@dataclass(slots=True)
class ResourceRequest:
    """任务资源请求。"""

    cpu_cores: int = 1
    memory_mb: int = 256
    gpu_vram_mb: int = 0


@dataclass(slots=True)
class ScheduleInput:
    """调度输入参数。"""

    require_gpu: bool = False
    prefer_gpu: bool = False
    priority: int = 0
    timeout_sec: float = 0
    resource_request: ResourceRequest = field(default_factory=ResourceRequest)


@dataclass(slots=True)
class ResourceUsage:
    """当前资源占用。"""

    used_cpu_cores: int = 0
    used_memory_mb: int = 0
    used_gpu_vram_mb: int = 0


class ResourceScheduler:
    """根据任务请求和节点能力决定使用 CPU 或 GPU。"""

    def __init__(self, capability: Capability, gpu_vram_threshold_mb: int = 4096):
        self.capability = capability
        self.gpu_vram_threshold_mb = gpu_vram_threshold_mb

    def decide(self, schedule_input: ScheduleInput, usage: ResourceUsage | None = None) -> ScheduleDecision:
        """给出调度结果。"""
        usage = usage or ResourceUsage()
        request = schedule_input.resource_request

        remaining_cpu = self.capability.cpu_cores - usage.used_cpu_cores
        remaining_mem = self.capability.total_memory_mb - usage.used_memory_mb
        remaining_gpu_vram = self.capability.gpu_vram_mb - usage.used_gpu_vram_mb

        if request.cpu_cores > remaining_cpu:
            return ScheduleDecision(accepted=False, device="none", reason="CPU 资源不足")

        if request.memory_mb > remaining_mem:
            return ScheduleDecision(accepted=False, device="none", reason="内存资源不足")

        if schedule_input.require_gpu:
            threshold = max(self.gpu_vram_threshold_mb, request.gpu_vram_mb)
            if self.capability.gpu_available and remaining_gpu_vram >= threshold:
                return ScheduleDecision(accepted=True, device="gpu")
            return ScheduleDecision(accepted=False, device="none", reason="GPU 不可用或显存不足")

        if (
            schedule_input.prefer_gpu
            and self.capability.gpu_available
            and remaining_gpu_vram >= max(self.gpu_vram_threshold_mb, request.gpu_vram_mb)
        ):
            return ScheduleDecision(accepted=True, device="gpu")

        return ScheduleDecision(accepted=True, device="cpu")
