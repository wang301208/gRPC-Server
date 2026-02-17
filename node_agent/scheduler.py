from __future__ import annotations

from dataclasses import dataclass

from node_agent.capability import Capability


@dataclass(slots=True)
class ScheduleDecision:
    """调度决策。"""

    accepted: bool
    device: str
    reason: str = ""


class ResourceScheduler:
    """根据任务请求和节点能力决定使用 CPU 或 GPU。"""

    def __init__(self, capability: Capability, gpu_vram_threshold_mb: int = 4096):
        self.capability = capability
        self.gpu_vram_threshold_mb = gpu_vram_threshold_mb

    def decide(self, require_gpu: bool, prefer_gpu: bool) -> ScheduleDecision:
        """给出调度结果。"""
        if require_gpu:
            if self.capability.gpu_available and self.capability.gpu_vram_mb >= self.gpu_vram_threshold_mb:
                return ScheduleDecision(accepted=True, device="gpu")
            return ScheduleDecision(accepted=False, device="none", reason="GPU 不可用或显存不足")

        if prefer_gpu and self.capability.gpu_available:
            return ScheduleDecision(accepted=True, device="gpu")

        return ScheduleDecision(accepted=True, device="cpu")
