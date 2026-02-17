from __future__ import annotations

import os
import re
import shutil
import subprocess
from dataclasses import dataclass, asdict


@dataclass(slots=True)
class Capability:
    """节点硬件能力。"""

    cpu_cores: int
    total_memory_mb: int
    total_disk_gb: int
    gpu_available: bool
    gpu_name: str = ""
    gpu_vram_mb: int = 0
    cuda_version: str = ""
    driver_version: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def _read_mem_mb() -> int:
    """读取总内存。"""
    if os.path.exists("/proc/meminfo"):
        with open("/proc/meminfo", "r", encoding="utf-8") as f:
            first = f.readline()
        kb = int(re.findall(r"\d+", first)[0])
        return kb // 1024
    return 0


def detect_capability() -> Capability:
    """自动检测 CPU/GPU/内存/磁盘信息。"""
    cpu_cores = os.cpu_count() or 1
    total_memory_mb = _read_mem_mb()
    disk = shutil.disk_usage("/")
    total_disk_gb = int(disk.total / (1024**3))

    gpu_available = False
    gpu_name = ""
    gpu_vram_mb = 0
    cuda_version = ""
    driver_version = ""

    if shutil.which("nvidia-smi"):
        try:
            # 通过 nvidia-smi 获取核心字段，避免强依赖第三方库。
            cmd = [
                "nvidia-smi",
                "--query-gpu=name,memory.total,driver_version",
                "--format=csv,noheader,nounits",
            ]
            out = subprocess.check_output(cmd, text=True, timeout=3).strip().splitlines()
            if out:
                first = out[0]
                parts = [p.strip() for p in first.split(",")]
                gpu_name = parts[0]
                gpu_vram_mb = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
                driver_version = parts[2] if len(parts) > 2 else ""
                gpu_available = True
            cuda_out = subprocess.check_output(["nvidia-smi"], text=True, timeout=3)
            m = re.search(r"CUDA Version:\s*([\d.]+)", cuda_out)
            if m:
                cuda_version = m.group(1)
        except (subprocess.SubprocessError, ValueError):
            gpu_available = False

    return Capability(
        cpu_cores=cpu_cores,
        total_memory_mb=total_memory_mb,
        total_disk_gb=total_disk_gb,
        gpu_available=gpu_available,
        gpu_name=gpu_name,
        gpu_vram_mb=gpu_vram_mb,
        cuda_version=cuda_version,
        driver_version=driver_version,
    )
