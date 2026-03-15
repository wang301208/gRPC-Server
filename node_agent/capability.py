from __future__ import annotations

import importlib
import importlib.util
import os
import re
import shutil
import subprocess
from dataclasses import asdict, dataclass, field


_HAS_PSUTIL = importlib.util.find_spec("psutil") is not None
_PSUTIL = importlib.import_module("psutil") if _HAS_PSUTIL else None


@dataclass(slots=True)
class GpuDevice:
    """单张 GPU 设备信息。"""

    index: int
    name: str
    total_vram_mb: int


@dataclass(slots=True)
class Capability:
    """节点硬件能力。"""

    cpu_cores: int
    total_memory_mb: int
    total_disk_gb: int
    gpu_available: bool
    gpus: list[GpuDevice] = field(default_factory=list)
    # 兼容字段：保留首张 GPU 信息，后续建议改用 gpus。
    gpu_name: str = ""
    gpu_vram_mb: int = 0
    cuda_version: str = ""
    driver_version: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def _read_mem_mb() -> int:
    """读取总内存。"""
    if _PSUTIL is not None:
        return int(_PSUTIL.virtual_memory().total / (1024**2))
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
    gpus: list[GpuDevice] = []
    gpu_name = ""
    gpu_vram_mb = 0
    cuda_version = ""
    driver_version = ""

    if shutil.which("nvidia-smi"):
        try:
            # 通过 nvidia-smi 获取核心字段，避免强依赖第三方库。
            cmd = [
                "nvidia-smi",
                "--query-gpu=index,name,memory.total,driver_version",
                "--format=csv,noheader,nounits",
            ]
            out = subprocess.check_output(cmd, text=True, timeout=3, stdin=subprocess.DEVNULL).strip().splitlines()
            if out:
                for line in out:
                    parts = [p.strip() for p in line.split(",")]
                    if len(parts) < 4:
                        continue
                    index_raw, name, total_vram_raw, drv_version = parts[0], parts[1], parts[2], parts[3]
                    if not index_raw.isdigit():
                        continue
                    total_vram_mb = int(total_vram_raw) if total_vram_raw.isdigit() else 0
                    gpus.append(
                        GpuDevice(
                            index=int(index_raw),
                            name=name,
                            total_vram_mb=total_vram_mb,
                        )
                    )
                    driver_version = drv_version

                if gpus:
                    # 兼容旧字段：沿用首张卡信息。
                    gpu_name = gpus[0].name
                    gpu_vram_mb = gpus[0].total_vram_mb
                    gpus.sort(key=lambda gpu: gpu.index)
                    gpu_available = True
            cuda_out = subprocess.check_output(["nvidia-smi"], text=True, timeout=3, stdin=subprocess.DEVNULL)
            m = re.search(r"CUDA Version:\s*([\d.]+)", cuda_out)
            if m:
                cuda_version = m.group(1)
        except (subprocess.SubprocessError, ValueError, OSError):
            gpu_available = False

    return Capability(
        cpu_cores=cpu_cores,
        total_memory_mb=total_memory_mb,
        total_disk_gb=total_disk_gb,
        gpu_available=gpu_available,
        gpus=gpus,
        gpu_name=gpu_name,
        gpu_vram_mb=gpu_vram_mb,
        cuda_version=cuda_version,
        driver_version=driver_version,
    )
