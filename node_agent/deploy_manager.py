from __future__ import annotations

import asyncio
from dataclasses import dataclass

from node_agent.models import DeployRequest


@dataclass(slots=True)
class DeployResult:
    """部署结果。"""

    ok: bool
    message: str


class DeployManager:
    """部署管理器，封装 docker compose 部署流程。"""

    async def deploy(self, request: DeployRequest) -> DeployResult:
        """执行部署并返回结果。"""
        image_type = "gpu" if request.require_gpu else "cpu"
        cmd = ["docker", "compose", "up", "-d"]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=request.workdir,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env={"IMAGE_PROFILE": image_type},
        )
        out, _ = await proc.communicate()
        if proc.returncode == 0:
            return DeployResult(ok=True, message=out.decode("utf-8", errors="ignore"))
        return DeployResult(ok=False, message=out.decode("utf-8", errors="ignore"))
