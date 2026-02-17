from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass

from node_agent.models import DeployRequest


@dataclass(slots=True)
class DeployResult:
    """部署结果。"""

    ok: bool
    message: str


class DeployManager:
    """部署管理器，封装 docker compose 部署流程。"""

    def _build_command(self, request: DeployRequest) -> list[str]:
        """根据部署类型构建执行命令。"""
        if request.command:
            return request.command

        if request.deploy_type == "website":
            return ["docker", "compose", "up", "-d", request.service_name]

        cmd = ["docker", "compose", "up", "-d"]
        if request.service_name:
            cmd.append(request.service_name)
        return cmd

    async def deploy(self, request: DeployRequest) -> DeployResult:
        """执行部署并返回结果。"""
        image_type = "gpu" if request.require_gpu else "cpu"
        cmd = self._build_command(request)
        env = os.environ.copy()
        env.update(request.env)
        env["IMAGE_PROFILE"] = image_type
        env["DEPLOY_TARGET"] = request.deploy_type

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=request.workdir,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=env,
        )
        out, _ = await proc.communicate()
        if proc.returncode == 0:
            return DeployResult(ok=True, message=out.decode("utf-8", errors="ignore"))
        return DeployResult(ok=False, message=out.decode("utf-8", errors="ignore"))
