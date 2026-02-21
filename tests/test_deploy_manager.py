import asyncio

from node_agent.deploy_manager import DeployManager
from node_agent.models import DeployRequest


class DummyProc:
    def __init__(self, returncode=0, output=b"ok"):
        self.returncode = returncode
        self._output = output

    async def communicate(self):
        return self._output, b""


def test_deploy_website_uses_service_name_and_target_env(monkeypatch):
    async def _run():
        manager = DeployManager()
        captured = {}

        async def fake_create_subprocess_exec(*cmd, **kwargs):
            captured["cmd"] = list(cmd)
            captured["env"] = kwargs["env"]
            return DummyProc(returncode=0, output=b"deployed")

        monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

        req = DeployRequest(
            service_name="web",
            workdir=".",
            deploy_type="website",
            require_gpu=False,
            env={"CUSTOM_DEPLOY": "yes"},
        )

        result = await manager.deploy(req)
        assert result.ok
        assert captured["cmd"] == ["docker", "compose", "up", "-d", "web"]
        assert captured["env"]["DEPLOY_TARGET"] == "website"
        assert captured["env"]["CUSTOM_DEPLOY"] == "yes"
        assert "PATH" in captured["env"]

    asyncio.run(_run())


def test_deploy_respects_custom_command(monkeypatch):
    async def _run():
        manager = DeployManager()
        captured = {}

        async def fake_create_subprocess_exec(*cmd, **kwargs):
            captured["cmd"] = list(cmd)
            captured["env"] = kwargs["env"]
            return DummyProc(returncode=1, output=b"failed")

        monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

        req = DeployRequest(
            service_name="ignored",
            workdir=".",
            command=["bash", "-lc", "echo deploy website"],
            deploy_type="website",
            require_gpu=True,
        )

        result = await manager.deploy(req)
        assert not result.ok
        assert captured["cmd"] == ["bash", "-lc", "echo deploy website"]
        assert captured["env"]["IMAGE_PROFILE"] == "gpu"

    asyncio.run(_run())


def test_deploy_command_not_found_returns_diagnosable_message(monkeypatch):
    async def _run():
        manager = DeployManager()

        async def fake_create_subprocess_exec(*cmd, **kwargs):
            raise FileNotFoundError("docker not found")

        monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)

        req = DeployRequest(
            service_name="web",
            workdir=".",
            deploy_type="website",
            require_gpu=False,
        )

        result = await manager.deploy(req)
        assert not result.ok
        assert "FileNotFoundError" in result.message
        assert "docker compose up -d web" in result.message
        assert "命令不存在或不可用" in result.message

    asyncio.run(_run())
