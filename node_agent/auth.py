from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass
from typing import Dict


@dataclass
class AuthManager:
    """API Key 认证管理器。"""

    api_keys: Dict[str, str]
    PBKDF2_ITERATIONS = 120_000

    def __post_init__(self) -> None:
        self.api_keys = {node_id: self._encrypt_api_key(api_key) for node_id, api_key in self.api_keys.items()}

    @staticmethod
    def _encrypt_api_key(api_key: str) -> str:
        """将 API Key 进行不可逆加密，避免明文驻留内存。"""
        # 兼容历史和已加密格式，避免重复加密。
        if api_key.startswith("sha256$") or api_key.startswith("pbkdf2_sha256$"):
            return api_key

        # 为每个 key 生成独立随机盐，使用 PBKDF2 派生密钥。
        salt = secrets.token_hex(16)
        derived = hashlib.pbkdf2_hmac(
            "sha256",
            api_key.encode("utf-8"),
            bytes.fromhex(salt),
            AuthManager.PBKDF2_ITERATIONS,
        )
        return f"pbkdf2_sha256${AuthManager.PBKDF2_ITERATIONS}${salt}${derived.hex()}"

    @staticmethod
    def _verify_pbkdf2(api_key: str, expected: str) -> bool:
        """验证 PBKDF2 格式密文。"""
        try:
            algorithm, iterations, salt, expected_hash = expected.split("$", 3)
            if algorithm != "pbkdf2_sha256":
                return False
            derived = hashlib.pbkdf2_hmac(
                "sha256",
                api_key.encode("utf-8"),
                bytes.fromhex(salt),
                int(iterations),
            )
            return hmac.compare_digest(expected_hash, derived.hex())
        except (TypeError, ValueError):
            return False

    def authenticate(self, node_id: str, api_key: str) -> bool:
        """校验节点凭证。"""
        expected = self.api_keys.get(node_id)
        if expected is None:
            return False

        if expected.startswith("pbkdf2_sha256$"):
            return self._verify_pbkdf2(api_key, expected)

        # 兼容旧版 sha256$ 格式，并在成功后升级到新格式。
        if expected.startswith("sha256$"):
            legacy_hash = hashlib.sha256(api_key.encode("utf-8")).hexdigest()
            if hmac.compare_digest(expected, f"sha256${legacy_hash}"):
                if hasattr(os, "urandom"):
                    self.api_keys[node_id] = self._encrypt_api_key(api_key)
                return True
            return False

        return False
