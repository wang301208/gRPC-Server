from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from typing import Dict


@dataclass
class AuthManager:
    """API Key 认证管理器。"""

    api_keys: Dict[str, str]

    def __post_init__(self) -> None:
        self.api_keys = {node_id: self._encrypt_api_key(api_key) for node_id, api_key in self.api_keys.items()}

    @staticmethod
    def _encrypt_api_key(api_key: str) -> str:
        """将 API Key 进行不可逆加密，避免明文驻留内存。"""
        if api_key.startswith("sha256$"):
            return api_key
        encrypted = hashlib.sha256(api_key.encode("utf-8")).hexdigest()
        return f"sha256${encrypted}"

    def authenticate(self, node_id: str, api_key: str) -> bool:
        """校验节点凭证。"""
        expected = self.api_keys.get(node_id)
        if expected is None:
            return False
        provided = self._encrypt_api_key(api_key)
        return hmac.compare_digest(expected, provided)
