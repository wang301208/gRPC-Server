from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass
class AuthManager:
    """API Key 认证管理器。"""

    api_keys: Dict[str, str]

    def authenticate(self, node_id: str, api_key: str) -> bool:
        """校验节点凭证。"""
        expected = self.api_keys.get(node_id)
        return expected is not None and expected == api_key
