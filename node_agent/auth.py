from __future__ import annotations

import hashlib
import hmac
import os
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Set


def _utc_now() -> datetime:
    """返回带时区的 UTC 当前时间。"""
    return datetime.now(timezone.utc)


def _parse_datetime(value: Any) -> Optional[datetime]:
    """兼容 datetime 或 ISO 字符串输入。"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
    raise TypeError(f"不支持的时间格式: {type(value).__name__}")


@dataclass(slots=True)
class KeyMetadata:
    """API Key 元数据：创建时间、过期时间、权限范围与角色。"""

    created_at: datetime
    expires_at: Optional[datetime] = None
    scopes: Set[str] = field(default_factory=set)
    role: str = "admin"


@dataclass(slots=True)
class KeyRecord:
    """已加密的单个密钥记录。"""

    encrypted_key: str
    metadata: KeyMetadata


@dataclass(slots=True)
class AuthContext:
    """鉴权后上下文，供后续权限判断使用。"""

    node_id: str
    role: str
    scopes: Set[str]


@dataclass(slots=True)
class NodeKeyBundle:
    """节点密钥包：支持主密钥 + 轮换期旧密钥。"""

    primary: KeyRecord
    secondary: Optional[KeyRecord] = None
    secondary_valid_until: Optional[datetime] = None


@dataclass
class AuthManager:
    """API Key 认证管理器。"""

    api_keys: Dict[str, Any]
    PBKDF2_ITERATIONS = 120_000

    def __post_init__(self) -> None:
        bundles: Dict[str, NodeKeyBundle] = {}
        for node_id, config in self.api_keys.items():
            if isinstance(config, str):
                metadata = KeyMetadata(
                    created_at=_utc_now(),
                    scopes={"task.submit", "task.cancel", "deploy"},
                    role="admin",
                )
                bundles[node_id] = NodeKeyBundle(primary=KeyRecord(encrypted_key=self._encrypt_api_key(config), metadata=metadata))
                continue

            key_value = config["key"]
            scopes = set(config.get("scopes") or {"task.submit", "task.cancel", "deploy"})
            metadata = KeyMetadata(
                created_at=_parse_datetime(config.get("created_at")) or _utc_now(),
                expires_at=_parse_datetime(config.get("expires_at")),
                scopes=scopes,
                role=config.get("role", "admin"),
            )
            bundles[node_id] = NodeKeyBundle(
                primary=KeyRecord(
                    encrypted_key=self._encrypt_api_key(key_value),
                    metadata=metadata,
                )
            )
        self.api_keys = bundles

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

    @staticmethod
    def _record_expired(record: KeyRecord) -> bool:
        """判断密钥是否已过期。"""
        return record.metadata.expires_at is not None and _utc_now() >= record.metadata.expires_at

    def _verify_record(self, record: KeyRecord, api_key: str) -> bool:
        expected = record.encrypted_key

        if expected.startswith("pbkdf2_sha256$"):
            return self._verify_pbkdf2(api_key, expected)

        # 兼容旧版 sha256$ 格式，并在成功后升级到新格式。
        if expected.startswith("sha256$"):
            legacy_hash = hashlib.sha256(api_key.encode("utf-8")).hexdigest()
            if hmac.compare_digest(expected, f"sha256${legacy_hash}"):
                if hasattr(os, "urandom"):
                    record.encrypted_key = self._encrypt_api_key(api_key)
                return True
            return False

        return False

    def rotate_api_key(
        self,
        node_id: str,
        new_api_key: str,
        *,
        grace_period_seconds: int,
        created_at: Optional[datetime] = None,
        expires_at: Optional[datetime] = None,
        scopes: Optional[Set[str]] = None,
        role: str = "admin",
    ) -> None:
        """执行密钥轮换：在过渡期内同时接受旧新两个 key。"""
        bundle = self.api_keys.get(node_id)
        if bundle is None:
            raise KeyError(f"节点不存在: {node_id}")

        bundle.secondary = bundle.primary
        bundle.secondary_valid_until = _utc_now() + timedelta(seconds=grace_period_seconds)
        bundle.primary = KeyRecord(
            encrypted_key=self._encrypt_api_key(new_api_key),
            metadata=KeyMetadata(
                created_at=created_at or _utc_now(),
                expires_at=expires_at,
                scopes=scopes or {"task.submit", "task.cancel", "deploy"},
                role=role,
            ),
        )

    def authenticate_with_context(self, node_id: str, api_key: str) -> Optional[AuthContext]:
        """校验节点凭证并返回权限上下文。"""
        bundle = self.api_keys.get(node_id)
        if bundle is None:
            return None

        # 清理已过轮换窗口的旧 key。
        if bundle.secondary is not None and bundle.secondary_valid_until is not None and _utc_now() > bundle.secondary_valid_until:
            bundle.secondary = None
            bundle.secondary_valid_until = None

        if not self._record_expired(bundle.primary) and self._verify_record(bundle.primary, api_key):
            return AuthContext(node_id=node_id, role=bundle.primary.metadata.role, scopes=set(bundle.primary.metadata.scopes))

        if bundle.secondary is not None:
            secondary_window_valid = bundle.secondary_valid_until is not None and _utc_now() <= bundle.secondary_valid_until
            if secondary_window_valid and not self._record_expired(bundle.secondary) and self._verify_record(bundle.secondary, api_key):
                return AuthContext(node_id=node_id, role=bundle.secondary.metadata.role, scopes=set(bundle.secondary.metadata.scopes))

        return None

    def authenticate(self, node_id: str, api_key: str) -> bool:
        """校验节点凭证。"""
        return self.authenticate_with_context(node_id, api_key) is not None

    @staticmethod
    def authorize(context: AuthContext, *, required_scope: str, allowed_roles: Set[str]) -> bool:
        """基于角色与权限范围进行访问控制。"""
        return context.role in allowed_roles and required_scope in context.scopes
