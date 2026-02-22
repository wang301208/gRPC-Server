from datetime import timedelta

from node_agent.auth import AuthManager, _utc_now


def test_authenticate_with_pbkdf2_storage():
    manager = AuthManager(api_keys={"n1": "k1"})

    assert manager.api_keys["n1"].primary.encrypted_key.startswith("pbkdf2_sha256$")
    assert manager.api_keys["n1"].primary.encrypted_key != "k1"
    assert manager.authenticate("n1", "k1")


def test_authenticate_legacy_sha256_and_rehash():
    legacy = "sha256$" + __import__("hashlib").sha256("k1".encode("utf-8")).hexdigest()
    manager = AuthManager(api_keys={"n1": legacy})

    assert manager.authenticate("n1", "k1")
    assert manager.api_keys["n1"].primary.encrypted_key.startswith("pbkdf2_sha256$")


def test_authenticate_rejects_wrong_key():
    manager = AuthManager(api_keys={"n1": "k1"})

    assert not manager.authenticate("n1", "bad-key")


def test_authenticate_rejects_expired_key():
    manager = AuthManager(
        api_keys={
            "n1": {
                "key": "k1",
                "created_at": _utc_now().isoformat(),
                "expires_at": (_utc_now() - timedelta(seconds=1)).isoformat(),
                "scopes": ["task.submit"],
                "role": "operator",
            }
        }
    )

    assert not manager.authenticate("n1", "k1")


def test_key_rotation_accepts_old_and_new_key_within_window():
    manager = AuthManager(api_keys={"n1": "old-key"})

    manager.rotate_api_key("n1", "new-key", grace_period_seconds=30)

    assert manager.authenticate("n1", "old-key")
    assert manager.authenticate("n1", "new-key")
