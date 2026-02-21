from node_agent.auth import AuthManager


def test_authenticate_with_pbkdf2_storage():
    manager = AuthManager(api_keys={"n1": "k1"})

    assert manager.api_keys["n1"].startswith("pbkdf2_sha256$")
    assert manager.api_keys["n1"] != "k1"
    assert manager.authenticate("n1", "k1")


def test_authenticate_legacy_sha256_and_rehash():
    legacy = "sha256$" + __import__("hashlib").sha256("k1".encode("utf-8")).hexdigest()
    manager = AuthManager(api_keys={"n1": legacy})

    assert manager.authenticate("n1", "k1")
    assert manager.api_keys["n1"].startswith("pbkdf2_sha256$")


def test_authenticate_rejects_wrong_key():
    manager = AuthManager(api_keys={"n1": "k1"})

    assert not manager.authenticate("n1", "bad-key")
