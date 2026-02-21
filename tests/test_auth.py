from node_agent.auth import AuthManager


def test_authenticate_with_encrypted_storage():
    manager = AuthManager(api_keys={"n1": "k1"})

    assert manager.api_keys["n1"].startswith("sha256$")
    assert manager.api_keys["n1"] != "k1"
    assert manager.authenticate("n1", "k1")
    assert not manager.authenticate("n1", "bad-key")


def test_authenticate_accepts_pre_encrypted_keys():
    seed = AuthManager(api_keys={"n1": "k1"})
    manager = AuthManager(api_keys={"n1": seed.api_keys["n1"]})

    assert manager.authenticate("n1", "k1")
