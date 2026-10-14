import importlib

import pytest

from src.config import config


@pytest.mark.parametrize("value", ["", None, "   ", "flowpassword", "Testing", "ADMIN123", "changeme-run-generate-secrets",
                                   "short", "12345678901"])
def test_weak_secrets_are_rejected(value):
    assert config.weak_secret_reason(value) != ""


def test_strong_secret_is_accepted():
    assert config.weak_secret_reason("a3f9c1d2e4b5a6978c0d1e2f3a4b5c6d") == ""


def test_reason_never_contains_the_secret():
    secret = "supersecretvalue-but-short"[:5]
    assert secret not in config.weak_secret_reason(secret)


@pytest.fixture(autouse=True)
def _restore_config(monkeypatch):
    """_reload() re-executes config.py in place; put the module back to its un-patched state afterwards so
    other test files do not see values from these tests."""
    yield
    monkeypatch.undo()
    importlib.reload(config)


def _reload(monkeypatch, **env):
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return importlib.reload(config)


def test_import_refuses_weak_secrets(monkeypatch):
    with pytest.raises(RuntimeError) as err:
        _reload(monkeypatch, ALLOW_DEFAULT_SECRETS="false", RABBITMQ_PASSWORD="flowpassword",
                MARIADB_PASSWORD="a3f9c1d2e4b5a6978c0d1e2f", SWIFT_ENABLED="false")
    assert "RABBITMQ_PASSWORD" in str(err.value)
    assert "flowpassword" not in str(err.value)


def test_import_accepts_strong_secrets(monkeypatch):
    cfg = _reload(monkeypatch, ALLOW_DEFAULT_SECRETS="false", RABBITMQ_PASSWORD="a3f9c1d2e4b5a6978c0d1e2f",
                  MARIADB_PASSWORD="b3f9c1d2e4b5a6978c0d1e2f", SWIFT_ENABLED="false")
    assert cfg.ALLOW_DEFAULT_SECRETS is False


def test_swift_key_only_checked_when_swift_enabled(monkeypatch):
    kw = dict(ALLOW_DEFAULT_SECRETS="false", RABBITMQ_PASSWORD="a3f9c1d2e4b5a6978c0d1e2f",
              MARIADB_PASSWORD="b3f9c1d2e4b5a6978c0d1e2f", SWIFT_KEY="testing")
    _reload(monkeypatch, SWIFT_ENABLED="false", **kw)
    with pytest.raises(RuntimeError) as err:
        _reload(monkeypatch, SWIFT_ENABLED="true", **kw)
    assert "SWIFT_KEY" in str(err.value)


def test_unexpanded_shell_reference_falls_back_to_default(monkeypatch, capsys):
    monkeypatch.setenv("X_TEST_PORT", "${X_TEST_PORT}")
    assert config._int_env("X_TEST_PORT", 42) == 42
    assert "unexpanded" in capsys.readouterr().err


def test_int_env_reads_value(monkeypatch):
    monkeypatch.setenv("X_TEST_PORT", "7")
    assert config._int_env("X_TEST_PORT", 42) == 7


@pytest.mark.parametrize("raw,expected", [("true", True), ("1", True), ("yes", True), ("on", True),
                                          ("false", False), ("", False), ("nope", False)])
def test_bool_env(monkeypatch, raw, expected):
    monkeypatch.setenv("X_TEST_FLAG", raw)
    assert config._bool_env("X_TEST_FLAG") is expected


def test_host_lists_are_built_from_node_ips(monkeypatch):
    # AGENTS rule: RABBITMQ_HOSTS / MARIADB_HOSTS stay blank in .env; config builds them from NODE*_IP.
    cfg = _reload(monkeypatch, RABBITMQ_HOSTS="", MARIADB_HOSTS="", NODE1_IP="10.0.0.1", NODE2_IP="10.0.0.2",
                  NODE3_IP="10.0.0.3", RABBITMQ_PORT="5672")
    assert cfg.RABBITMQ_HOSTS == "10.0.0.1:5672,10.0.0.2:5672,10.0.0.3:5672"
    assert cfg.MARIADB_HOSTS == "10.0.0.1,10.0.0.2,10.0.0.3"


def test_publish_connections_are_clamped(monkeypatch):
    assert _reload(monkeypatch, API_PUBLISH_CONNECTIONS="0").API_PUBLISH_CONNECTIONS == 1
    assert _reload(monkeypatch, API_PUBLISH_CONNECTIONS="999").API_PUBLISH_CONNECTIONS == 32
    assert _reload(monkeypatch, API_PUBLISH_CONNECTIONS="6").API_PUBLISH_CONNECTIONS == 6


def test_zk_connection_state_never_connects(monkeypatch):
    """The readiness probe reads this; it must report local state and never create a client (that would block)."""
    from src.zookeeper import zk
    monkeypatch.setattr(zk, "_client", None)
    monkeypatch.setattr(zk, "get_client", lambda: (_ for _ in ()).throw(AssertionError("connected")))
    assert zk.connection_state() == "not_started"

    class Fake:
        connected = True
    monkeypatch.setattr(zk, "_client", Fake())
    assert zk.connection_state() == "connected"
    Fake.connected = False
    assert zk.connection_state() == "disconnected"
