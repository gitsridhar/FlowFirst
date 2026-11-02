"""scripts/swift_object_demo.py: target resolution, object naming, table rendering, CLI parsing. No Swift server needed."""
import importlib.util
import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="module")
def demo():
    spec = importlib.util.spec_from_file_location("swift_object_demo", os.path.join(ROOT, "scripts", "swift_object_demo.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


TARGETS = {
    "mgmtnode1": "http://10.0.0.1:8888/auth/v1.0",
    "mgmtnode2": "http://10.0.0.2:8888/auth/v1.0",
    "mgmtnode3": "http://10.0.0.3:8888/auth/v1.0",
    "vip": "http://10.0.0.9:8090/auth/v1.0",
}


def test_resolve_named_host_and_url(demo):
    assert demo.resolve_target("mgmtnode2", TARGETS) == ("mgmtnode2", "http://10.0.0.2:8888/auth/v1.0")
    assert demo.resolve_target("vip", TARGETS) == ("vip", "http://10.0.0.9:8090/auth/v1.0")
    label, url = demo.resolve_target("192.168.1.50:8890", TARGETS)
    assert label == "192.168.1.50:8890" and url.endswith("://192.168.1.50:8890/auth/v1.0")
    assert demo.resolve_target("https://sw.example:8443/auth/v1.0", TARGETS) == ("sw.example:8443", "https://sw.example:8443/auth/v1.0")


def test_resolve_empty_is_error(demo):
    with pytest.raises(ValueError):
        demo.resolve_target("  ", TARGETS)
    with pytest.raises(ValueError):
        demo.resolve_targets(" , ", TARGETS)


def test_resolve_invalid_port(demo):
    with pytest.raises(ValueError, match="invalid port"):
        demo.resolve_target("10.0.0.1:abc", TARGETS)


def test_resolve_targets_drops_duplicate_urls(demo):
    res = demo.resolve_targets("mgmtnode1,mgmtnode2,mgmtnode1", TARGETS)
    assert [label for label, _ in res] == ["mgmtnode1", "mgmtnode2"]


def test_endpoint_of(demo):
    assert demo.endpoint_of("http://10.0.0.1:8888/auth/v1.0") == "10.0.0.1:8888"


def test_demo_container_never_equals_pipeline_container(demo):
    from src.config.config import SWIFT_CONTAINER

    name = demo.container_name("abc12345")
    assert name.startswith(demo.DEMO_PREFIX + "_")
    assert name != SWIFT_CONTAINER
    assert "/" not in name


def test_etag_is_md5_of_content(demo):
    assert demo.etag_of(b"") == "d41d8cd98f00b204e9800998ecf8427e"
    assert demo.etag_of(b"abc") == "900150983cd24fb0d6963f7d28e17f72"


def test_describe_error_hides_message(demo):
    from swiftclient.client import ClientException

    e = ClientException("Object PUT failed: http://secret-host:8888/v1/AUTH_x/c/o 503 Service Unavailable", http_status=503)
    out = demo.describe_error(e)
    assert out == "HTTP 503" and "secret-host" not in out
    assert demo.describe_error(OSError("host 10.1.1.1 refused")) == "OSError"


def test_wait_for_returns_when_done_and_on_timeout(demo):
    calls = []

    def flaky():
        calls.append(1)
        return len(calls) >= 3, "late"
    ok, _detail, _ms = demo.wait_for(flaky, 2.0, 0.01)
    assert ok and len(calls) == 3

    ok, detail, _ms = demo.wait_for(lambda: (False, "never"), 0.05, 0.01)
    assert not ok and detail == "never"


def test_render_table_aligns_and_handles_none(demo):
    out = demo.render_table(
        [{"a": "mgmtnode1", "b": None}, {"a": "mgmtnode2", "b": "5.1"}],
        [("a", "Node"), ("b", "Login")],
    )
    lines = out.splitlines()
    assert lines[0].startswith("Node ") and "Login" in lines[0]
    assert len(lines) == 4
    assert "None" not in out


def test_every_run_stage_has_a_table_definition(demo):
    assert set(demo.RUN_ORDER) == set(demo.STAGES)
    for _title, func, cols in demo.STAGES.values():
        assert callable(func) and cols


def test_cli_parsing(demo):
    p = demo.build_parser()
    assert p.parse_args(["run", "--keep"]).keep is True
    assert p.parse_args(["status", "--nodes", "mgmtnode1,mgmtnode2"]).nodes == "mgmtnode1,mgmtnode2"
    assert p.parse_args(["cleanup", "--run-id", "abc"]).func is demo.cmd_cleanup
    with pytest.raises(SystemExit):
        p.parse_args(["cleanup"])        # --run-id is required
    with pytest.raises(SystemExit):
        p.parse_args(["status", "--keep"])    # status creates nothing, so it has no --keep


def test_main_missing_credentials_is_usage_error(demo, monkeypatch, capsys):
    monkeypatch.setattr(demo, "SWIFT_USER", "")
    assert demo.main(["status"]) == 2
    assert "SWIFT_USER" in capsys.readouterr().err


def test_main_bad_node_is_usage_error_not_traceback(demo, monkeypatch, capsys):
    monkeypatch.setattr(demo, "SWIFT_USER", "acct:user")
    monkeypatch.setattr(demo, "SWIFT_KEY", "k" * 16)
    assert demo.main(["status", "--nodes", " "]) == 2
    assert "no nodes given" in capsys.readouterr().err


def test_unreachable_proxy_is_reported_not_raised(demo, monkeypatch, capsys):
    """Nothing listens on port 1: status must print 'unreachable' and exit 1, never a traceback."""
    monkeypatch.setattr(demo, "SWIFT_USER", "acct:user")
    monkeypatch.setattr(demo, "SWIFT_KEY", "k" * 16)
    assert demo.main(["status", "--nodes", "127.0.0.1:1", "--timeout", "1"]) == 1
    out = capsys.readouterr().out
    assert "unreachable" in out
