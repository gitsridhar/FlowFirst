"""scripts/db_replication_demo.py: target resolution, table rendering, argument handling. No database needed."""
import importlib.util
import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="module")
def demo():
    spec = importlib.util.spec_from_file_location("db_replication_demo", os.path.join(ROOT, "scripts", "db_replication_demo.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


TARGETS = {"mgmtnode1": "10.0.0.1", "mgmtnode2": "10.0.0.2", "mgmtnode3": "10.0.0.3", "vip": "10.0.0.100"}


def test_resolve_named_and_literal(demo):
    assert demo.resolve_target("mgmtnode2", TARGETS) == ("mgmtnode2", "10.0.0.2")
    assert demo.resolve_target(" vip ", TARGETS) == ("vip", "10.0.0.100")
    assert demo.resolve_target("192.168.5.5", TARGETS) == ("192.168.5.5", "192.168.5.5")


def test_resolve_empty_is_error(demo):
    with pytest.raises(ValueError):
        demo.resolve_target("  ", TARGETS)
    with pytest.raises(ValueError):
        demo.resolve_targets(" , ", TARGETS)


def test_resolve_targets_drops_duplicate_hosts(demo):
    assert demo.resolve_targets("mgmtnode1,mgmtnode2,10.0.0.1", TARGETS) == [("mgmtnode1", "10.0.0.1"), ("mgmtnode2", "10.0.0.2")]


def test_missing_entries(demo):
    assert demo.missing_entries(["a", "b", "c"], ["b"]) == ["a", "c"]
    assert demo.missing_entries(["a"], ["a", "x"]) == []


def test_render_table_aligns_and_handles_none(demo):
    out = demo.render_table([{"a": "xx", "b": None}, {"a": "y", "b": "long-value"}], [("a", "A"), ("b", "Header B")])
    lines = out.splitlines()
    assert lines[0].startswith("A ") and "Header B" in lines[0]
    assert len(lines) == 4
    assert len({len(line.rstrip()) for line in lines[:2]}) <= 2  # header + rule share column starts
    assert "None" not in out


def test_default_command_is_run(demo):
    p = demo.build_parser()
    assert p.parse_args(["run", "--keep"]).keep is True
    assert p.parse_args(["write", "--node", "mgmtnode2"]).func is demo.cmd_write
    assert p.parse_args(["read"]).node == "all"


def test_main_refuses_without_password(demo, monkeypatch, capsys):
    monkeypatch.setattr(demo, "MARIADB_PASSWORD", "")
    assert demo.main(["read", "--node", "mgmtnode1"]) == 2
    assert "MARIADB_PASSWORD" in capsys.readouterr().err


def test_main_bad_node_is_usage_error_not_traceback(demo, monkeypatch, capsys):
    monkeypatch.setattr(demo, "MARIADB_PASSWORD", "not-printed-anywhere-123")
    assert demo.main(["write", "--node", " "]) == 2
    err = capsys.readouterr().err
    assert "empty node name" in err and "not-printed-anywhere-123" not in err
