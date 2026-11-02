"""scripts/zk_coordination_demo.py: target resolution, table rendering, 4LW parsing, and CLI parsing. No live ensemble needed."""
import importlib.util
import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="module")
def demo():
    spec = importlib.util.spec_from_file_location(
        "zk_coordination_demo", os.path.join(ROOT, "scripts", "zk_coordination_demo.py")
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


TARGETS = {
    "mgmtnode1": ("10.0.0.1", 2181),
    "mgmtnode2": ("10.0.0.2", 2181),
    "mgmtnode3": ("10.0.0.3", 2181),
}


def test_resolve_named_and_literal(demo):
    assert demo.resolve_target("mgmtnode2", TARGETS) == ("mgmtnode2", "10.0.0.2", 2181)
    assert demo.resolve_target("192.168.1.50:2281", TARGETS) == ("192.168.1.50:2281", "192.168.1.50", 2281)


def test_resolve_empty_is_error(demo):
    with pytest.raises(ValueError):
        demo.resolve_target("  ", TARGETS)
    with pytest.raises(ValueError):
        demo.resolve_targets(" , ", TARGETS)


def test_resolve_invalid_port(demo):
    with pytest.raises(ValueError, match="invalid port"):
        demo.resolve_target("10.0.0.1:abc", TARGETS)


def test_resolve_targets_drops_duplicate_hosts(demo):
    res = demo.resolve_targets("mgmtnode1,mgmtnode2,10.0.0.1:2181", TARGETS)
    assert res == [("mgmtnode1", "10.0.0.1", 2181), ("mgmtnode2", "10.0.0.2", 2181)]


def test_parse_mntr_response(demo):
    sample = """
    zk_version	3.9.2-e454e960fc832628411e210f01f5e679621b1821, built on 2024-03-14 10:05 UTC
    zk_avg_latency	0.1250
    zk_max_latency	15
    zk_min_latency	0
    zk_packets_received	1240
    zk_packets_sent	1242
    zk_num_alive_connections	3
    zk_outstanding_requests	0
    zk_server_state	leader
    zk_znode_count	24
    zk_synced_followers	2.0
    """
    parsed = demo.parse_mntr_response(sample)
    assert parsed["zk_server_state"] == "leader"
    assert parsed["zk_synced_followers"] == "2.0"
    assert parsed["zk_znode_count"] == "24"
    assert parsed["zk_avg_latency"] == "0.1250"


def test_missing_entries(demo):
    assert demo.missing_entries(["a", "b", "c"], ["b"]) == ["a", "c"]
    assert demo.missing_entries(["a"], ["a", "x"]) == []


def test_render_table_aligns_and_handles_none(demo):
    out = demo.render_table(
        [{"a": "leader", "b": None}, {"a": "follower", "b": "2.0"}],
        [("a", "Role"), ("b", "Synced Followers")],
    )
    lines = out.splitlines()
    assert lines[0].startswith("Role ") and "Synced Followers" in lines[0]
    assert len(lines) == 4
    assert len({len(line.rstrip()) for line in lines[:2]}) <= 2
    assert "None" not in out


def test_default_command_is_run(demo):
    p = demo.build_parser()
    assert p.parse_args(["run", "--keep"]).keep is True
    assert p.parse_args(["status", "--nodes", "mgmtnode1,mgmtnode2"]).func is demo.cmd_status
    assert p.parse_args(["election"]).func is demo.cmd_election
    assert p.parse_args(["config"]).func is demo.cmd_config
    assert p.parse_args(["registry"]).func is demo.cmd_registry
    assert p.parse_args(["dedup"]).func is demo.cmd_dedup


def test_main_bad_node_is_usage_error_not_traceback(demo, capsys):
    assert demo.main(["status", "--nodes", " "]) == 2
    err = capsys.readouterr().err
    assert "no nodes given" in err
