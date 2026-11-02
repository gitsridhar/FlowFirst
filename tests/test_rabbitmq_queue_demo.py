"""scripts/rabbitmq_queue_demo.py: target resolution, object naming, table rendering, CLI parsing. No broker needed."""
import importlib.util
import os
import re

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="module")
def demo():
    spec = importlib.util.spec_from_file_location("rabbitmq_queue_demo", os.path.join(ROOT, "scripts", "rabbitmq_queue_demo.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


TARGETS = {
    "node1": ("10.0.0.1", 5672),
    "node2": ("10.0.0.2", 5672),
    "node3": ("10.0.0.3", 5672),
}


def test_resolve_named_and_literal(demo):
    assert demo.resolve_target("node2", TARGETS) == ("node2", "10.0.0.2", 5672)
    assert demo.resolve_target("192.168.1.50:5671", TARGETS) == ("192.168.1.50:5671", "192.168.1.50", 5671)


def test_resolve_empty_is_error(demo):
    with pytest.raises(ValueError):
        demo.resolve_target("  ", TARGETS)
    with pytest.raises(ValueError):
        demo.resolve_targets(" , ", TARGETS)


def test_resolve_invalid_port(demo):
    with pytest.raises(ValueError, match="invalid port"):
        demo.resolve_target("10.0.0.1:abc", TARGETS)


def test_resolve_targets_drops_duplicate_hosts(demo):
    res = demo.resolve_targets("node1,node2,10.0.0.1:5672", TARGETS)
    assert res == [("node1", "10.0.0.1", 5672), ("node2", "10.0.0.2", 5672)]


def test_demo_objects_never_collide_with_pipeline_queues(demo):
    """The flowfirst-dlx policy (scripts/setup_rabbitmq_dlx.sh) matches ^flow[123]_; demo objects must stay outside it."""
    from src.config.config import ALL_QUEUES, QUEUE_DEAD_LETTERS

    names = list(demo.demo_names("abc12345").values()) + [demo.fanout_queue_name("abc12345", i) for i in (1, 2, 3)]
    for name in names:
        assert name.startswith(demo.DEMO_PREFIX + ".")
        assert not re.match(r"^flow[123]_", name)
        assert name not in ALL_QUEUES and name != QUEUE_DEAD_LETTERS


def test_fanout_queue_names_are_per_node_and_unique(demo):
    names = {demo.fanout_queue_name("r1", i) for i in (1, 2, 3)}
    assert len(names) == 3
    assert demo.fanout_queue_name("r1", 1).startswith(demo.demo_names("r1")["fanout_queue"])


def test_demo_queue_arguments_expire_and_follow_queue_type(demo):
    assert demo.demo_queue_arguments("classic") == {"x-expires": demo.QUEUE_EXPIRES_MS}
    assert demo.demo_queue_arguments("quorum") == {"x-queue-type": "quorum", "x-expires": demo.QUEUE_EXPIRES_MS}


def test_share_percent_and_missing_entries(demo):
    assert demo.share_percent(1, 4) == "25%"
    assert demo.share_percent(0, 0) == "0%"
    assert demo.missing_entries(["a", "b", "c"], ["b"]) == ["a", "c"]


def test_render_table_aligns_and_handles_none(demo):
    out = demo.render_table(
        [{"a": "node1", "b": None}, {"a": "node2", "b": "5.1"}],
        [("a", "Node"), ("b", "Connect")],
    )
    lines = out.splitlines()
    assert lines[0].startswith("Node ") and "Connect" in lines[0]
    assert len(lines) == 4
    assert "None" not in out


def test_every_run_stage_has_a_table_definition(demo):
    assert set(demo.RUN_ORDER) == set(demo.STAGES)
    for _title, func, cols in demo.STAGES.values():
        assert callable(func) and cols


def test_cli_parsing(demo):
    p = demo.build_parser()
    assert p.parse_args(["run", "--keep", "--queue-type", "quorum"]).queue_type == "quorum"
    assert p.parse_args(["work", "--count", "9"]).count == 9
    assert p.parse_args(["status", "--nodes", "node1,node2"]).nodes == "node1,node2"
    assert p.parse_args(["cleanup", "--run-id", "abc"]).func is demo.cmd_cleanup
    with pytest.raises(SystemExit):
        p.parse_args(["run", "--queue-type", "bogus"])
    with pytest.raises(SystemExit):
        p.parse_args(["cleanup"])        # --run-id is required


def test_main_bad_node_is_usage_error_not_traceback(demo, capsys):
    assert demo.main(["status", "--nodes", " "]) == 2
    assert "no nodes given" in capsys.readouterr().err


def test_main_negative_count_is_usage_error(demo, capsys):
    assert demo.main(["work", "--count", "-1"]) == 2
    assert "--count" in capsys.readouterr().err


def test_unreachable_broker_is_reported_not_raised(demo, capsys):
    """Nothing listens on port 1: status must print 'unreachable' and exit 1, never a traceback."""
    assert demo.main(["status", "--nodes", "127.0.0.1:1", "--timeout", "1"]) == 1
    out = capsys.readouterr().out
    assert "unreachable" in out
