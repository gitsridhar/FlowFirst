"""scripts/haproxy_lb_demo.py: node resolution, HAProxy stats parsing, balance check, table rendering, CLI parsing.
No HAProxy, cluster or network needed (the one network test uses a port nothing listens on)."""
import importlib.util
import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="module")
def demo():
    spec = importlib.util.spec_from_file_location("haproxy_lb_demo", os.path.join(ROOT, "scripts", "haproxy_lb_demo.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def named(demo):
    return {
        "mgmtnode1": demo.Node("mgmtnode1", "10.0.0.1", "mgmtnode1", 8082),
        "mgmtnode2": demo.Node("mgmtnode2", "10.0.0.2", "mgmtnode2", 8082),
        "mgmtnode3": demo.Node("mgmtnode3", "10.0.0.3", "mgmtnode3", 8082),
    }


# Shape of HAProxy's "show stat" / stats;csv output (only the columns the demo reads, in HAProxy's order).
STATS = (
    "# pxname,svname,qcur,qmax,scur,smax,slim,stot,bin,bout,dreq,dresp,ereq,econ,eresp,wretr,wredis,status,weight,act,bck,chkfail,chkdown,lastchg,downtime,qlimit,pid,iid,sid,throttle,lbtot,tracked,type,rate,rate_lim,rate_max,check_status\n"
    "flowfirst_api_front,FRONTEND,,,0,3,2000,40,0,0,0,0,0,,,,,OPEN,,,,,,,,,1,2,0,,,,0,0,0,2,\n"
    "flowfirst_api_back,mgmtnode1,0,0,0,1,,14,0,0,,0,,0,0,0,0,UP,1,1,0,0,0,300,0,,1,3,1,,14,,2,0,,1,L7OK\n"
    "flowfirst_api_back,mgmtnode2,0,0,0,1,,13,0,0,,0,,0,0,0,0,UP 1/2,1,1,0,0,0,300,0,,1,3,2,,13,,2,0,,1,L7OK\n"
    "flowfirst_api_back,mgmtnode3,0,0,0,0,,0,0,0,,0,,0,0,0,0,DOWN,1,1,0,3,1,40,40,,1,3,3,,0,,2,0,,0,L4CON\n"
    "flowfirst_api_back,BACKEND,0,0,0,2,200,27,0,0,0,0,,0,0,0,0,UP,2,2,0,,0,300,0,,1,3,0,,27,,1,0,,2,\n"
    "swift_back,mgmtnode1,0,0,0,0,,5,0,0,,0,,0,0,0,0,UP,1,1,0,0,0,300,0,,1,6,1,,5,,2,0,,1,L7OK\n"
)


def test_parse_stats_csv_reads_header_and_rows(demo):
    rows = demo.parse_stats_csv(STATS)
    assert rows[0]["pxname"] == "flowfirst_api_front"      # the leading "# " of the header is removed
    assert len(rows) == 6


def test_parse_stats_csv_empty(demo):
    assert demo.parse_stats_csv("") == []
    assert demo.parse_stats_csv("\n  \n") == []


def test_server_rows_skip_frontend_and_backend_summary(demo):
    rows = demo.parse_stats_csv(STATS)
    names = [r["svname"] for r in demo.server_rows(rows, "flowfirst_api_back")]
    assert names == ["mgmtnode1", "mgmtnode2", "mgmtnode3"]
    assert demo.server_rows(rows, "nope") == []


def test_sessions_by_server_and_delta(demo):
    rows = demo.parse_stats_csv(STATS)
    before = demo.sessions_by_server(rows, "flowfirst_api_back")
    assert before == {"mgmtnode1": 14, "mgmtnode2": 13, "mgmtnode3": 0}
    after = {"mgmtnode1": 18, "mgmtnode2": 17, "mgmtnode3": 0}
    assert demo.counter_delta(before, after) == {"mgmtnode1": 4, "mgmtnode2": 4, "mgmtnode3": 0}
    # a restarted HAProxy resets its counters: never report a negative number
    assert demo.counter_delta({"mgmtnode1": 50}, {"mgmtnode1": 2}) == {"mgmtnode1": 0}


def test_sessions_by_server_tolerates_garbage(demo):
    rows = [{"pxname": "b", "svname": "n1", "stot": "x"}, {"pxname": "b", "svname": "n2", "stot": ""}]
    assert demo.sessions_by_server(rows, "b") == {"n1": 0, "n2": 0}


def test_is_up_understands_haproxy_states(demo):
    assert demo.is_up("UP") and demo.is_up("UP 1/2") and demo.is_up(" up ")
    for s in ("DOWN", "DOWN 1/3", "NOLB", "MAINT", "", None):
        assert not demo.is_up(s)


def test_state_line(demo):
    rows = demo.parse_stats_csv(STATS)
    assert demo.state_line(rows, "flowfirst_api_back") == "mgmtnode1=UP mgmtnode2=UP mgmtnode3=DOWN"


def test_is_balanced(demo):
    assert demo.is_balanced({"mgmtnode1": 4, "mgmtnode2": 4, "mgmtnode3": 4}, 12)
    assert demo.is_balanced({"mgmtnode1": 5, "mgmtnode2": 4, "mgmtnode3": 3}, 12)
    assert not demo.is_balanced({"mgmtnode1": 12, "mgmtnode2": 0, "mgmtnode3": 0}, 12)      # one node got everything
    assert not demo.is_balanced({"mgmtnode1": 10, "mgmtnode2": 1, "mgmtnode3": 1}, 12)      # far from even
    assert not demo.is_balanced({}, 12) and not demo.is_balanced({"mgmtnode1": 1}, 0)


def test_is_balanced_two_nodes_after_a_failure(demo):
    assert demo.is_balanced({"mgmtnode1": 6, "mgmtnode2": 6}, 12)


def test_share_percent(demo):
    assert demo.share_percent(1, 4) == "25%" and demo.share_percent(0, 0) == "0%"


def test_resolve_nodes_named_and_literal(demo, named):
    res = demo.resolve_nodes("mgmtnode2", named)
    assert [n.host for n in res] == ["10.0.0.2"]
    lit = demo.resolve_nodes("192.168.1.9:8090", named)[0]
    assert (lit.host, lit.api_port) == ("192.168.1.9", 8090)


def test_resolve_nodes_drops_duplicates_and_rejects_bad_input(demo, named):
    assert len(demo.resolve_nodes("mgmtnode1,mgmtnode2,mgmtnode1", named)) == 2
    with pytest.raises(ValueError, match="no nodes"):
        demo.resolve_nodes(" , ", named)
    with pytest.raises(ValueError, match="invalid port"):
        demo.resolve_nodes("10.0.0.1:abc", named)


def test_named_nodes_use_configured_server_names(demo, monkeypatch):
    monkeypatch.setenv("NODE2_NAME", "galera-b")
    assert demo.named_nodes()["mgmtnode2"].name == "galera-b"      # must match the HAProxy 'server' line name


def test_describe_error_hides_message(demo):
    assert demo.describe_error(demo.HttpStatusError(503)) == "HTTP 503"
    assert demo.describe_error(OSError("connect to 10.1.1.1 failed")) == "OSError"


def test_render_table_aligns_and_handles_none(demo):
    out = demo.render_table([{"a": "mgmtnode1", "b": None}, {"a": "mgmtnode2", "b": "5"}], [("a", "Node"), ("b", "Count")])
    lines = out.splitlines()
    assert len(lines) == 4 and "None" not in out and lines[0].startswith("Node ")


def test_every_run_stage_is_defined_and_has_a_backend(demo):
    assert set(demo.RUN_ORDER) == set(demo.STAGES)
    for key in ("api", "mariadb", "rabbitmq", "swift"):
        assert key in demo.BACKENDS
    template = open(os.path.join(ROOT, "haproxy", "haproxy.cfg.template")).read()
    for backend in demo.BACKENDS.values():
        assert f"backend {backend}\n" in template, f"{backend} is not a backend in haproxy.cfg.template"


def test_cli_parsing(demo):
    p = demo.build_parser()
    a = p.parse_args(["run", "--requests", "30", "--nodes", "mgmtnode1,mgmtnode2", "--vip", "10.0.0.9"])
    assert (a.requests, a.nodes, a.vip) == (30, "mgmtnode1,mgmtnode2", "10.0.0.9")
    assert p.parse_args(["watch", "--seconds", "5"]).func is demo.cmd_watch
    assert p.parse_args(["api", "--api-port", "9999"]).api_port == 9999
    with pytest.raises(SystemExit):
        p.parse_args(["run", "--requests", "x"])


def test_main_rejects_bad_numbers_without_traceback(demo, capsys):
    assert demo.main(["api", "--requests", "0"]) == 2
    assert "--requests" in capsys.readouterr().err
    assert demo.main(["watch", "--seconds", "0"]) == 2
    assert "--seconds" in capsys.readouterr().err


def test_main_bad_node_is_usage_error(demo, capsys):
    assert demo.main(["status", "--nodes", " "]) == 2
    assert "no nodes given" in capsys.readouterr().err


def test_stats_stage_skips_cleanly_without_password(demo, monkeypatch, capsys):
    monkeypatch.delenv("HAPROXY_STATS_PASS", raising=False)
    assert demo.main(["backends"]) == 0
    assert "SKIP" in capsys.readouterr().out


def test_unreachable_vip_is_reported_not_raised(demo, monkeypatch, capsys):
    """Nothing listens on port 1: status must print CLOSED and exit 1, never a traceback."""
    monkeypatch.delenv("HAPROXY_STATS_PASS", raising=False)
    code = demo.main(["status", "--vip", "127.0.0.1", "--nodes", "127.0.0.1:1", "--timeout", "1",
                      "--api-port", "1", "--stats-port", "1", "--mariadb-port", "1", "--rabbitmq-port", "1", "--swift-port", "1"])
    out = capsys.readouterr().out
    assert code == 1 and "CLOSED" in out
