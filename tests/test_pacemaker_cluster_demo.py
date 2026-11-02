"""scripts/pacemaker_cluster_demo.py: crm_mon XML parsing, placement rules, preflight, systemd-vs-Pacemaker comparison,
polling, the two stages that change the cluster (against a fake cluster, restore path included), CLI parsing.
No Pacemaker, ssh or network needed (the one network test uses a port nothing listens on)."""
import importlib.util
import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="module")
def demo():
    spec = importlib.util.spec_from_file_location("pacemaker_cluster_demo", os.path.join(ROOT, "scripts", "pacemaker_cluster_demo.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _res(rid, nodes, failed="false"):
    active = "true" if nodes else "false"
    inner = "".join(f'<node name="{n}" id="{i}" cached="true"/>' for i, n in enumerate(nodes, 1))
    return f'<resource id="{rid}" resource_agent="x" role="Started" active="{active}" failed="{failed}">{inner}</resource>'


def make_xml(vip="node1", standby=(), offline=(), clones=None, quorum="true", failures=(), dc="node1", failed=()):
    """Shape of 'crm_mon -1 -r --output-as=xml' (only the attributes the demo reads)."""
    names = ["node1", "node2", "node3"]
    eligible = [n for n in names if n not in standby and n not in offline]
    nodes = "".join(
        f'<node name="{n}" id="{i}" online="{"false" if n in offline else "true"}" standby="{"true" if n in standby else "false"}" '
        f'maintenance="false" is_dc="{"true" if n == dc else "false"}"/>' for i, n in enumerate(names, 1))
    holders = [vip] if vip else []
    group = f'<group id="vip-haproxy-group">{_res("flowfirst-vip", holders)}{_res("haproxy-res", holders)}</group>'
    clones = clones or {}
    body = ""
    for n in (1, 2, 3, 4):
        rid = f"flowfirst-p{n}-res"
        body += f'<clone id="{rid}-clone">' + "".join(
            _res(rid, [h], "true" if rid in failed else "false") for h in clones.get(rid, eligible)) + "</clone>"
    fails = "".join(f'<failure op_key="{k}" node="node2"/>' for k in failures)
    return (f'<pacemaker-result api-version="2.29"><summary><current_dc present="true" name="{dc}" with_quorum="{quorum}"/></summary>'
            f"<nodes>{nodes}</nodes><resources>{group}{body}</resources><failures>{fails}</failures></pacemaker-result>")


def view_of(demo, **kw):
    return demo.parse_view(make_xml(**kw))


# ---------------------------------------------------------------- parsing

def test_parse_view_healthy_cluster(demo):
    v = view_of(demo)
    assert [n.name for n in v.nodes] == ["node1", "node2", "node3"]
    assert v.dc == "node1" and v.quorum is True and v.nodes[0].is_dc
    assert v.running["flowfirst-vip"] == ["node1"] and v.running["haproxy-res"] == ["node1"]
    assert v.running["flowfirst-p3-res"] == ["node1", "node2", "node3"]    # clone instances share one id and are merged
    assert v.failed == [] and v.failures == []


def test_parse_view_standby_failed_and_no_quorum(demo):
    v = view_of(demo, standby=("node3",), quorum="false", failures=("flowfirst-p2-res_monitor_30000",), failed=("flowfirst-p2-res",))
    assert v.nodes[2].standby and v.quorum is False
    assert v.failed == ["flowfirst-p2-res"] and v.failures == ["flowfirst-p2-res_monitor_30000@node2"]


def test_parse_view_rejects_garbage(demo):
    for text in ("", "not xml", "<other/>"):
        with pytest.raises(ValueError):
            demo.parse_view(text)


def test_parse_view_no_dc_means_no_quorum(demo):
    v = demo.parse_view('<pacemaker-result><summary><current_dc present="false"/></summary><nodes/><resources/></pacemaker-result>')
    assert v.dc == "" and v.quorum is False


# ---------------------------------------------------------------- placement rules

def test_eligible_nodes_skip_standby_and_offline(demo):
    assert demo.eligible_nodes(view_of(demo, standby=("node2",), offline=("node3",))) == ["node1"]


def test_vip_holder_and_expected_placement(demo):
    v = view_of(demo, vip="node2")
    assert demo.vip_holder(v) == "node2"
    assert demo.expected_placement(v, "haproxy-res") == ["node2"]
    assert demo.expected_placement(v, "flowfirst-p1-res") == ["node1", "node2", "node3"]
    assert demo.vip_holder(view_of(demo, vip=None)) is None


def test_vip_holder_none_when_on_two_nodes(demo):
    v = view_of(demo)._replace(running={"flowfirst-vip": ["node1", "node2"]})
    assert demo.vip_holder(v) is None


def test_placement_signature_changes_when_a_resource_moves(demo):
    assert demo.placement_signature(view_of(demo, vip="node1")) != demo.placement_signature(view_of(demo, vip="node2"))
    assert demo.placement_signature(view_of(demo)) == demo.placement_signature(view_of(demo))


# ---------------------------------------------------------------- preflight (guards the stages that change the cluster)

def test_preflight_passes_on_healthy_cluster(demo):
    assert demo.preflight(view_of(demo)) == []


@pytest.mark.parametrize("kw, text", [
    ({"quorum": "false"}, "no quorum"),
    ({"standby": ("node2",)}, "node2 is already in standby"),
    ({"offline": ("node3",)}, "node3 is offline"),
    ({"vip": None}, "VIP is not running on exactly one node"),
    ({"clones": {"flowfirst-p4-res": ["node1", "node2"]}}, "DB Updater (Process 4) is not running on every active node"),
    ({"failures": ("x_monitor_0",)}, "failed resources or failed actions"),
    ({"failed": ("flowfirst-p1-res",)}, "failed resources or failed actions"),
])
def test_preflight_refuses(demo, kw, text):
    assert any(text in p for p in demo.preflight(view_of(demo, **kw))), demo.preflight(view_of(demo, **kw))


# ---------------------------------------------------------------- node report vs Pacemaker

REPORT = ("active\nactive\nactive\nactive\nactive\ninactive\nactive\n---\n"
          "1: lo    inet 127.0.0.1/8 scope host lo\n2: ens3    inet 10.10.1.128/24 brd 10.10.1.255 scope global ens3\n"
          "2: ens3    inet 10.10.1.200/24 scope global secondary ens3\n")


def test_parse_node_report(demo):
    r = demo.parse_node_report(REPORT)
    assert r.units["corosync"] == "active" and r.units["flowfirst-process3"] == "inactive" and r.units["flowfirst-process4"] == "active"
    assert "10.10.1.200" in r.addresses and "127.0.0.1" in r.addresses


def test_parse_node_report_short_output(demo):
    r = demo.parse_node_report("active\n---\n")
    assert r.units == {"corosync": "active"} and r.addresses == set()


def test_service_mismatches(demo):
    v = view_of(demo, clones={"flowfirst-p3-res": ["node2", "node3"]})          # Pacemaker: p3 not on node1
    r = demo.parse_node_report(REPORT)                                         # systemd: p3 inactive on this node
    assert demo.service_mismatches(v, "node1", r) == []
    v2 = view_of(demo)                                                         # Pacemaker says p3 runs on node1 ...
    assert demo.service_mismatches(v2, "node1", r) == ["flowfirst-process3 is inactive but Pacemaker says it runs here"]
    v3 = view_of(demo, clones={"flowfirst-p4-res": ["node2", "node3"]})        # ... or says p4 must NOT run on node1
    assert demo.service_mismatches(v3, "node1", r)[-1] == "flowfirst-process4 is active but Pacemaker says it does not run here"


def test_service_mismatches_flags_a_stopped_cluster_daemon(demo):
    r = demo.parse_node_report(REPORT.replace("active\nactive\nactive", "active\ninactive\nactive", 1))
    assert "pacemaker is inactive" in demo.service_mismatches(view_of(demo, clones={"flowfirst-p3-res": ["node2", "node3"]}), "node1", r)


# ---------------------------------------------------------------- helpers

def test_wait_until_with_fake_clock(demo, monkeypatch):
    t = [0.0]
    monkeypatch.setattr(demo, "_now", lambda: t[0])
    monkeypatch.setattr(demo, "_sleep", lambda s: t.__setitem__(0, t[0] + s))
    assert demo.wait_until(lambda: t[0] >= 3, timeout=10) == (True, 3.0)
    ok, waited = demo.wait_until(lambda: False, timeout=5)
    assert ok is False and waited >= 5


def test_render_table_handles_none(demo):
    out = demo.render_table([{"a": "node1", "b": None}], [("a", "Node"), ("b", "State")])
    assert "None" not in out and out.splitlines()[0].startswith("Node")


def test_describe_error_hides_message(demo):
    assert demo.describe_error(demo.CmdError(1)) == "a cluster command failed (exit 1)"
    assert demo.describe_error(OSError("connect to 10.1.1.1 failed")) == "OSError"


def test_safe_rejects_option_injection_and_spaces(demo):
    for bad in ("-rf", "node1 node2", "a;b", "", "$(x)"):
        with pytest.raises(ValueError):
            demo._safe(bad, "x")
    assert demo._safe("node-1.example", "x") == "node-1.example"


def test_run_cmd_missing_binary_is_exit_127(demo):
    assert demo.run_cmd(["definitely-not-a-command-xyz"], 2) == (127, "")


def test_every_resource_id_exists_in_the_setup_script(demo):
    text = open(os.path.join(ROOT, "pacemaker", "configure_multinode_resources.sh")).read()
    for name in (demo.RES_VIP, demo.RES_HAPROXY, demo.GROUP):
        assert name in text
    for rid, clone, unit, _label in demo.PROCESSES.values():
        assert f"{rid} systemd:{unit}" in text and clone in text


# ---------------------------------------------------------------- a fake cluster for the stages that change it

class FakeCluster:
    """Stands in for Pacemaker: pcs calls change the state, get_view reports it; moves take `delay` polls."""

    def __init__(self, demo, delay=2):
        self.demo = demo
        self.standby, self.banned, self.vip, self.calls = set(), {}, "node1", []
        self.delay, self.ticks_since = delay, None
        self.fail_unstandby = False

    def view(self):
        eligible = [n for n in ("node1", "node2", "node3") if n not in self.standby]
        if self.ticks_since is not None:
            self.ticks_since += 1
            if self.ticks_since >= self.delay and self.vip in self.standby:
                self.vip = next(n for n in eligible)
        clones = {f"flowfirst-p{n}-res": [h for h in eligible if self.banned.get(f"flowfirst-p{n}-res-clone") != h] for n in (1, 2, 3, 4)}
        vip = self.vip if self.vip not in self.standby or self.ticks_since is None or self.ticks_since >= self.delay else None
        return self.demo.parse_view(make_xml(vip=vip, standby=tuple(self.standby), clones=clones))

    def pcs(self, st, *args):
        self.calls.append(args)
        if args[:2] == ("node", "standby"):
            self.standby.add(args[2])
            self.ticks_since = 0
        elif args[:2] == ("node", "unstandby"):
            if self.fail_unstandby:
                raise self.demo.CmdError(1)
            self.standby.discard(args[2])
        elif args[:2] == ("resource", "ban"):
            self.banned[args[2]] = args[3]
        elif args[:2] == ("resource", "clear"):
            self.banned.pop(args[2], None)


@pytest.fixture()
def fake(demo, monkeypatch):
    fc = FakeCluster(demo)
    t = [0.0]
    monkeypatch.setattr(demo, "_now", lambda: t[0])
    monkeypatch.setattr(demo, "_sleep", lambda s: t.__setitem__(0, t[0] + s))
    monkeypatch.setattr(demo, "get_view", lambda st: fc.view())
    monkeypatch.setattr(demo, "pcs", fc.pcs)
    monkeypatch.setattr(demo, "crm_cleanup", lambda st, r: fc.calls.append(("cleanup", r)))
    monkeypatch.setattr(demo, "probe_vip", lambda st: (fc.vip not in fc.standby and not (fc.ticks_since is not None and fc.ticks_since < 3), "x"))
    monkeypatch.setattr(demo, "say", lambda m: None)
    return fc


def settings(demo, *extra, cmd="failover"):
    args = demo.build_parser().parse_args([cmd, "--yes", "--wait", "30", *extra])
    return demo.Settings(args)


def test_failover_moves_the_vip_and_restores_the_node(demo, fake):
    ok, rows, _ = demo.stage_failover(settings(demo))
    text = " | ".join(r["result"] for r in rows)
    assert ok, text
    assert fake.standby == set()                                  # restored
    assert ("node", "standby", "node1") in fake.calls and ("node", "unstandby", "node1") in fake.calls
    assert any("node1 -> node2" in r["result"] for r in rows)
    assert rows[-1]["step"] == "pcs node unstandby node1"


def test_failover_restores_even_when_the_move_never_happens(demo, fake):
    fake.delay = 10 ** 6                                          # Pacemaker never moves the VIP
    ok, rows, _ = demo.stage_failover(settings(demo))
    assert not ok and fake.standby == set()
    assert any("not moved within 30 s" in r["result"] for r in rows)


def test_failover_reports_when_the_restore_itself_fails(demo, fake):
    fake.fail_unstandby = True
    ok, rows, _ = demo.stage_failover(settings(demo))
    assert not ok and "Run by hand NOW: sudo pcs node unstandby node1" in rows[-1]["result"]


def test_failover_refuses_on_an_unhealthy_cluster(demo, fake):
    fake.standby.add("node3")
    ok, rows, _ = demo.stage_failover(settings(demo))
    assert not ok and rows[0]["result"].startswith("REFUSED") and ("node", "standby", "node1") not in fake.calls


def test_ban_stops_one_instance_and_clears_the_ban(demo, fake, monkeypatch):
    monkeypatch.setattr(demo, "fetch_report", lambda st, nd: demo.NodeReport({"flowfirst-process2": "inactive"}, set()))
    ok, rows, _ = demo.stage_ban(settings(demo, "--process", "2", "--node", "node3", cmd="ban"))
    assert ok, rows
    assert fake.banned == {}                                      # cleared
    assert ("resource", "ban", "flowfirst-p2-res-clone", "node3") in fake.calls
    assert ("cleanup", "flowfirst-p2-res-clone") in fake.calls


def test_ban_default_target_is_not_the_vip_holder(demo, fake, monkeypatch):
    monkeypatch.setattr(demo, "fetch_report", lambda st, nd: demo.NodeReport({}, set()))
    demo.stage_ban(settings(demo, cmd="ban"))
    assert ("resource", "ban", "flowfirst-p1-res-clone", "node2") in fake.calls


def test_ban_refuses_an_unknown_node(demo, fake):
    ok, rows, _ = demo.stage_ban(settings(demo, "--node", "node9", cmd="ban"))
    assert not ok and "REFUSED" in rows[0]["result"] and fake.calls == []


# ---------------------------------------------------------------- CLI

def test_cli_parsing(demo):
    p = demo.build_parser()
    a = p.parse_args(["ban", "--yes", "--process", "3", "--node", "node2", "--wait", "10"])
    assert (a.yes, a.process, a.node, a.wait) == (True, 3, "node2", 10)
    assert p.parse_args(["watch", "--seconds", "5"]).func is demo.cmd_watch
    with pytest.raises(SystemExit):
        p.parse_args(["ban", "--process", "5"])
    assert p.parse_args(["failover"]).yes is False


def test_changing_stages_need_confirmation(demo, capsys):
    assert demo.main(["failover"]) == 2
    assert "--yes" in capsys.readouterr().err
    assert demo.main(["ban"]) == 2


def test_main_rejects_bad_numbers_and_names(demo, capsys):
    assert demo.main(["cluster", "--wait", "0"]) == 2
    assert demo.main(["watch", "--seconds", "0"]) == 2
    assert demo.main(["cluster", "--ssh-user=-oProxyCommand=x"]) == 2
    assert "not allowed" in capsys.readouterr().err


def test_missing_crm_mon_is_reported_not_raised(demo, monkeypatch, capsys):
    monkeypatch.setattr(demo, "run_cmd", lambda args, timeout: (127, ""))
    assert demo.main(["cluster"]) == 1
    out = capsys.readouterr().out
    assert "exit 127" in out and "Traceback" not in out


def test_probe_vip_unreachable_is_reported_not_raised(demo):
    ok, who = demo.probe_vip(demo.Settings(demo.build_parser().parse_args(
        ["vip", "--vip", "127.0.0.1", "--api-port", "1", "--timeout", "1"])))
    assert ok is False and who


def test_every_stage_is_defined(demo):
    assert set(demo.RUN_ORDER) == set(demo.STAGES)
