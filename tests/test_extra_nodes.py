"""EXTRA_NODES (ReadMe Scenario 76): the bash parser, the HAProxy rendering and the orchestrator's dry run.

The shell scripts are run as real subprocesses with a scrubbed environment; nothing is installed or contacted.
"""
import os
import shutil
import subprocess

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BASH = shutil.which("bash")
pytestmark = pytest.mark.skipif(BASH is None, reason="bash not available")

BASE_ENV = {
    "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
    "NODE1_IP": "10.0.0.1", "NODE2_IP": "10.0.0.2", "NODE3_IP": "10.0.0.3", "NODE4_IP": "10.0.0.4",
    "FLOWFIRST_VIP": "10.0.0.100",
    "HAPROXY_STATS_PASS": "test-placeholder-haproxy-pass-for-unit-tests",
}


def sh(script, env=None, args=()):
    full = dict(BASE_ENV)
    full.update(env or {})
    return subprocess.run([BASH, "-c", script, "bash", *args], cwd=ROOT, env=full,
                          capture_output=True, text=True, timeout=30)


def lib(snippet, extra=""):
    return sh('source scripts/extra_nodes_lib.sh; extra_nodes_load "$1" || exit 7; ' + snippet, args=(extra,))


def test_lib_blank_means_no_extra_nodes():
    r = lib('echo "$EXTRA_COUNT [$(extra_ips_csv)]"', "")
    assert r.stdout.strip() == "0 []"


def test_lib_parses_names_ips_and_ids():
    r = lib('echo "$EXTRA_COUNT $(extra_ips_csv) $(extra_node_id extramgmtnode1) $(extra_node_id extramgmtnode12) '
            '$(extra_name_for_ip 10.0.0.8)"', " extramgmtnode1:10.0.0.7 , extramgmtnode2:10.0.0.8 ")
    assert r.stdout.strip() == "2 10.0.0.7,10.0.0.8 4 15 extramgmtnode2"


@pytest.mark.parametrize("raw", [
    "node5:10.0.0.7", "extramgmtnode1", "extramgmtnode1:10.0.0.999", "extramgmtnode1:host.example.com",
    "extramgmtnode1:10.0.0.7,extramgmtnode1:10.0.0.8", "extramgmtnode1:10.0.0.7,extramgmtnode2:10.0.0.7",
    "extramgmtnode1:10.0.0.7;touch /tmp/pwned", "extramgmtnode1:10.0.0.7$(id)",
])
def test_lib_rejects_bad_entries_without_echoing_them(raw):
    r = lib("true", raw)
    assert r.returncode == 7
    assert "ERROR: EXTRA_NODES entry #" in r.stderr
    assert "pwned" not in r.stderr and "$(id)" not in r.stderr


def render(extra=""):
    return sh("HAPROXY_RENDER_ONLY=1 bash haproxy/setup_haproxy.sh", env={"EXTRA_NODES": extra})


def test_haproxy_default_output_has_no_extra_servers_and_no_placeholders():
    r = render("")
    assert r.returncode == 0, r.stderr
    assert "extramgmtnode" not in r.stdout
    assert "__EXTRA_" not in r.stdout
    assert r.stdout.count("    server node3 ") == 4          # one per backend, unchanged


def test_haproxy_adds_one_server_line_per_extra_node_to_every_backend():
    r = render("extramgmtnode1:10.0.0.7,extramgmtnode2:10.0.0.8")
    assert r.returncode == 0, r.stderr
    out = r.stdout
    assert "__EXTRA_" not in out
    for name, ip in (("extramgmtnode1", "10.0.0.7"), ("extramgmtnode2", "10.0.0.8")):
        assert f"    server {name} {ip}:8082 check\n" in out                       # API (Process 1)
        assert f"    server {name} {ip}:3306 check\n" in out                       # MariaDB
        assert f"    server {name} {ip}:5672 check inter 5s fall 2 rise 2\n" in out   # RabbitMQ
        assert f"    server {name} {ip}:8888 check\n" in out                       # Swift proxy


def test_haproxy_extra_nodes_get_verified_tls_when_enabled():
    env = {"EXTRA_NODES": "extramgmtnode1:10.0.0.7", "TLS_API_ENABLED": "true", "TLS_SWIFT_ENABLED": "true"}
    r = sh("HAPROXY_RENDER_ONLY=1 bash haproxy/setup_haproxy.sh", env=env)
    assert r.returncode == 0, r.stderr
    tls_lines = [ln for ln in r.stdout.splitlines()
                 if ln.startswith("    server extramgmtnode1 ") and (":8082 " in ln or ":8888 " in ln)]
    assert len(tls_lines) == 2
    for ln in tls_lines:
        assert "verify required" in ln and "verifyhost extramgmtnode1" in ln and "sni str(extramgmtnode1)" in ln


def test_haproxy_refuses_an_invalid_extra_nodes_value():
    r = render("extramgmtnode1:10.0.0.7;id")
    assert r.returncode != 0
    assert "EXTRA_NODES" in r.stderr


def test_orchestrator_plan_lists_every_step_and_changes_nothing():
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        env_file = os.path.join(ROOT, ".env")
        assert not os.path.exists(env_file), "tests must not run next to a real .env"
        try:
            with open(env_file, "w") as fh:
                fh.write("NODE1_IP=10.0.0.1\nNODE2_IP=10.0.0.2\nNODE3_IP=10.0.0.3\nNODE4_IP=10.0.0.4\n"
                         "EXTRA_NODES=extramgmtnode1:10.0.0.7\n")
            r = subprocess.run([BASH, "scripts/add_extra_node.sh", "extramgmtnode1", "--plan"], cwd=ROOT,
                               capture_output=True, text=True, timeout=30, env={"PATH": BASE_ENV["PATH"], "HOME": d})
            assert r.returncode == 0, r.stderr
            assert "ZooKeeper" in r.stdout and "Pacemaker" in r.stdout and "10. Verify" in r.stdout
            r = subprocess.run([BASH, "scripts/add_extra_node.sh", "extramgmtnode2", "--plan"], cwd=ROOT,
                               capture_output=True, text=True, timeout=30, env={"PATH": BASE_ENV["PATH"], "HOME": d})
            assert r.returncode != 0 and "not in EXTRA_NODES" in r.stderr
            r = subprocess.run([BASH, "scripts/add_extra_node.sh", "node4", "--plan"], cwd=ROOT,
                               capture_output=True, text=True, timeout=30, env={"PATH": BASE_ENV["PATH"], "HOME": d})
            assert r.returncode == 2          # node4 is the remote worker, never an extra node
        finally:
            os.remove(env_file)


@pytest.mark.parametrize("script", [
    "scripts/extra_nodes_lib.sh", "scripts/add_extra_node.sh", "scripts/join_rabbitmq_cluster.sh",
    "swift/add_ring_node.sh", "pacemaker/add_extra_node.sh",
])
def test_new_scripts_are_executable_and_parse(script):
    assert os.access(os.path.join(ROOT, script), os.X_OK)
    assert subprocess.run([BASH, "-n", script], cwd=ROOT).returncode == 0
