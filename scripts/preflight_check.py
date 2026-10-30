#!/usr/bin/env python3
"""Read-only production-readiness check for ONE FlowFirst node. Changes nothing, prints no secret value.

    python3 scripts/preflight_check.py                  check ./.env  (or /opt/flowfirst/.env)
    python3 scripts/preflight_check.py --env /path/.env
    sudo python3 scripts/preflight_check.py             as root it can also read RabbitMQ policy / queue state

Each line is PASS, WARN, FAIL or SKIP. Exit code 1 if anything FAILed (WARN does not fail). It checks configuration and,
where the tools exist on this node, RabbitMQ state. It does NOT start anything and does NOT prove the cluster works:
use scripts/test_flows_and_inspect_db.sh and security/verify_tls.sh for that. Run it on every node.
"""
import argparse
import os
import shutil
import stat
import subprocess
import sys

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, ROOT)
RESULTS = []


def say(level, name, detail=""):
    RESULTS.append(level)
    print("%-5s %s%s" % (level, name, (": " + detail) if detail else ""))


def truthy(v):
    return str(v or "").strip().lower() in ("1", "true", "yes", "on")


def run(cmd, timeout=20):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, (p.stdout or "") + (p.stderr or "")
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 127, str(exc)


def find_env(explicit):
    for cand in ([explicit] if explicit else [os.path.join(ROOT, ".env"), "/opt/flowfirst/.env"]):
        if cand and os.path.isfile(cand):
            return os.path.abspath(cand)
    return None


def check_env_file(path, env):
    mode = stat.S_IMODE(os.stat(path).st_mode)
    if mode & 0o007:
        say("FAIL", ".env permissions", "%s is readable by everyone (mode %o); run: chmod 600 %s" % (path, mode, path))
    elif mode & 0o037:
        say("WARN", ".env permissions", "mode %o; 600 is recommended (it holds passwords)" % mode)
    else:
        say("PASS", ".env permissions", "mode %o" % mode)
    bad = [k for k, v in env.items() if v and "${" in v]
    say("FAIL" if bad else "PASS", ".env literal values", ("unexpanded ${...} in " + ", ".join(bad)) if bad else "no ${VAR} references")
    filled = [k for k in ("RABBITMQ_HOSTS", "MARIADB_HOSTS") if (env.get(k) or "").strip()]
    say("FAIL" if filled else "PASS", "host lists blank", (", ".join(filled) + " must be blank (built from NODE*_IP)") if filled else "RABBITMQ_HOSTS, MARIADB_HOSTS blank")
    ips = [env.get("NODE%d_IP" % i, "") for i in (1, 2, 3, 4)]
    say("FAIL" if (not all(ips) or "127.0.0.1" in ips) else "PASS", "node addresses", "NODE1..4_IP set" if all(ips) and "127.0.0.1" not in ips else "NODE*_IP missing or 127.0.0.1")
    for k in ("NODE_NAME", "CURRENT_NODE_IP"):
        say("PASS" if env.get(k) else "FAIL", k, "set" if env.get(k) else "missing (differs per node)")


def load_config(env):
    """Import config.py against the values from the file, without .env auto-loading and without its fail-closed raise."""
    import dotenv
    dotenv.load_dotenv = lambda *a, **k: False
    for k, v in env.items():
        os.environ[k] = v or ""
    os.environ["ALLOW_DEFAULT_SECRETS"] = "true"          # so import cannot raise; we judge the secrets ourselves
    from src.config import config
    return config


def check_secrets(env, cfg):
    names = ["RABBITMQ_PASSWORD", "MARIADB_PASSWORD", "HAPROXY_STATS_PASS"]
    if truthy(env.get("SWIFT_ENABLED", "true")):
        names += ["SWIFT_KEY", "SWIFT_HASH_PATH_PREFIX", "SWIFT_HASH_PATH_SUFFIX"]
    bad = []
    for n in names:
        why = cfg.weak_secret_reason(env.get(n, ""))
        if why:
            bad.append("%s %s" % (n, why))
    say("FAIL" if bad else "PASS", "secrets", "; ".join(bad) if bad else "%d secrets strong (length and known-sample check only)" % len(names))
    if truthy(env.get("ALLOW_DEFAULT_SECRETS")):
        say("FAIL", "ALLOW_DEFAULT_SECRETS", "true: development opt-out is on")
    else:
        say("PASS", "ALLOW_DEFAULT_SECRETS", "off")


def check_api(env, cfg):
    limits = {"API_MAX_BODY_BYTES": cfg.API_MAX_BODY_BYTES, "API_MAX_BATCH_COUNT": cfg.API_MAX_BATCH_COUNT,
              "API_MAX_LIST_LIMIT": cfg.API_MAX_LIST_LIMIT, "API_REQUEST_TIMEOUT_S": cfg.API_REQUEST_TIMEOUT_S}
    zero = [k for k, v in limits.items() if v <= 0]
    say("FAIL" if zero else "PASS", "request limits", ("not positive: " + ", ".join(zero)) if zero else ", ".join("%s=%s" % kv for kv in limits.items()))
    say("FAIL" if cfg.API_EXPOSE_ERROR_DETAILS else "PASS", "API_EXPOSE_ERROR_DETAILS", "true leaks exception text to clients" if cfg.API_EXPOSE_ERROR_DETAILS else "false")
    say("PASS", "API_PUBLISH_CONNECTIONS", "%d (range 1..32; see ReadMe K000 before changing)" % cfg.API_PUBLISH_CONNECTIONS)
    if cfg.API_PORT == cfg.API_BACKEND_PORT:
        say("FAIL", "ports", "API_PORT and API_BACKEND_PORT are both %d; HAProxy fronts 8080, API Gateway (Process 1) binds 8082" % cfg.API_PORT)
    else:
        say("PASS", "ports", "HAProxy %d -> API Gateway (Process 1) %d" % (cfg.API_PORT, cfg.API_BACKEND_PORT))


def check_admin(env, cfg):
    allowed = [x for x in (env.get("ADMIN_ALLOWED_CNS") or "").split(",") if x.strip()]
    if truthy(env.get("ALLOW_UNAUTHENTICATED_ADMIN")):
        say("FAIL", "admin endpoint", "ALLOW_UNAUTHENTICATED_ADMIN=true: anyone who can reach the API may change runtime config")
    elif not cfg.TLS_API_ENABLED:
        say("WARN", "admin endpoint", "TLS_API_ENABLED=false: POST /zk/config is refused for everyone (safe, but unusable)")
    elif not allowed:
        say("WARN", "admin endpoint", "ADMIN_ALLOWED_CNS is blank: nobody can change runtime config (safe, but unusable)")
    else:
        say("PASS", "admin endpoint", "restricted to: " + ", ".join(x.strip() for x in allowed))


def check_notifications(env, cfg):
    if not cfg.NOTIFICATIONS_ENABLED:
        say("SKIP", "notifications", "NOTIFICATIONS_ENABLED=false (opt-in; set true to activate)")
        return
    # Read the raw .env value so an explicit blank catches the misconfiguration even though
    # _str_env() falls back to the default "flowfirst.notifications".
    exchange = (env.get("NOTIFICATIONS_EXCHANGE") or "").strip()
    if not exchange:
        say("FAIL", "notifications", "NOTIFICATIONS_ENABLED=true but NOTIFICATIONS_EXCHANGE is blank")
    else:
        flows = (cfg.NOTIFICATIONS_FLOWS or "").strip()
        detail = "exchange=%s" % exchange
        if flows and flows != "*":
            detail += ", flows=%s" % flows
        else:
            detail += ", flows=all (1-7)"
        say("PASS", "notifications", detail)


def check_metrics(env, cfg):
    if not cfg.METRICS_ENABLED:
        say("SKIP", "metrics", "METRICS_ENABLED=false: no /metrics listener, nothing for Prometheus to scrape")
        return
    ports = "%d-%d" % (cfg.METRICS_PORT_BASE + 1, cfg.METRICS_PORT_BASE + 5)
    if cfg.TLS_API_ENABLED and cfg.TLS_API_CLIENT_AUTH == "required":
        say("PASS", "metrics", "ports %s need a client certificate (TLS_API_CLIENT_AUTH=required)" % ports)
    elif cfg.METRICS_HOST in ("0.0.0.0", "::", ""):
        say("WARN", "metrics", "ports %s answer anyone on the network without a client certificate: "
            "set METRICS_HOST to this node's IP and firewall them to Prometheus, or enable mutual TLS" % ports)
    else:
        say("PASS", "metrics", "ports %s bound to %s (no client certificate: restrict with the firewall)" % (ports, cfg.METRICS_HOST))


def check_tls(env, cfg):
    sw = {"api": cfg.TLS_API_ENABLED, "rabbitmq": cfg.TLS_RABBITMQ_ENABLED, "mariadb": cfg.TLS_MARIADB_ENABLED,
          "zookeeper": cfg.TLS_ZK_ENABLED, "swift": cfg.TLS_SWIFT_ENABLED}
    off = [k for k, v in sw.items() if not v]
    say("WARN" if off else "PASS", "TLS switches", ("OFF (plaintext): " + ", ".join(off)) if off else "all on")
    if any(sw.values()):
        missing = [p for p in (cfg.TLS_CA_FILE, cfg.TLS_CLIENT_CERT, cfg.TLS_CLIENT_KEY) if not os.path.isfile(p)]
        say("FAIL" if missing else "PASS", "TLS files", ("missing: " + ", ".join(missing)) if missing else "CA, client certificate and key present")
        key = cfg.TLS_CLIENT_KEY
        if os.path.isfile(key) and stat.S_IMODE(os.stat(key).st_mode) & 0o007:
            say("FAIL", "TLS key permissions", "%s is world-readable" % key)
        check_crl(cfg)


def check_crl(cfg):
    """A CRL past its nextUpdate makes every certificate unverifiable: the components that load it refuse all clients."""
    path = cfg.TLS_CRL_FILE
    if not path:
        say("WARN", "revocation list", "TLS_CRL_FILE is blank: revoked certificates are still accepted by API Gateway (Process 1) and the clients")
        return
    try:
        from cryptography import x509
        with open(path, "rb") as fh:
            crl = x509.load_pem_x509_crl(fh.read())
    except Exception as exc:
        say("FAIL", "revocation list", "%s unreadable: %s" % (path, type(exc).__name__))
        return
    import datetime
    left = crl.next_update_utc - datetime.datetime.now(datetime.timezone.utc)
    if left.total_seconds() <= 0:
        say("FAIL", "revocation list", "EXPIRED %d day(s) ago: every certificate is refused. Run security/refresh_crl.sh" % (-left.days))
    elif left.days < 7:
        say("WARN", "revocation list", "expires in %d day(s): run security/refresh_crl.sh and install_certs.sh --crl-only" % left.days)
    else:
        say("PASS", "revocation list", "valid for %d more day(s), %d revoked" % (left.days, len(list(crl))))


def check_sandbox():
    """Installed FlowFirst units on this node should carry the sandbox drop-in (systemd/flowfirst-hardening.conf)."""
    unit_dir = "/etc/systemd/system"
    if not os.path.isdir(unit_dir):
        say("SKIP", "service sandbox", "no systemd unit directory on this machine")
        return
    units = sorted(f for f in os.listdir(unit_dir) if f.startswith("flowfirst-process") and f.endswith(".service"))
    if not units:
        say("SKIP", "service sandbox", "no flowfirst-process*.service installed on this node")
        return
    bare = [u for u in units if not os.path.isfile(os.path.join(unit_dir, u + ".d", "10-hardening.conf"))]
    if bare:
        say("WARN", "service sandbox", "no hardening drop-in for: %s. Re-run: sudo ./systemd/install_services.sh <dir>" % ", ".join(bare))
    else:
        say("PASS", "service sandbox", "drop-in present for %d unit(s); score: systemd-analyze security <unit>" % len(units))


def check_swift_services():
    """Check that Swift systemd units are active on this node (nodes 1-3 only)."""
    # openstack-swift-* units are not installed on node4; skip silently when none are present.
    swift_units = [
        "openstack-swift-proxy",
        "openstack-swift-account",
        "openstack-swift-container",
        "openstack-swift-object",
        "memcached",
        "rsyncd",   # RHEL 9: the rsync-daemon package installs rsyncd.service (package name ≠ unit name)
    ]
    present = [u for u in swift_units if shutil.which("systemctl") and
               run(["systemctl", "cat", u + ".service"], timeout=5)[0] == 0]
    if not present:
        say("SKIP", "Swift services", "no openstack-swift units installed on this node (node4 or pre-install)")
        return
    inactive = []
    for u in present:
        rc, _ = run(["systemctl", "is-active", "--quiet", u], timeout=5)
        if rc != 0:
            inactive.append(u)
    if inactive:
        say("FAIL", "Swift services", "not active: %s — check: journalctl -u %s -n 50" % (
            ", ".join(inactive), inactive[0]))
    else:
        say("PASS", "Swift services", "%d unit(s) active" % len(present))


def check_backup_tools():
    """Tools the backup script needs on this node. Does not prove a backup exists or has been verified."""
    missing = [t for t in ("openssl", "gzip", "tar") if not shutil.which(t)]
    if missing:
        say("FAIL", "backup tools", "missing: " + ", ".join(missing))
        return
    if not (shutil.which("mariadb-dump") or shutil.which("mysqldump")):
        say("WARN", "backup tools", "no mariadb-dump / mysqldump on this node (needed on the node that runs the db backup)")
    else:
        say("PASS", "backup tools", "openssl, gzip, tar, dump client present; run scripts/backup_flowfirst.sh and --verify it (ReadMe: Backup and Restore)")


def check_rabbitmq():
    if not shutil.which("rabbitmqctl"):
        say("SKIP", "RabbitMQ policy / dead letters", "rabbitmqctl not on this node (run on nodes 1-3)")
        return
    rc, out = run(["rabbitmqctl", "list_policies", "-q", "--formatter", "tsv"])
    if rc != 0:
        say("SKIP", "RabbitMQ policy / dead letters", "cannot query RabbitMQ (run as root): " + out.strip().splitlines()[-1][:100] if out.strip() else "no output")
        return
    has = any("flowfirst-dlx" in line and "flowfirst.dlx" in line for line in out.splitlines())
    say("PASS" if has else "FAIL", "dead-letter policy", "flowfirst-dlx applied" if has else "missing: rejected messages are DISCARDED. Run: sudo ./scripts/setup_rabbitmq_dlx.sh")
    rc, out = run(["rabbitmqctl", "list_queues", "name", "messages", "-q", "--formatter", "tsv"])
    depth = None
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) == 2 and parts[0] == "flowfirst_dead_letters" and parts[1].isdigit():
            depth = int(parts[1])
    if depth is None:
        say("WARN", "dead-letter queue", "flowfirst_dead_letters does not exist yet (created when a process starts)")
    elif depth:
        say("WARN", "dead-letter queue", "%d message(s) waiting: python3 scripts/replay_dead_letters.py" % depth)
    else:
        say("PASS", "dead-letter queue", "empty")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--env", help="path to the .env to check")
    args = ap.parse_args()
    path = find_env(args.env)
    if not path:
        print("FAIL .env: not found (looked in the repo root and /opt/flowfirst)")
        return 1
    print("Checking %s on %s\n" % (path, os.uname().nodename))
    import dotenv
    env = {k: (v or "") for k, v in dotenv.dotenv_values(path, interpolate=False).items()}
    check_env_file(path, env)
    try:
        cfg = load_config(env)
    except Exception as exc:                                   # missing packages, bad number in .env, ...
        say("FAIL", "load configuration", "%s: %s" % (type(exc).__name__, str(exc)[:160]))
        cfg = None
    if cfg:
        check_secrets(env, cfg)
        check_api(env, cfg)
        check_admin(env, cfg)
        check_notifications(env, cfg)
        check_metrics(env, cfg)
        check_tls(env, cfg)
    check_rabbitmq()
    check_swift_services()
    check_backup_tools()
    check_sandbox()
    print("\n%d PASS, %d WARN, %d FAIL, %d SKIP" % tuple(RESULTS.count(x) for x in ("PASS", "WARN", "FAIL", "SKIP")))
    return 1 if "FAIL" in RESULTS else 0


if __name__ == "__main__":
    sys.exit(main())
