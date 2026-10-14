"""Prometheus metrics: text format, label mapping, certificate expiry, and the real listener (plain and TLS).
The listener tests run a child process because starting it calls gt.monkey_patch(), which must not happen in pytest."""
import datetime
import http.client
import os
import re
import socket
import ssl
import subprocess
import sys
import textwrap
import time

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from src.gt import gt
from src.metrics import metrics

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    gt.metrics_counter.clear()
    monkeypatch.setattr(metrics, "_gauges", {})
    yield
    gt.metrics_counter.clear()


def test_counters_become_labelled_series():
    gt.metrics_counter["flow1_published"] += 3
    gt.metrics_counter["flow2_failed"] += 1
    gt.metrics_counter["http_5xx"] += 2
    gt.metrics_counter["dedup_reaped"] += 7
    text = metrics.render("process1")
    assert 'flowfirst_messages_total{node="' in text
    assert re.search(r'flowfirst_messages_total\{[^}]*process="process1",flow="1",event="published"\} 3', text)
    assert re.search(r'flowfirst_messages_total\{[^}]*flow="2",event="failed"\} 1', text)
    assert re.search(r'flowfirst_http_responses_total\{[^}]*class="5xx"\} 2', text)
    assert re.search(r'flowfirst_events_total\{[^}]*event="dedup_reaped"\} 7', text)
    assert "# TYPE flowfirst_messages_total counter" in text
    assert re.search(r"flowfirst_up\{[^}]*\} 1", text)


def test_no_counters_means_no_empty_families():
    text = metrics.render("process3")
    assert "flowfirst_messages_total" not in text
    assert "flowfirst_up" in text


def test_every_sample_line_is_well_formed():
    gt.metrics_counter["flow1_published"] += 1
    metrics.register_gauge("flowfirst_x", "x", lambda: [({"cert": 'a"b'}, 1.5)])
    for line in metrics.render("process1").splitlines():
        if not line.startswith("#"):
            assert re.fullmatch(r'[a-z_]+\{[^{}]*\} -?[0-9.e+-]+', line), line
    assert 'cert="a\\"b"' in metrics.render("process1")


def test_a_failing_gauge_does_not_blank_the_scrape():
    def boom():
        raise RuntimeError("nope")
    metrics.register_gauge("flowfirst_bad", "bad", boom)
    metrics.register_gauge("flowfirst_good", "good", lambda: 4)
    text = metrics.render("process1")
    assert "flowfirst_bad" not in text
    assert re.search(r"flowfirst_good\{[^}]*\} 4", text)


def test_no_message_content_in_output():
    gt.metrics_counter["flow1_published"] += 1
    assert "payload" not in metrics.render("process1").lower()


def _make_cert(path, days):
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "t")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
            .serial_number(1).not_valid_before(now - datetime.timedelta(days=10))
            .not_valid_after(now + datetime.timedelta(days=days)).sign(key, hashes.SHA256()))
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


def test_cert_expiry_seconds(tmp_path, monkeypatch):
    from src.config import config
    ok, expired = tmp_path / "ok.crt", tmp_path / "old.crt"
    _make_cert(ok, 30)
    _make_cert(expired, -2)
    monkeypatch.setattr(config, "TLS_API_CERT", str(ok))
    monkeypatch.setattr(config, "TLS_CLIENT_CERT", str(expired))
    monkeypatch.setattr(config, "TLS_CA_FILE", str(tmp_path / "missing.crt"))   # absent file is skipped
    got = {labels["cert"]: value for labels, value in metrics._cert_expiry()}
    assert set(got) == {"api", "pipeline"}
    assert 29 * 86400 < got["api"] < 31 * 86400
    assert got["pipeline"] < 0


_CHILD = textwrap.dedent("""
    import os, sys
    sys.path.insert(0, %r)
    if sys.platform == "darwin":
        os.environ.setdefault("EVENTLET_HUB", "selects")
    from src.gt import gt
    gt.monkey_patch()
    from src.metrics import metrics
    import eventlet
    gt.metrics_counter["flow4_persisted"] += 5
    metrics.start("process4")
    print("READY", flush=True)
    eventlet.sleep(25)
""")


def _free_base():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port - 4          # process4 listens on base + 4


def _child(env_extra):
    env = dict(os.environ, ALLOW_DEFAULT_SECRETS="true", LOG_LEVEL="WARNING", LOG_DIR=os.devnull + "-none",
               METRICS_ENABLED="true", METRICS_HOST="127.0.0.1", PYTHONPATH=ROOT)
    env.update(env_extra)
    p = subprocess.Popen([sys.executable, "-W", "ignore", "-c", _CHILD % ROOT], stdout=subprocess.PIPE,
                         stderr=subprocess.DEVNULL, text=True, env=env, cwd=ROOT)
    for line in p.stdout:
        if line.strip() == "READY":
            break
    else:
        raise AssertionError("child did not report READY")
    time.sleep(1.0)
    return p


def _wait_get(port, ctx=None, path="/metrics", attempts=20):
    last = None
    for _ in range(attempts):
        try:
            if ctx:
                conn = http.client.HTTPSConnection("127.0.0.1", port, timeout=3, context=ctx)
            else:
                conn = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
            conn.request("GET", path)
            r = conn.getresponse()
            return r.status, r.getheader("Content-Type"), r.read().decode()
        except (ConnectionRefusedError, OSError) as e:
            last = e
            time.sleep(0.5)
    raise last


def test_listener_plain_http():
    base = _free_base()
    p = _child({"METRICS_PORT_BASE": str(base), "TLS_API_ENABLED": "false"})
    try:
        status, ctype, body = _wait_get(base + 4)
        assert status == 200 and ctype.startswith("text/plain; version=0.0.4")
        assert re.search(r'flowfirst_messages_total\{[^}]*process="process4",flow="4",event="persisted"\} 5', body)
        assert _wait_get(base + 4, path="/other")[0] == 404
    finally:
        p.kill()
        p.wait()


def test_listener_disabled_by_default_opens_nothing():
    base = _free_base()
    p = _child({"METRICS_PORT_BASE": str(base), "METRICS_ENABLED": "false"})
    try:
        with pytest.raises(OSError):
            socket.create_connection(("127.0.0.1", base + 4), timeout=2).close()
    finally:
        p.kill()
        p.wait()


def test_listener_tls_requires_client_certificate(tmp_path):
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-ca")])
    now = datetime.datetime.now(datetime.timezone.utc)

    def issue(cn, key, usage_server):
        b = (x509.CertificateBuilder().subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)]))
             .issuer_name(ca_name).public_key(key.public_key()).serial_number(x509.random_serial_number())
             .not_valid_before(now - datetime.timedelta(days=1)).not_valid_after(now + datetime.timedelta(days=5))
             .add_extension(x509.SubjectAlternativeName([x509.IPAddress(__import__("ipaddress").ip_address("127.0.0.1"))]),
                            critical=False))
        return b.sign(ca_key, hashes.SHA256())

    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name).public_key(ca_key.public_key())
          .serial_number(1).not_valid_before(now - datetime.timedelta(days=1))
          .not_valid_after(now + datetime.timedelta(days=5))
          .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True).sign(ca_key, hashes.SHA256()))
    files = {}
    for cn in ("api", "prom"):
        key = ec.generate_private_key(ec.SECP256R1())
        crt = issue(cn, key, cn == "api")
        files[cn] = (tmp_path / f"{cn}.crt", tmp_path / f"{cn}.key")
        files[cn][0].write_bytes(crt.public_bytes(serialization.Encoding.PEM))
        files[cn][1].write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                                   serialization.NoEncryption()))
    (tmp_path / "ca.crt").write_bytes(ca.public_bytes(serialization.Encoding.PEM))

    base = _free_base()
    p = _child({"METRICS_PORT_BASE": str(base), "TLS_API_ENABLED": "true", "TLS_API_CLIENT_AUTH": "required",
                "TLS_API_CERT": str(files["api"][0]), "TLS_API_KEY": str(files["api"][1]),
                "TLS_CA_FILE": str(tmp_path / "ca.crt"), "TLS_CLIENT_CERT": str(files["prom"][0])})
    try:
        good = ssl.create_default_context(cafile=str(tmp_path / "ca.crt"))
        good.load_cert_chain(str(files["prom"][0]), str(files["prom"][1]))
        status, _, body = _wait_get(base + 4, ctx=good)
        assert status == 200
        assert 'flowfirst_tls_cert_expiry_seconds{' in body and 'cert="api"' in body

        no_cert = ssl.create_default_context(cafile=str(tmp_path / "ca.crt"))
        with pytest.raises((ssl.SSLError, ConnectionError, OSError)):
            _wait_get(base + 4, ctx=no_cert, attempts=1)
        with pytest.raises(Exception):                      # plaintext client against a TLS port
            _wait_get(base + 4, attempts=1)
    finally:
        p.kill()
        p.wait()


def test_expected_counters_are_seeded_at_zero(monkeypatch):
    monkeypatch.setattr(metrics, "METRICS_ENABLED", True)
    monkeypatch.setattr(metrics.eventlet, "spawn", lambda *a, **k: None)      # do not open a port
    metrics.start("process4")
    text = metrics.render("process4")
    assert re.search(r'flow="1",event="persisted"\} 0', text)
    assert re.search(r'flow="3",event="failed"\} 0', text)
    assert re.search(r'event="dedup_reaped"\} 0', text)


def test_expected_counters_match_names_the_code_increments():
    """Every seeded name must be one a process really increments, so a rename cannot leave a dead series behind."""
    import glob
    source = "".join(open(p).read() for p in glob.glob(os.path.join(ROOT, "src", "application", "process*.py")))
    for names in metrics.EXPECTED_COUNTERS.values():
        for key in names:
            if key.startswith("http_"):
                continue                                      # built from the status code in _send_json
            assert f'"{key}"' in source, key
