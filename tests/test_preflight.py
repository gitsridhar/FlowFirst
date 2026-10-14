"""scripts/preflight_check.py: right verdicts, right exit code, never prints a secret."""
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STRONG = {"RABBITMQ_PASSWORD": "a3f9c1d2e4b5a6978c0d1e2f3a4b5c6d", "MARIADB_PASSWORD": "b3f9c1d2e4b5a6978c0d1e2f3a4b5c6d",
          "SWIFT_KEY": "c3f9c1d2e4b5a6978c0d1e2f3a4b5c6d", "HAPROXY_STATS_PASS": "d3f9c1d2e4b5a6978c0d1e2f3a4b5c6d",
          "SWIFT_HASH_PATH_PREFIX": "e3f9c1d2e4b5a6978c0d1e2f3a4b5c6d", "SWIFT_HASH_PATH_SUFFIX": "f3f9c1d2e4b5a6978c0d1e2f3a4b5c6d",
          "NODE1_IP": "10.0.0.1", "NODE2_IP": "10.0.0.2", "NODE3_IP": "10.0.0.3", "NODE4_IP": "10.0.0.4",
          "NODE_NAME": "node1", "CURRENT_NODE_IP": "10.0.0.1"}


def run_check(tmp_path, overrides, mode=0o600):
    values = dict(STRONG, **overrides)
    env_file = tmp_path / "test.env"
    env_file.write_text("".join("%s=%s\n" % kv for kv in values.items()))
    os.chmod(env_file, mode)
    clean = {k: v for k, v in os.environ.items() if k not in values and not k.startswith(("TLS_", "API_", "ADMIN_", "METRICS_"))}
    clean.update(LOG_LEVEL="WARNING", LOG_DIR=str(tmp_path))
    p = subprocess.run([sys.executable, "-W", "ignore", os.path.join(ROOT, "scripts", "preflight_check.py"),
                        "--env", str(env_file)], capture_output=True, text=True, env=clean, cwd=ROOT, timeout=60)
    return p.returncode, p.stdout + p.stderr


def test_good_env_has_no_failures(tmp_path):
    code, out = run_check(tmp_path, {})
    assert code == 0, out
    assert " 0 FAIL" in out


def test_sample_password_fails_and_value_is_not_printed(tmp_path):
    code, out = run_check(tmp_path, {"RABBITMQ_PASSWORD": "flowpassword"})
    assert code == 1
    assert "RABBITMQ_PASSWORD is a well-known sample" in out
    assert "flowpassword" not in out


def test_strong_secrets_are_never_printed(tmp_path):
    _, out = run_check(tmp_path, {})
    assert not any(v in out for v in STRONG.values() if len(v) > 20)


def test_world_readable_env_fails(tmp_path):
    code, out = run_check(tmp_path, {}, mode=0o644)
    assert code == 1 and ".env permissions" in out


def test_unexpanded_reference_fails(tmp_path):
    code, out = run_check(tmp_path, {"MARIADB_PASSWORD": "${MARIADB_PASSWORD}"})
    assert code == 1 and "unexpanded" in out


def test_filled_host_list_fails(tmp_path):
    code, out = run_check(tmp_path, {"RABBITMQ_HOSTS": "1.2.3.4:5672"})
    assert code == 1 and "must be blank" in out


def test_unauthenticated_admin_fails(tmp_path):
    code, out = run_check(tmp_path, {"ALLOW_UNAUTHENTICATED_ADMIN": "true"})
    assert code == 1 and "ALLOW_UNAUTHENTICATED_ADMIN" in out


def test_error_details_fail(tmp_path):
    code, out = run_check(tmp_path, {"API_EXPOSE_ERROR_DETAILS": "true"})
    assert code == 1 and "API_EXPOSE_ERROR_DETAILS" in out


def test_missing_env_file_fails(tmp_path):
    p = subprocess.run([sys.executable, os.path.join(ROOT, "scripts", "preflight_check.py"), "--env", str(tmp_path / "nope")],
                       capture_output=True, text=True, cwd=ROOT, timeout=30)
    assert p.returncode == 1


def test_metrics_off_is_skipped(tmp_path):
    code, out = run_check(tmp_path, {})
    assert code == 0 and "SKIP" in out and "METRICS_ENABLED=false" in out


def test_metrics_open_to_the_network_warns(tmp_path):
    code, out = run_check(tmp_path, {"METRICS_ENABLED": "true"})
    assert code == 0 and "WARN" in out and "without a client certificate" in out


def test_metrics_bound_to_one_address_passes(tmp_path):
    code, out = run_check(tmp_path, {"METRICS_ENABLED": "true", "METRICS_HOST": "10.0.0.1"})
    assert code == 0 and "bound to 10.0.0.1" in out


def _tls_files(tmp_path, crl_days=None):
    """Throw-away CA files for the preflight TLS checks. crl_days=None -> no CRL file is written."""
    import datetime
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import NameOID
    now = datetime.datetime.now(datetime.timezone.utc)
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "ca")])
    ca = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key()).serial_number(1)
          .not_valid_before(now - datetime.timedelta(days=30)).not_valid_after(now + datetime.timedelta(days=300))
          .sign(key, hashes.SHA256()))
    pem = ca.public_bytes(serialization.Encoding.PEM)
    for fname in ("ca.crt", "pipeline.crt"):
        (tmp_path / fname).write_bytes(pem)
    (tmp_path / "pipeline.key").write_bytes(key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    os.chmod(tmp_path / "pipeline.key", 0o600)
    settings = {"TLS_API_ENABLED": "true", "TLS_CA_FILE": str(tmp_path / "ca.crt"),
                "TLS_CLIENT_CERT": str(tmp_path / "pipeline.crt"), "TLS_CLIENT_KEY": str(tmp_path / "pipeline.key")}
    if crl_days is not None:
        crl = (x509.CertificateRevocationListBuilder().issuer_name(name)
               .last_update(now - datetime.timedelta(days=40))
               .next_update(now + datetime.timedelta(days=crl_days)).sign(key, hashes.SHA256()))
        (tmp_path / "ca.crl").write_bytes(crl.public_bytes(serialization.Encoding.PEM))
        settings["TLS_CRL_FILE"] = str(tmp_path / "ca.crl")
    return settings


def test_crl_blank_warns(tmp_path):
    code, out = run_check(tmp_path, _tls_files(tmp_path))
    assert code == 0 and "TLS_CRL_FILE is blank" in out


def test_crl_valid_passes(tmp_path):
    code, out = run_check(tmp_path, _tls_files(tmp_path, crl_days=20))
    assert code == 0 and re.search(r"valid for (19|20) more day", out)


def test_crl_about_to_lapse_warns(tmp_path):
    code, out = run_check(tmp_path, _tls_files(tmp_path, crl_days=3))
    assert code == 0 and re.search(r"expires in (2|3) day", out)


def test_expired_crl_fails_the_check(tmp_path):
    code, out = run_check(tmp_path, _tls_files(tmp_path, crl_days=-2))
    assert code == 1 and "refresh_crl.sh" in out and "every certificate is refused" in out


def test_missing_crl_file_fails(tmp_path):
    settings = _tls_files(tmp_path)
    settings["TLS_CRL_FILE"] = str(tmp_path / "nope.crl")
    code, out = run_check(tmp_path, settings)
    assert code == 1 and "unreadable" in out
