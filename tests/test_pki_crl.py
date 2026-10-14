"""security/refresh_crl.sh and the CRL line of security/list_certs.sh, against a throw-away CA in a temp directory."""
import os
import shutil
import subprocess

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
pytestmark = pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl not installed")


@pytest.fixture(scope="module")
def pki(tmp_path_factory):
    d = tmp_path_factory.mktemp("pki")
    (d / "env").write_text("NODE1_IP=10.9.0.1\nNODE2_IP=10.9.0.2\nNODE3_IP=10.9.0.3\nNODE4_IP=10.9.0.4\nFLOWFIRST_VIP=10.9.0.9\n")
    env = dict(os.environ, FLOWFIRST_ENV_FILE=str(d / "env"), PKI_DIR=str(d / "pki"), PKI_CA_PASS="test-passphrase-123",
               PKI_CA_BITS="2048")
    for k in ("PKI_CA_PASSPHRASE_FILE", "PKI_CRL_DAYS"):
        env.pop(k, None)

    def sh(*args, extra=None):
        p = subprocess.run([os.path.join(ROOT, "security", args[0])] + list(args[1:]), capture_output=True, text=True,
                           env=dict(env, **(extra or {})), cwd=ROOT, timeout=120)
        return p.returncode, p.stdout + p.stderr

    assert sh("init_ca.sh")[0] == 0
    assert sh("issue_cert.sh", "--name", "c1", "--profile", "client", "--cn", "c1")[0] == 0
    return d, sh


def _next_update(d):
    out = subprocess.run(["openssl", "crl", "-in", str(d / "pki" / "ca" / "ca.crl"), "-noout", "-nextupdate"],
                         capture_output=True, text=True).stdout
    return out.strip().split("=", 1)[1]


def test_refresh_changes_next_update_and_keeps_revocations(pki):
    d, sh = pki
    assert sh("revoke_cert.sh", "c1")[0] == 0
    before = _next_update(d)
    code, out = sh("refresh_crl.sh", "--days", "90")
    assert code == 0, out
    assert _next_update(d) != before
    text = subprocess.run(["openssl", "crl", "-in", str(d / "pki" / "ca" / "ca.crl"), "-noout", "-text"],
                          capture_output=True, text=True).stdout
    assert "Serial Number" in text                      # the revocation survived the refresh


def test_refresh_rejects_bad_days(pki):
    _, sh = pki
    for bad in ("0", "-1", "abc"):
        code, out = sh("refresh_crl.sh", "--days", bad)
        assert code != 0 and "positive integer" in out


def test_wrong_passphrase_is_refused_and_ca_file_untouched(pki):
    d, sh = pki
    crl = d / "pki" / "ca" / "ca.crl"
    before = crl.read_bytes()
    code, out = sh("refresh_crl.sh", extra={"PKI_CA_PASS": "wrong-wrong-wrong"})
    assert code != 0 and "passphrase" in out.lower()
    assert crl.read_bytes() == before


def test_list_certs_reports_crl_and_warns_when_it_is_about_to_lapse(pki):
    d, sh = pki
    assert sh("refresh_crl.sh", "--days", "90")[0] == 0
    code, out = sh("list_certs.sh")
    assert "CRL: nextUpdate" in out and "EXPIRED" not in out.split("CRL:")[1] and "expires within" not in out.split("CRL:")[1]
    assert sh("refresh_crl.sh", "--days", "3")[0] == 0
    code, out = sh("list_certs.sh")
    assert code == 1 and "CRL:" in out and "expires within 7 days" in out
