import datetime
import ssl

import pytest

pytest.importorskip("cryptography")
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from src.config import config
from src.tls import tls


def _name(cn):
    return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])


def _write(path, cert, key):
    path.with_suffix(".crt").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    path.with_suffix(".key").write_bytes(key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))


@pytest.fixture(scope="module")
def pki(tmp_path_factory):
    """A throw-away CA and one leaf certificate, generated in memory-sized files under pytest's tmp dir."""
    d = tmp_path_factory.mktemp("pki")
    now = datetime.datetime.now(datetime.timezone.utc)
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca = (x509.CertificateBuilder().subject_name(_name("test-ca")).issuer_name(_name("test-ca"))
          .public_key(ca_key.public_key()).serial_number(1).not_valid_before(now - datetime.timedelta(days=1))
          .not_valid_after(now + datetime.timedelta(days=2))
          .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
          .sign(ca_key, hashes.SHA256()))
    key = ec.generate_private_key(ec.SECP256R1())
    leaf = (x509.CertificateBuilder().subject_name(_name("pipeline")).issuer_name(ca.subject)
            .public_key(key.public_key()).serial_number(2).not_valid_before(now - datetime.timedelta(days=1))
            .not_valid_after(now + datetime.timedelta(days=2)).sign(ca_key, hashes.SHA256()))
    _write(d / "ca", ca, ca_key)
    _write(d / "leaf", leaf, key)
    return d


@pytest.fixture
def tls_files(pki, monkeypatch):
    monkeypatch.setattr(config, "TLS_CA_FILE", str(pki / "ca.crt"))
    monkeypatch.setattr(config, "TLS_CLIENT_CERT", str(pki / "leaf.crt"))
    monkeypatch.setattr(config, "TLS_CLIENT_KEY", str(pki / "leaf.key"))
    monkeypatch.setattr(config, "TLS_API_CERT", str(pki / "leaf.crt"))
    monkeypatch.setattr(config, "TLS_API_KEY", str(pki / "leaf.key"))
    monkeypatch.setattr(config, "TLS_CRL_FILE", "")
    monkeypatch.setattr(config, "TLS_MIN_VERSION", "1.2")
    monkeypatch.setattr(config, "TLS_API_CLIENT_AUTH", "required")
    return pki


def test_client_context_verifies_server_and_hostname(tls_files):
    ctx = tls.client_context("test")
    assert ctx.verify_mode == ssl.CERT_REQUIRED
    assert ctx.check_hostname is True
    assert ctx.minimum_version == ssl.TLSVersion.TLSv1_2


def test_min_version_13(tls_files, monkeypatch):
    monkeypatch.setattr(config, "TLS_MIN_VERSION", "1.3")
    assert tls.client_context().minimum_version == ssl.TLSVersion.TLSv1_3


@pytest.mark.parametrize("bad", ["1.0", "1.1", "", "tls"])
def test_unsupported_min_version_is_refused(tls_files, monkeypatch, bad):
    monkeypatch.setattr(config, "TLS_MIN_VERSION", bad)
    with pytest.raises(tls.TLSConfigError):
        tls.client_context()


def test_missing_file_raises_instead_of_falling_back(tls_files, monkeypatch):
    monkeypatch.setattr(config, "TLS_CA_FILE", "/nonexistent/ca.crt")
    with pytest.raises(tls.TLSConfigError) as err:
        tls.client_context("x")
    assert "TLS_CA_FILE" in str(err.value)


def test_empty_path_raises(tls_files, monkeypatch):
    monkeypatch.setattr(config, "TLS_CLIENT_KEY", "")
    with pytest.raises(tls.TLSConfigError):
        tls.client_context()


@pytest.mark.parametrize("mode,expected", [("required", ssl.CERT_REQUIRED), ("optional", ssl.CERT_OPTIONAL),
                                           ("none", ssl.CERT_NONE)])
def test_server_context_client_auth_modes(tls_files, monkeypatch, mode, expected):
    monkeypatch.setattr(config, "TLS_API_CLIENT_AUTH", mode)
    assert tls.server_context().verify_mode == expected


def test_server_context_rejects_unknown_mode(tls_files, monkeypatch):
    monkeypatch.setattr(config, "TLS_API_CLIENT_AUTH", "maybe")
    with pytest.raises(tls.TLSConfigError):
        tls.server_context()


def test_bad_cipher_string_is_refused(tls_files, monkeypatch):
    monkeypatch.setattr(config, "TLS_CIPHERS", "NOT-A-CIPHER")
    with pytest.raises(tls.TLSConfigError):
        tls.client_context()


def test_crl_enables_leaf_check(tls_files, monkeypatch):
    # The CA file doubles as a loadable PEM; the point is that the flag is switched on.
    monkeypatch.setattr(config, "TLS_CRL_FILE", str(tls_files / "ca.crt"))
    assert tls.client_context().verify_flags & ssl.VERIFY_CRL_CHECK_LEAF


def test_off_switches_return_nothing(monkeypatch):
    monkeypatch.setattr(config, "TLS_MARIADB_ENABLED", False)
    monkeypatch.setattr(config, "TLS_ZK_ENABLED", False)
    monkeypatch.setattr(config, "TLS_SWIFT_ENABLED", False)
    assert tls.mysql_ssl() is None
    assert tls.zk_kwargs() == {}
    assert tls.swift_kwargs() == {}


def test_zk_kwargs_verify_certificates(tls_files, monkeypatch):
    monkeypatch.setattr(config, "TLS_ZK_ENABLED", True)
    kw = tls.zk_kwargs()
    assert kw["use_ssl"] is True and kw["verify_certs"] is True and kw["check_hostname"] is True


def test_swift_over_http_is_refused_when_tls_on(tls_files, monkeypatch):
    monkeypatch.setattr(config, "TLS_SWIFT_ENABLED", True)
    monkeypatch.setattr(config, "SWIFT_AUTH_URL", "http://vip:8090/auth/v1.0")
    with pytest.raises(tls.TLSConfigError):
        tls.swift_kwargs()


def test_swift_https_uses_cert_key_not_password(tls_files, monkeypatch):
    monkeypatch.setattr(config, "TLS_SWIFT_ENABLED", True)
    monkeypatch.setattr(config, "SWIFT_AUTH_URL", "https://vip:8090/auth/v1.0")
    kw = tls.swift_kwargs()
    assert "cert_key" in kw and "key" not in kw and kw["insecure"] is False


def test_describe_lists_enabled_components(monkeypatch):
    monkeypatch.setattr(config, "TLS_API_ENABLED", True)
    monkeypatch.setattr(config, "TLS_RABBITMQ_ENABLED", False)
    text = tls.describe()
    assert "api" in text and "rabbitmq" not in text
