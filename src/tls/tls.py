"""
tls.py - TLS contexts for every FlowFirst connection (client side and the API Gateway (Process 1) HTTPS listener).

The certificates come from security/ (a private root CA). Each component has a switch in .env
(TLS_RABBITMQ_ENABLED, TLS_MARIADB_ENABLED, TLS_ZK_ENABLED, TLS_SWIFT_ENABLED, TLS_API_ENABLED), all off
by default. Rules applied everywhere:

  * The server certificate is always verified against TLS_CA_FILE, including its host name / IP.
  * The client presents the pipeline identity (TLS_CLIENT_CERT / TLS_CLIENT_KEY) so servers can require
    mutual TLS. Unreadable files raise TLSConfigError: a process never silently falls back to plaintext.
  * Minimum protocol TLS_MIN_VERSION (1.2 or 1.3); TLS 1.2 suites restricted to TLS_CIPHERS (AEAD, forward secrecy).
  * TLS_CRL_FILE (optional) makes the context reject revoked peer certificates (leaf check).

Importing this module never touches the network and does not need eventlet to be patched.
"""
import os
import ssl
from typing import Dict, Optional

from src.config import config as _cfg


class TLSConfigError(RuntimeError):
    """TLS is switched on but its configuration or files are unusable."""


def _need_file(path: str, what: str, env_name: str) -> str:
    if not path:
        raise TLSConfigError(f"{what}: {env_name} is empty.")
    if not os.path.isfile(path):
        raise TLSConfigError(f"{what}: file not found: {path} (set {env_name}; see security/install_certs.sh)")
    if not os.access(path, os.R_OK):
        raise TLSConfigError(f"{what}: file not readable by this user: {path}")
    return path


def _min_version() -> ssl.TLSVersion:
    v = _cfg.TLS_MIN_VERSION.strip()
    if v == "1.2":
        return ssl.TLSVersion.TLSv1_2
    if v == "1.3":
        return ssl.TLSVersion.TLSv1_3
    raise TLSConfigError(f"TLS_MIN_VERSION must be 1.2 or 1.3 (got '{v}')")


def _harden(ctx: ssl.SSLContext) -> ssl.SSLContext:
    ctx.minimum_version = _min_version()
    if _cfg.TLS_CIPHERS:
        try:
            ctx.set_ciphers(_cfg.TLS_CIPHERS)     # affects TLS 1.2 only; TLS 1.3 suites are always strong
        except ssl.SSLError as exc:
            raise TLSConfigError(f"TLS_CIPHERS '{_cfg.TLS_CIPHERS}' is not usable: {exc}")
    ctx.options |= ssl.OP_NO_COMPRESSION
    return ctx


def _apply_crl(ctx: ssl.SSLContext) -> None:
    if _cfg.TLS_CRL_FILE:
        ctx.load_verify_locations(cafile=_need_file(_cfg.TLS_CRL_FILE, "CRL", "TLS_CRL_FILE"))
        ctx.verify_flags |= ssl.VERIFY_CRL_CHECK_LEAF


def client_context(component: str = "") -> ssl.SSLContext:
    """Context for an outgoing connection: verifies the server, presents the pipeline client certificate."""
    what = f"TLS client ({component or 'generic'})"
    ca = _need_file(_cfg.TLS_CA_FILE, what + " CA", "TLS_CA_FILE")
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.check_hostname = True
    ctx.load_verify_locations(cafile=ca)
    ctx.load_cert_chain(
        certfile=_need_file(_cfg.TLS_CLIENT_CERT, what + " certificate", "TLS_CLIENT_CERT"),
        keyfile=_need_file(_cfg.TLS_CLIENT_KEY, what + " key", "TLS_CLIENT_KEY"),
    )
    _apply_crl(ctx)
    return _harden(ctx)


def server_context() -> ssl.SSLContext:
    """Context for the API Gateway (Process 1) HTTPS listener (TLS_API_CLIENT_AUTH: required | optional | none)."""
    mode = _cfg.TLS_API_CLIENT_AUTH
    if mode not in ("required", "optional", "none"):
        raise TLSConfigError(f"TLS_API_CLIENT_AUTH must be required, optional or none (got '{mode}')")
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(
        certfile=_need_file(_cfg.TLS_API_CERT, "TLS API certificate", "TLS_API_CERT"),
        keyfile=_need_file(_cfg.TLS_API_KEY, "TLS API key", "TLS_API_KEY"),
    )
    ctx.load_verify_locations(cafile=_need_file(_cfg.TLS_CA_FILE, "TLS API client CA", "TLS_CA_FILE"))
    ctx.verify_mode = {"required": ssl.CERT_REQUIRED, "optional": ssl.CERT_OPTIONAL, "none": ssl.CERT_NONE}[mode]
    _apply_crl(ctx)
    return _harden(ctx)


def mysql_ssl() -> Optional[ssl.SSLContext]:
    """Value for pymysql.connect(ssl=...); None when MariaDB TLS is off."""
    return client_context("mariadb") if _cfg.TLS_MARIADB_ENABLED else None


def zk_kwargs() -> Dict[str, object]:
    """Extra KazooClient keyword arguments; empty when ZooKeeper TLS is off."""
    if not _cfg.TLS_ZK_ENABLED:
        return {}
    return dict(
        use_ssl=True,
        ca=_need_file(_cfg.TLS_CA_FILE, "TLS ZooKeeper CA", "TLS_CA_FILE"),
        certfile=_need_file(_cfg.TLS_CLIENT_CERT, "TLS ZooKeeper certificate", "TLS_CLIENT_CERT"),
        keyfile=_need_file(_cfg.TLS_CLIENT_KEY, "TLS ZooKeeper key", "TLS_CLIENT_KEY"),
        verify_certs=True,
        check_hostname=True,
    )


def swift_kwargs() -> Dict[str, object]:
    """Extra swiftclient Connection keyword arguments; empty when Swift TLS is off."""
    if not _cfg.TLS_SWIFT_ENABLED:
        return {}
    if not _cfg.SWIFT_AUTH_URL.lower().startswith("https://"):
        raise TLSConfigError(f"TLS_SWIFT_ENABLED=true but SWIFT_AUTH_URL is not https:// ({_cfg.SWIFT_AUTH_URL}); "
                             "credentials would be sent in plaintext. Fix SWIFT_AUTH_URL in .env.")
    return dict(
        cacert=_need_file(_cfg.TLS_CA_FILE, "TLS Swift CA", "TLS_CA_FILE"),
        cert=_need_file(_cfg.TLS_CLIENT_CERT, "TLS Swift certificate", "TLS_CLIENT_CERT"),
        cert_key=_need_file(_cfg.TLS_CLIENT_KEY, "TLS Swift key", "TLS_CLIENT_KEY"),   # NOT 'key': that is the Swift password
        insecure=False,
    )


def describe() -> str:
    """One line for the startup log: which connections are protected."""
    c = _cfg
    on = [n for n, v in (("api", c.TLS_API_ENABLED), ("rabbitmq", c.TLS_RABBITMQ_ENABLED),
                         ("mariadb", c.TLS_MARIADB_ENABLED), ("zookeeper", c.TLS_ZK_ENABLED),
                         ("swift", c.TLS_SWIFT_ENABLED)) if v]
    return "TLS enabled for: " + (", ".join(on) if on else "nothing (plaintext)") + \
           f"  min=TLS{c.TLS_MIN_VERSION}  crl={'yes' if c.TLS_CRL_FILE else 'no'}"
