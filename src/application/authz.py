"""Who is calling? - identity resolution and the admin allow-list for Process 1.

Pure functions (no sockets, no globals) so they can be unit-tested.

Identity sources, in order of trust:
  1. The client certificate presented on the TLS connection to Process 1 (verified by the TLS layer against the
     FlowFirst CA). Its subject CN is the identity.
  2. The X-SSL-Client-CN header, ONLY when the connection itself was made by a trusted proxy identity (the HAProxy
     backend certificate, i.e. one of ADMIN_TRUSTED_PROXY_CNS). HAProxy deletes any client-supplied copy of that header
     and sets it from the verified client certificate. A direct caller that holds some other valid certificate and sends
     the header gets its own certificate CN, not the claimed one.
No certificate and no trusted proxy -> identity None -> never authorised for an admin action.
"""
from typing import Iterable, Optional


def cert_common_name(peer_cert: Optional[dict]) -> Optional[str]:
    """Subject CN from ssl.SSLSocket.getpeercert() output, or None."""
    if not peer_cert:
        return None
    for rdn in peer_cert.get("subject", ()):
        for key, value in rdn:
            if key == "commonName" and value:
                return value
    return None


def resolve_identity(peer_cert: Optional[dict], header_cn: Optional[str],
                     trusted_proxy_cns: Iterable[str]) -> Optional[str]:
    """Return the caller's identity (a CN) or None when it cannot be established."""
    connection_cn = cert_common_name(peer_cert)
    if connection_cn is None:
        return None                                   # no verified certificate: never trust a header
    header_cn = (header_cn or "").strip()
    if header_cn and connection_cn in set(trusted_proxy_cns):
        return header_cn                              # forwarded by a trusted proxy that verified the real client
    return connection_cn


def parse_list(text: str) -> frozenset:
    """'a, b ,,c' -> frozenset({'a','b','c'})"""
    return frozenset(item.strip() for item in (text or "").split(",") if item.strip())


def admin_decision(identity: Optional[str], tls_enabled: bool, allowed_cns: frozenset,
                   allow_unauthenticated: bool) -> tuple:
    """Decide whether an admin action (changing runtime config) is permitted. Returns (allowed, reason)."""
    if not tls_enabled:
        # Without TLS there is no way to know who is calling. Fail closed unless the operator opted out explicitly.
        if allow_unauthenticated:
            return True, "unauthenticated admin allowed (ALLOW_UNAUTHENTICATED_ADMIN=true)"
        return False, "TLS is off, so the caller cannot be identified (set TLS_API_ENABLED=true, or ALLOW_UNAUTHENTICATED_ADMIN=true for a dev setup)"
    if identity is None:
        return False, "no verified client certificate"
    if identity in allowed_cns:
        return True, "identity is in ADMIN_ALLOWED_CNS"
    return False, "identity '%s' is not in ADMIN_ALLOWED_CNS" % identity
