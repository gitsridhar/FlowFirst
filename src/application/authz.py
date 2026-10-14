"""Who is calling? — identity resolution, admin gate, and pluggable API authentication for Process 1.

Pure functions (no sockets, no I/O, no globals) so they can be unit-tested.

Authentication modes (AUTH_MODE in .env):
  none      No authentication on flow endpoints (original behaviour, default). Admin endpoint still
            requires a certificate CN in ADMIN_ALLOWED_CNS when TLS is enabled.
  api_key   X-Api-Key header must match a key in AUTH_API_KEYS (stored as SHA-256 hex digests).
            Generate a key with: python3 -c "import secrets,hashlib; k=secrets.token_urlsafe(32);
            print(f'label:{hashlib.sha256(k.encode()).hexdigest()}  # raw key: {k}')"
  token     Authorization: Bearer <token> header. Tokens are issued by POST /auth/token
            (with a valid api_key) and expire after AUTH_TOKEN_TTL_S (default 3600 s).
            Tokens are HMAC-SHA256-signed with AUTH_TOKEN_SECRET. Both Bearer and X-Api-Key
            are accepted in token mode so callers can choose.
  cert      Client certificate from the TLS connection. Any valid certificate (signed by the
            FlowFirst CA) passes. Requires TLS_API_ENABLED=true. This is the same mechanism
            used for the admin gate but applied to every flow endpoint.

Identity sources for the admin gate (unchanged, in order of trust):
  1. The client certificate presented on the TLS connection to Process 1 (verified by the TLS layer).
     Its subject CN is the identity.
  2. The X-SSL-Client-CN header, ONLY when the connection was made by a trusted proxy CN from
     ADMIN_TRUSTED_PROXY_CNS (HAProxy's backend identity). HAProxy deletes any client-supplied copy.
  No certificate and no trusted proxy -> identity None -> never authorised for an admin action.
"""
import base64
import hashlib
import hmac
import json
import time
from typing import Iterable, Optional


# ---------------------------------------------------------------------------
# Existing certificate identity helpers (unchanged)
# ---------------------------------------------------------------------------

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
        if allow_unauthenticated:
            return True, "unauthenticated admin allowed (ALLOW_UNAUTHENTICATED_ADMIN=true)"
        return False, "TLS is off, so the caller cannot be identified (set TLS_API_ENABLED=true, or ALLOW_UNAUTHENTICATED_ADMIN=true for a dev setup)"
    if identity is None:
        return False, "no verified client certificate"
    if identity in allowed_cns:
        return True, "identity is in ADMIN_ALLOWED_CNS"
    return False, "identity '%s' is not in ADMIN_ALLOWED_CNS" % identity


# ---------------------------------------------------------------------------
# API-key helpers
# ---------------------------------------------------------------------------

def parse_api_keys(raw: str) -> dict:
    """Parse AUTH_API_KEYS into {label: sha256_hex}.

    Format: 'label1:sha256hex1, label2:sha256hex2, ...'
    A line without a colon is silently ignored (allows blank/comment entries).
    """
    result = {}
    for entry in (raw or "").split(","):
        entry = entry.strip()
        if ":" not in entry:
            continue
        label, digest = entry.split(":", 1)
        label, digest = label.strip(), digest.strip()
        if label and len(digest) == 64:           # SHA-256 hex = 64 chars
            result[label] = digest
    return result


def verify_api_key(raw_key: str, api_keys: dict) -> Optional[str]:
    """Return the label if raw_key matches any stored digest, else None.

    Uses constant-time comparison to prevent timing side-channels.
    """
    if not raw_key or not api_keys:
        return None
    candidate = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
    for label, digest in api_keys.items():
        if hmac.compare_digest(candidate, digest):
            return label
    return None


# ---------------------------------------------------------------------------
# HMAC token helpers
# ---------------------------------------------------------------------------

_TOKEN_VERSION = "ff1"


def _token_payload(subject: str, ttl_s: int) -> str:
    exp = int(time.time()) + ttl_s
    return base64.urlsafe_b64encode(
        json.dumps({"v": _TOKEN_VERSION, "sub": subject, "exp": exp}, separators=(",", ":")).encode()
    ).decode().rstrip("=")


def _token_sign(payload: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()  # type: ignore[attr-defined]


def issue_token(subject: str, ttl_s: int, secret: str) -> str:
    """Issue a signed bearer token for *subject* that expires in *ttl_s* seconds."""
    if not secret:
        raise ValueError("AUTH_TOKEN_SECRET is not set; cannot issue tokens")
    payload = _token_payload(subject, ttl_s)
    sig = _token_sign(payload, secret)
    return f"{payload}.{sig}"


def verify_token(token: str, secret: str) -> Optional[str]:
    """Return the subject if the token is valid and unexpired, else None."""
    if not token or not secret:
        return None
    parts = token.split(".")
    if len(parts) != 2:
        return None
    payload, sig = parts
    expected = _token_sign(payload, secret)
    if not hmac.compare_digest(expected, sig):
        return None
    # Decode payload
    try:
        pad = "=" * (-len(payload) % 4)
        data = json.loads(base64.urlsafe_b64decode(payload + pad).decode())
    except Exception:
        return None
    if data.get("v") != _TOKEN_VERSION:
        return None
    if int(data.get("exp", 0)) < int(time.time()):
        return None
    return data.get("sub") or None


# ---------------------------------------------------------------------------
# Unified flow-endpoint authentication
# ---------------------------------------------------------------------------

def check_request_auth(
    mode: str,
    raw_api_key: Optional[str],          # value of X-Api-Key header (or None)
    bearer_token: Optional[str],         # value of Authorization: Bearer ... (or None)
    peer_cert: Optional[dict],           # ssl.SSLSocket.getpeercert() result (or None)
    api_keys: dict,                      # label -> sha256_hex, from parse_api_keys()
    token_secret: str,                   # AUTH_TOKEN_SECRET
) -> tuple:
    """Check whether the request is authenticated for flow endpoints.

    Returns (ok: bool, label: str, reason: str).
    label is a human-readable identity for logging (e.g. key label, token subject, cert CN).
    """
    if mode == "none":
        return True, "anonymous", "auth disabled"

    if mode in ("api_key", "token"):
        # Try api_key first (works in both modes)
        if raw_api_key:
            label = verify_api_key(raw_api_key, api_keys)
            if label:
                return True, label, "valid api_key"
            return False, "", "invalid api_key"
        if mode == "token" and bearer_token:
            subject = verify_token(bearer_token, token_secret)
            if subject:
                return True, subject, "valid bearer token"
            return False, "", "invalid or expired token"
        if mode == "api_key":
            return False, "", "X-Api-Key header missing"
        return False, "", "X-Api-Key or Authorization: Bearer header missing"

    if mode == "cert":
        cn = cert_common_name(peer_cert)
        if cn:
            return True, cn, "valid client certificate (CN=%s)" % cn
        return False, "", "no valid client certificate (TLS_API_ENABLED must be true)"

    return False, "", "unknown AUTH_MODE '%s'" % mode
