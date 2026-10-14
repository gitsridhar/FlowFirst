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

# ===========================================================================
# AUTHORIZATION ENGINE  (RBAC / ABAC / ACL)
# ===========================================================================
#
# Authorization runs AFTER authentication.  Authentication answers "who are
# you?"; authorization answers "what are you allowed to do?".
#
# Resources and actions used in FlowFirst:
#   resource            action(s)
#   flow:1              publish
#   flow:2              publish
#   flow:3              publish
#   flow:*              publish          (matches any flow:N)
#   zk:config           read, write
#   zk:health           read
#   swift:objects       list
#   swift:object        read
#   gt:status           read
#   health              read
#   ready               read
#
# AUTHZ_MODE (env): none (default) | rbac | acl | abac | rbac+acl | rbac+abac | rbac+acl+abac
#
# Evaluation order when multiple components are active:
#   1. ABAC deny rules fire first (an explicit deny beats everything).
#   2. RBAC is evaluated.
#   3. ACL is evaluated (AUTHZ_ACL_MODE=add: ACL can grant what RBAC denied).
#   4. ABAC allow rules fire last (can grant what RBAC+ACL denied).
#   5. Default: deny (fail closed).
#
# All logic is pure-Python, no I/O, no external dependency, fully unit-testable.
# ---------------------------------------------------------------------------

import fnmatch  # used for resource glob matching (flow:*)


# ---------------------------------------------------------------------------
# RBAC
# ---------------------------------------------------------------------------

# Built-in roles available without any configuration.
# Operators may define additional roles in AUTHZ_ROLES and AUTHZ_IDENTITY_ROLES.
_BUILTIN_ROLES: dict = {
    # publisher: may submit messages to all three flows (and batch).
    "publisher": frozenset({"flow:1:publish", "flow:2:publish", "flow:3:publish"}),
    # reader: may inspect health, status, ZooKeeper config and Swift listings.
    "reader": frozenset({
        "health:read", "ready:read", "gt:status:read",
        "zk:health:read", "zk:config:read",
        "swift:objects:list", "swift:object:read",
    }),
    # admin: may change ZooKeeper runtime config (and do everything reader can).
    "admin": frozenset({
        "health:read", "ready:read", "gt:status:read",
        "zk:health:read", "zk:config:read", "zk:config:write",
        "swift:objects:list", "swift:object:read",
    }),
    # operator: publisher + reader + admin (all-access built-in role).
    "operator": frozenset({
        "flow:1:publish", "flow:2:publish", "flow:3:publish",
        "health:read", "ready:read", "gt:status:read",
        "zk:health:read", "zk:config:read", "zk:config:write",
        "swift:objects:list", "swift:object:read",
    }),
}


def parse_roles(raw_roles: str, raw_identity_roles: str) -> tuple:
    """Parse AUTHZ_ROLES and AUTHZ_IDENTITY_ROLES into usable dicts.

    AUTHZ_ROLES format:
        rolename=action:resource,action:resource;  rolename2=...
    (Semicolon-separated role definitions; comma-separated permission tokens within each.)

    AUTHZ_IDENTITY_ROLES format:
        identity1=role1,role2;  identity2=role3

    Returns:
        roles: dict  role_name -> frozenset of "resource:action" permission tokens
        identity_roles: dict  identity -> frozenset of role names
    """
    roles: dict = dict(_BUILTIN_ROLES)
    for entry in (raw_roles or "").split(";"):
        entry = entry.strip()
        if "=" not in entry:
            continue
        name, perms_str = entry.split("=", 1)
        name = name.strip()
        if not name:
            continue
        # Each permission is stored as "resource:action" (normalised).
        # Input format: "action:resource" or "resource:action" — we normalise to "resource:action".
        perms = frozenset(p.strip() for p in perms_str.split(",") if p.strip())
        roles[name] = perms

    identity_roles: dict = {}
    for entry in (raw_identity_roles or "").split(";"):
        entry = entry.strip()
        if "=" not in entry:
            continue
        identity, role_str = entry.split("=", 1)
        identity = identity.strip()
        if not identity:
            continue
        identity_roles[identity] = frozenset(r.strip() for r in role_str.split(",") if r.strip())

    return roles, identity_roles


def rbac_check(identity: Optional[str], action: str, resource: str,
               roles: dict, identity_roles: dict) -> Optional[bool]:
    """Return True if RBAC grants access, False if explicitly checked but denied, None if no assignment.

    A wildcard resource (flow:*) in a permission token matches any flow:N resource.
    """
    if not identity:
        return None
    assigned_roles = identity_roles.get(identity, frozenset())
    if not assigned_roles:
        return None     # no role mapping for this identity — caller falls through to next check
    target = f"{resource}:{action}"
    for role_name in assigned_roles:
        perms = roles.get(role_name, frozenset())
        for perm in perms:
            # Support glob matching for the resource part (e.g. "flow:*:publish")
            if perm == target:
                return True
            parts = perm.rsplit(":", 1)
            if len(parts) == 2:
                res_pat, act = parts
                if act == action and fnmatch.fnmatchcase(resource, res_pat):
                    return True
    return False    # identity has roles but none grant this action


# ---------------------------------------------------------------------------
# ACL
# ---------------------------------------------------------------------------

def parse_acl(raw_acl: str) -> dict:
    """Parse AUTHZ_ACL into {(resource, action): frozenset of allowed identities}.

    Format:  resource:action=identity1,identity2;  resource2:action=identity3
    Example: flow:1:publish=ci_bot,data_ingest;  zk:config:write=admin-cert
    """
    acl: dict = {}
    for entry in (raw_acl or "").split(";"):
        entry = entry.strip()
        if "=" not in entry:
            continue
        key, identities_str = entry.split("=", 1)
        key = key.strip()
        if not key or key.count(":") < 1:
            continue
        # key is "resource:action"; resource itself may contain colons (e.g. "flow:1:publish")
        # so split from the RIGHT to get the last token as action.
        resource, action = key.rsplit(":", 1)
        resource, action = resource.strip(), action.strip()
        if resource and action:
            acl[(resource, action)] = frozenset(
                i.strip() for i in identities_str.split(",") if i.strip()
            )
    return acl


def acl_check(identity: Optional[str], action: str, resource: str, acl: dict) -> Optional[bool]:
    """Return True if ACL grants, False if explicitly listed but not present, None if no ACL entry.

    Wildcard entries like ('flow:*', 'publish') are matched with fnmatch.
    """
    if not identity:
        return None
    # Exact match first
    allowed = acl.get((resource, action))
    if allowed is not None:
        return identity in allowed
    # Wildcard scan
    for (res_pat, act), allowed in acl.items():
        if act == action and fnmatch.fnmatchcase(resource, res_pat):
            return identity in allowed
    return None     # no ACL entry for this resource:action


# ---------------------------------------------------------------------------
# ABAC (inline policy)
# ---------------------------------------------------------------------------

def parse_policy(raw_policy: str) -> list:
    """Parse AUTHZ_POLICY (JSON string) into a list of rule dicts.

    JSON format:
        [
          {"when": {"role": "publisher", "action": "publish"}, "effect": "allow"},
          {"when": {"identity": "bad_bot"},                    "effect": "deny"},
          {"when": {"action": "publish", "hour_utc_lt": 9},   "effect": "deny"},
          {"when": {"action": "publish", "hour_utc_gte": 18}, "effect": "deny"}
        ]

    Supported 'when' keys:
        identity            exact match against the caller's identity
        role                caller must have this role (requires AUTHZ_IDENTITY_ROLES)
        action              exact match
        resource            exact match (or fnmatch glob)
        method              HTTP method (GET, POST, …)
        hour_utc_lt         UTC hour < value  (0-23)
        hour_utc_gte        UTC hour >= value (0-23)
    """
    if not raw_policy or not raw_policy.strip():
        return []
    try:
        rules = json.loads(raw_policy)
        if not isinstance(rules, list):
            return []
        return [r for r in rules if isinstance(r, dict) and "effect" in r and "when" in r]
    except Exception:
        return []


def _rule_matches(rule: dict, attrs: dict) -> bool:
    """Return True if all conditions in rule['when'] match attrs."""
    for key, val in rule["when"].items():
        if key == "identity":
            if attrs.get("identity") != val:
                return False
        elif key == "role":
            if val not in attrs.get("roles", frozenset()):
                return False
        elif key == "action":
            if attrs.get("action") != val:
                return False
        elif key == "resource":
            res = attrs.get("resource", "")
            if not fnmatch.fnmatchcase(res, val) and res != val:
                return False
        elif key == "method":
            if attrs.get("method", "").upper() != val.upper():
                return False
        elif key == "hour_utc_lt":
            if int(time.gmtime().tm_hour) >= int(val):
                return False
        elif key == "hour_utc_gte":
            if int(time.gmtime().tm_hour) < int(val):
                return False
        # Unknown keys are ignored (forward-compatible)
    return True


def abac_check(attrs: dict, policy: list) -> Optional[bool]:
    """Evaluate ABAC policy rules in order.  Returns True (allow), False (deny), or None (no match).

    'attrs' must contain at minimum: identity, action, resource.
    Optional: method, roles (frozenset of role names).
    """
    for rule in policy:
        if _rule_matches(rule, attrs):
            return rule["effect"] == "allow"
    return None     # no rule matched


# ---------------------------------------------------------------------------
# Combined authorization check
# ---------------------------------------------------------------------------

def check_authz(
    authz_mode: str,            # AUTHZ_MODE from config
    identity: Optional[str],    # resolved from check_request_auth
    action: str,                # e.g. "publish", "read", "write", "list"
    resource: str,              # e.g. "flow:1", "zk:config", "swift:objects"
    method: str,                # HTTP method ("GET", "POST", …)
    roles: dict,                # from parse_roles()
    identity_roles: dict,       # from parse_roles()
    acl: dict,                  # from parse_acl()
    policy: list,               # from parse_policy()
) -> tuple:
    """Decide whether *identity* may perform *action* on *resource*.

    Returns (allowed: bool, reason: str).

    Evaluation order (each step can only be reached if the previous did not decide):
      1. AUTHZ_MODE=none → always allow (no-op, existing behaviour).
      2. ABAC deny rules (explicit deny beats everything).
      3. RBAC check (if rbac in mode).
      4. ACL check (if acl in mode; AUTHZ_ACL_MODE=add can grant what RBAC denied).
      5. ABAC allow rules (can grant what RBAC+ACL denied).
      6. Default: deny.
    """
    if not authz_mode or authz_mode == "none":
        return True, "authz disabled"

    if not identity:
        return False, "no authenticated identity for authorization check"

    parts = set(authz_mode.lower().split("+"))

    # Collect the caller's roles (needed for ABAC 'role' conditions too)
    caller_roles = identity_roles.get(identity, frozenset())
    attrs = {"identity": identity, "action": action, "resource": resource,
             "method": method, "roles": caller_roles}

    # 1. ABAC explicit deny — checked first regardless of mode combination.
    if "abac" in parts:
        deny_rules = [r for r in policy if r.get("effect") == "deny"]
        for rule in deny_rules:
            if _rule_matches(rule, attrs):
                return False, "abac: explicit deny rule matched"

    # 2. RBAC
    if "rbac" in parts:
        result = rbac_check(identity, action, resource, roles, identity_roles)
        if result is True:
            return True, "rbac: role grants %s:%s" % (resource, action)
        if result is False:
            # Identity has roles but none match. ACL (add mode) can still override.
            if "acl" in parts:
                acl_result = acl_check(identity, action, resource, acl)
                if acl_result is True:
                    return True, "acl: explicit grant overrides rbac denial"
            # ABAC allow can still override
            if "abac" in parts:
                allow_rules = [r for r in policy if r.get("effect") == "allow"]
                for rule in allow_rules:
                    if _rule_matches(rule, attrs):
                        return True, "abac: allow rule overrides rbac denial"
            return False, "rbac: identity has roles but none grant %s:%s" % (resource, action)
        # result is None → identity has no roles: fall through

    # 3. ACL (without RBAC, or RBAC returned None)
    if "acl" in parts:
        acl_result = acl_check(identity, action, resource, acl)
        if acl_result is True:
            return True, "acl: explicit grant for %s" % identity
        if acl_result is False:
            # ACL entry exists but identity is not in it
            if "abac" in parts:
                allow_rules = [r for r in policy if r.get("effect") == "allow"]
                for rule in allow_rules:
                    if _rule_matches(rule, attrs):
                        return True, "abac: allow rule overrides acl denial"
            return False, "acl: identity not in allow-list for %s:%s" % (resource, action)

    # 4. ABAC allow rules (last chance to grant)
    if "abac" in parts:
        result = abac_check(attrs, [r for r in policy if r.get("effect") == "allow"])
        if result is True:
            return True, "abac: allow rule matched"

    return False, "no authorization rule grants %s for %s:%s" % (identity, resource, action)
