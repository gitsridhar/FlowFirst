import time
from src.application import authz

CERT = {"subject": ((("commonName", "alice"),),)}
PROXY_CERT = {"subject": ((("organizationName", "x"),), (("commonName", "node1-pipeline"),))}


def test_cert_common_name():
    assert authz.cert_common_name(CERT) == "alice"
    assert authz.cert_common_name(PROXY_CERT) == "node1-pipeline"
    assert authz.cert_common_name(None) is None
    assert authz.cert_common_name({}) is None
    assert authz.cert_common_name({"subject": ((("commonName", ""),),)}) is None


def test_parse_list():
    assert authz.parse_list("a, b ,,c") == frozenset({"a", "b", "c"})
    assert authz.parse_list("") == frozenset()
    assert authz.parse_list(None) == frozenset()


def test_header_ignored_without_certificate():
    assert authz.resolve_identity(None, "admin", ["node1-pipeline"]) is None


def test_header_ignored_from_untrusted_connection():
    # A client with its own valid certificate must not be able to claim another identity.
    assert authz.resolve_identity(CERT, "admin", ["node1-pipeline"]) == "alice"


def test_header_trusted_from_proxy_connection():
    assert authz.resolve_identity(PROXY_CERT, "alice", ["node1-pipeline"]) == "alice"
    assert authz.resolve_identity(PROXY_CERT, "  ", ["node1-pipeline"]) == "node1-pipeline"
    assert authz.resolve_identity(PROXY_CERT, None, ["node1-pipeline"]) == "node1-pipeline"


def test_admin_decision_without_tls_fails_closed():
    allowed, _ = authz.admin_decision("alice", False, frozenset({"alice"}), False)
    assert allowed is False


def test_admin_decision_dev_opt_out():
    assert authz.admin_decision(None, False, frozenset(), True)[0] is True


def test_admin_decision_with_tls():
    allow = frozenset({"alice"})
    assert authz.admin_decision("alice", True, allow, False)[0] is True
    assert authz.admin_decision("bob", True, allow, False)[0] is False
    assert authz.admin_decision(None, True, allow, False)[0] is False
    assert authz.admin_decision("alice", True, frozenset(), False)[0] is False   # blank list = nobody


def test_admin_decision_opt_out_does_not_weaken_tls():
    assert authz.admin_decision("bob", True, frozenset({"alice"}), True)[0] is False


# ---------------------------------------------------------------------------
# API-key tests
# ---------------------------------------------------------------------------

import hashlib

def _make_keys(pairs):
    """Build a parsed api_keys dict from [(label, raw_key), ...]."""
    raw = ",".join(f"{label}:{hashlib.sha256(k.encode()).hexdigest()}" for label, k in pairs)
    return authz.parse_api_keys(raw)


def test_parse_api_keys_valid():
    keys = _make_keys([("app1", "secret1"), ("app2", "secret2")])
    assert set(keys.keys()) == {"app1", "app2"}


def test_parse_api_keys_ignores_malformed():
    keys = authz.parse_api_keys("bad_no_colon, :tooshort, ,  ")
    assert keys == {}


def test_verify_api_key_correct():
    keys = _make_keys([("myapp", "correctkey")])
    assert authz.verify_api_key("correctkey", keys) == "myapp"


def test_verify_api_key_wrong():
    keys = _make_keys([("myapp", "correctkey")])
    assert authz.verify_api_key("wrongkey", keys) is None


def test_verify_api_key_empty():
    keys = _make_keys([("myapp", "correctkey")])
    assert authz.verify_api_key("", keys) is None
    assert authz.verify_api_key(None, keys) is None


def test_verify_api_key_no_keys():
    assert authz.verify_api_key("key", {}) is None


# ---------------------------------------------------------------------------
# HMAC token tests
# ---------------------------------------------------------------------------

SECRET = "a" * 32


def test_token_round_trip():
    token = authz.issue_token("alice", 60, SECRET)
    assert authz.verify_token(token, SECRET) == "alice"


def test_token_wrong_secret():
    token = authz.issue_token("alice", 60, SECRET)
    assert authz.verify_token(token, "b" * 32) is None


def test_token_expired():
    token = authz.issue_token("alice", -1, SECRET)   # already expired
    assert authz.verify_token(token, SECRET) is None


def test_token_tampered_payload():
    token = authz.issue_token("alice", 60, SECRET)
    payload, sig = token.split(".")
    # Change a byte in the payload
    tampered = payload[:-1] + ("A" if payload[-1] != "A" else "B")
    assert authz.verify_token(f"{tampered}.{sig}", SECRET) is None


def test_token_tampered_sig():
    token = authz.issue_token("alice", 60, SECRET)
    payload, sig = token.split(".")
    bad_sig = sig[:-1] + ("0" if sig[-1] != "0" else "1")
    assert authz.verify_token(f"{payload}.{bad_sig}", SECRET) is None


def test_token_missing_secret():
    import pytest
    with pytest.raises(ValueError):
        authz.issue_token("alice", 60, "")


def test_verify_token_no_secret():
    token = authz.issue_token("alice", 60, SECRET)
    assert authz.verify_token(token, "") is None


def test_verify_token_malformed():
    assert authz.verify_token("not.a.valid.token", SECRET) is None
    assert authz.verify_token("nodot", SECRET) is None


# ---------------------------------------------------------------------------
# check_request_auth tests
# ---------------------------------------------------------------------------

def _api_keys():
    return _make_keys([("svc", "rawkey123")])


def test_check_auth_mode_none():
    ok, label, _ = authz.check_request_auth("none", None, None, None, {}, "")
    assert ok is True
    assert label == "anonymous"


def test_check_auth_api_key_valid():
    ok, label, _ = authz.check_request_auth("api_key", "rawkey123", None, None, _api_keys(), "")
    assert ok is True and label == "svc"


def test_check_auth_api_key_wrong():
    ok, _, reason = authz.check_request_auth("api_key", "badkey", None, None, _api_keys(), "")
    assert ok is False and "invalid" in reason


def test_check_auth_api_key_missing():
    ok, _, reason = authz.check_request_auth("api_key", None, None, None, _api_keys(), "")
    assert ok is False and "missing" in reason


def test_check_auth_token_via_bearer():
    token = authz.issue_token("svc", 60, SECRET)
    ok, label, _ = authz.check_request_auth("token", None, token, None, _api_keys(), SECRET)
    assert ok is True and label == "svc"


def test_check_auth_token_also_accepts_api_key():
    """In token mode, X-Api-Key is also accepted (so clients don't need to implement the token flow)."""
    ok, label, _ = authz.check_request_auth("token", "rawkey123", None, None, _api_keys(), SECRET)
    assert ok is True and label == "svc"


def test_check_auth_token_expired():
    token = authz.issue_token("svc", -1, SECRET)
    ok, _, reason = authz.check_request_auth("token", None, token, None, _api_keys(), SECRET)
    assert ok is False and "expired" in reason


def test_check_auth_token_missing_headers():
    ok, _, reason = authz.check_request_auth("token", None, None, None, _api_keys(), SECRET)
    assert ok is False and "missing" in reason


def test_check_auth_cert_with_cert():
    ok, label, _ = authz.check_request_auth("cert", None, None, CERT, {}, "")
    assert ok is True and "alice" in label


def test_check_auth_cert_without_cert():
    ok, _, reason = authz.check_request_auth("cert", None, None, None, {}, "")
    assert ok is False and "certificate" in reason


def test_check_auth_unknown_mode():
    ok, _, reason = authz.check_request_auth("magic", None, None, None, {}, "")
    assert ok is False and "unknown" in reason


# ===========================================================================
# AUTHORIZATION ENGINE — RBAC / ACL / ABAC
# ===========================================================================

# ---------------------------------------------------------------------------
# parse_roles
# ---------------------------------------------------------------------------

def test_parse_roles_builtin_roles_always_present():
    roles, id_roles = authz.parse_roles("", "")
    assert "publisher" in roles
    assert "reader" in roles
    assert "admin" in roles
    assert "operator" in roles
    assert id_roles == {}


def test_parse_roles_custom_role():
    roles, _ = authz.parse_roles("ingest=flow:1:publish,flow:2:publish", "")
    assert "ingest" in roles
    assert "flow:1:publish" in roles["ingest"]
    assert "flow:2:publish" in roles["ingest"]


def test_parse_roles_identity_mapping():
    _, id_roles = authz.parse_roles("", "alice=publisher,reader;  bob=operator")
    assert id_roles["alice"] == frozenset({"publisher", "reader"})
    assert id_roles["bob"] == frozenset({"operator"})


def test_parse_roles_ignores_malformed():
    roles, id_roles = authz.parse_roles("=bad;no_equals", "=bad;no_equals")
    # built-ins still present; bad entries silently dropped
    assert "publisher" in roles
    assert id_roles == {}


# ---------------------------------------------------------------------------
# rbac_check
# ---------------------------------------------------------------------------

def _rbac_setup():
    return authz.parse_roles(
        "ingest=flow:1:publish,flow:2:publish",
        "alice=publisher; bob=ingest; carol=operator",
    )


def test_rbac_check_grants():
    roles, id_roles = _rbac_setup()
    assert authz.rbac_check("alice", "publish", "flow:1", roles, id_roles) is True
    assert authz.rbac_check("carol", "write", "zk:config", roles, id_roles) is True


def test_rbac_check_denies_wrong_action():
    roles, id_roles = _rbac_setup()
    assert authz.rbac_check("alice", "read", "flow:1", roles, id_roles) is False


def test_rbac_check_denies_missing_resource():
    # bob has the 'ingest' custom role which only grants flow:1 and flow:2
    roles, id_roles = _rbac_setup()
    assert authz.rbac_check("bob", "publish", "flow:3", roles, id_roles) is False


def test_rbac_check_no_role_returns_none():
    roles, id_roles = _rbac_setup()
    assert authz.rbac_check("unknown", "publish", "flow:1", roles, id_roles) is None


def test_rbac_check_glob_resource():
    roles, id_roles = authz.parse_roles(
        "batchpub=flow:*:publish", "dave=batchpub"
    )
    assert authz.rbac_check("dave", "publish", "flow:1", roles, id_roles) is True
    assert authz.rbac_check("dave", "publish", "flow:3", roles, id_roles) is True
    assert authz.rbac_check("dave", "read", "flow:1", roles, id_roles) is False


def test_rbac_check_no_identity():
    roles, id_roles = _rbac_setup()
    assert authz.rbac_check(None, "publish", "flow:1", roles, id_roles) is None


# ---------------------------------------------------------------------------
# parse_acl
# ---------------------------------------------------------------------------

def test_parse_acl_basic():
    acl = authz.parse_acl("flow:1:publish=ci_bot,data_ingest;  zk:config:write=admin-cert")
    assert acl[("flow:1", "publish")] == frozenset({"ci_bot", "data_ingest"})
    assert acl[("zk:config", "write")] == frozenset({"admin-cert"})


def test_parse_acl_ignores_bad_entries():
    acl = authz.parse_acl("no_equals;  =bad_key")
    assert acl == {}


def test_parse_acl_empty():
    assert authz.parse_acl("") == {}
    assert authz.parse_acl(None) == {}


# ---------------------------------------------------------------------------
# acl_check
# ---------------------------------------------------------------------------

def _acl():
    return authz.parse_acl("flow:1:publish=ci_bot,alice;  zk:config:write=admin-cert")


def test_acl_check_grants():
    assert authz.acl_check("alice", "publish", "flow:1", _acl()) is True
    assert authz.acl_check("admin-cert", "write", "zk:config", _acl()) is True


def test_acl_check_identity_not_in_list():
    assert authz.acl_check("bob", "publish", "flow:1", _acl()) is False


def test_acl_check_no_entry_returns_none():
    assert authz.acl_check("alice", "publish", "flow:3", _acl()) is None
    assert authz.acl_check(None, "publish", "flow:1", _acl()) is None


def test_acl_check_wildcard_resource():
    acl = authz.parse_acl("flow:*:publish=svc")
    assert authz.acl_check("svc", "publish", "flow:1", acl) is True
    assert authz.acl_check("svc", "publish", "flow:2", acl) is True
    assert authz.acl_check("other", "publish", "flow:1", acl) is False


# ---------------------------------------------------------------------------
# parse_policy / _rule_matches / abac_check
# ---------------------------------------------------------------------------

def test_parse_policy_valid():
    raw = '[{"when":{"identity":"bad_bot"},"effect":"deny"}]'
    rules = authz.parse_policy(raw)
    assert len(rules) == 1
    assert rules[0]["effect"] == "deny"


def test_parse_policy_empty_or_invalid():
    assert authz.parse_policy("") == []
    assert authz.parse_policy("not json") == []
    assert authz.parse_policy('{"not":"a list"}') == []


def test_rule_matches_identity():
    rule = {"when": {"identity": "bad_bot"}, "effect": "deny"}
    assert authz._rule_matches(rule, {"identity": "bad_bot", "action": "publish", "resource": "flow:1"})
    assert not authz._rule_matches(rule, {"identity": "alice", "action": "publish", "resource": "flow:1"})


def test_rule_matches_action():
    rule = {"when": {"action": "write"}, "effect": "deny"}
    assert authz._rule_matches(rule, {"identity": "x", "action": "write", "resource": "zk:config"})
    assert not authz._rule_matches(rule, {"identity": "x", "action": "read", "resource": "zk:config"})


def test_rule_matches_resource_glob():
    rule = {"when": {"resource": "flow:*"}, "effect": "allow"}
    assert authz._rule_matches(rule, {"identity": "x", "action": "publish", "resource": "flow:1"})
    assert not authz._rule_matches(rule, {"identity": "x", "action": "read", "resource": "zk:config"})


def test_rule_matches_role():
    rule = {"when": {"role": "publisher"}, "effect": "allow"}
    assert authz._rule_matches(rule, {"identity": "x", "action": "publish", "resource": "flow:1",
                                      "roles": frozenset({"publisher"})})
    assert not authz._rule_matches(rule, {"identity": "x", "action": "publish", "resource": "flow:1",
                                          "roles": frozenset({"reader"})})


def test_abac_check_deny_wins():
    policy = authz.parse_policy('[{"when":{"identity":"bad"},"effect":"deny"}]')
    assert authz.abac_check({"identity": "bad", "action": "publish", "resource": "flow:1"}, policy) is False


def test_abac_check_allow():
    policy = authz.parse_policy('[{"when":{"action":"read"},"effect":"allow"}]')
    assert authz.abac_check({"identity": "x", "action": "read", "resource": "health"}, policy) is True


def test_abac_check_no_match_returns_none():
    policy = authz.parse_policy('[{"when":{"identity":"x"},"effect":"allow"}]')
    assert authz.abac_check({"identity": "other", "action": "read", "resource": "health"}, policy) is None


# ---------------------------------------------------------------------------
# check_authz — combined engine
# ---------------------------------------------------------------------------

_ROLES, _ID_ROLES = authz.parse_roles(
    "",
    "alice=publisher; bob=reader; carol=operator",
)
_ACL = authz.parse_acl("flow:1:publish=ci_bot")
_POLICY = authz.parse_policy('[{"when":{"identity":"banned"},"effect":"deny"}]')


def test_check_authz_mode_none_always_allows():
    ok, _ = authz.check_authz("none", None, "publish", "flow:1", "POST",
                               _ROLES, _ID_ROLES, _ACL, _POLICY)
    assert ok is True


def test_check_authz_no_identity_denied():
    ok, reason = authz.check_authz("rbac", None, "publish", "flow:1", "POST",
                                   _ROLES, _ID_ROLES, _ACL, _POLICY)
    assert ok is False
    assert "identity" in reason


def test_check_authz_rbac_grants():
    ok, reason = authz.check_authz("rbac", "alice", "publish", "flow:1", "POST",
                                   _ROLES, _ID_ROLES, _ACL, _POLICY)
    assert ok is True
    assert "rbac" in reason


def test_check_authz_rbac_denies():
    ok, reason = authz.check_authz("rbac", "bob", "publish", "flow:1", "POST",
                                   _ROLES, _ID_ROLES, _ACL, _POLICY)
    assert ok is False


def test_check_authz_rbac_unknown_identity_denied():
    ok, _ = authz.check_authz("rbac", "unknown", "publish", "flow:1", "POST",
                              _ROLES, _ID_ROLES, _ACL, _POLICY)
    assert ok is False


def test_check_authz_acl_only_grants():
    ok, reason = authz.check_authz("acl", "ci_bot", "publish", "flow:1", "POST",
                                   _ROLES, _ID_ROLES, _ACL, _POLICY)
    assert ok is True
    assert "acl" in reason


def test_check_authz_acl_only_denies_unlisted():
    ok, _ = authz.check_authz("acl", "alice", "publish", "flow:1", "POST",
                              _ROLES, _ID_ROLES, _ACL, _POLICY)
    assert ok is False


def test_check_authz_acl_no_entry_denied():
    ok, _ = authz.check_authz("acl", "alice", "publish", "flow:3", "POST",
                              _ROLES, _ID_ROLES, _ACL, _POLICY)
    assert ok is False


def test_check_authz_rbac_plus_acl_acl_overrides_rbac_denial():
    # bob has reader role (no publish), but ci_bot is in ACL for flow:1:publish
    ok, reason = authz.check_authz("rbac+acl", "ci_bot", "publish", "flow:1", "POST",
                                   _ROLES, _ID_ROLES, _ACL, _POLICY)
    assert ok is True
    assert "acl" in reason


def test_check_authz_abac_deny_fires_first():
    ok, reason = authz.check_authz("rbac+acl+abac", "banned", "publish", "flow:1", "POST",
                                   _ROLES, _ID_ROLES, _ACL, _POLICY)
    assert ok is False
    assert "deny" in reason


def test_check_authz_rbac_plus_abac_allow_overrides():
    # dave has no roles, but ABAC allow rule grants publisher identity
    allow_policy = authz.parse_policy('[{"when":{"identity":"dave"},"effect":"allow"}]')
    ok, reason = authz.check_authz("rbac+abac", "dave", "publish", "flow:1", "POST",
                                   _ROLES, _ID_ROLES, _ACL, allow_policy)
    assert ok is True
    assert "abac" in reason


def test_check_authz_operator_has_all_access():
    for resource, action in [
        ("flow:1", "publish"), ("flow:2", "publish"), ("flow:3", "publish"),
        ("zk:config", "read"), ("zk:config", "write"),
        ("health", "read"), ("swift:objects", "list"),
    ]:
        ok, _ = authz.check_authz("rbac", "carol", action, resource, "POST",
                                  _ROLES, _ID_ROLES, {}, [])
        assert ok is True, f"operator should have {resource}:{action}"
