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
