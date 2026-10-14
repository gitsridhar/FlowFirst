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
