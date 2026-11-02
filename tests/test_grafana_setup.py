"""Static checks for the Grafana install script, provisioning files and dashboards.

Nothing here starts Grafana: the dashboards are checked for structure, for the fixed datasource uid, and for the
metric names they use against the names FlowFirst really exposes and the job names prometheus.yml defines."""
import glob
import json
import os
import re
import subprocess

import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MON = os.path.join(ROOT, "monitoring")
GRAFANA = os.path.join(MON, "grafana")
DASHBOARDS = sorted(glob.glob(os.path.join(GRAFANA, "dashboards", "*.json")))
DS_UID = "flowfirst-prometheus"


def _text(*parts):
    with open(os.path.join(*parts)) as f:
        return f.read()


def _targets(dash):
    return [(p["title"], t) for p in dash["panels"] for t in p.get("targets", [])]


def test_two_dashboards_ship():
    names = sorted(os.path.basename(p) for p in DASHBOARDS)
    assert names == ["flowfirst-infrastructure.json", "flowfirst-pipeline.json"]


def test_dashboards_are_well_formed():
    uids = set()
    for path in DASHBOARDS:
        d = json.load(open(path))
        assert d["uid"] and d["uid"] not in uids
        uids.add(d["uid"])
        assert d["title"].startswith("FlowFirst")
        assert "id" not in d, "a provisioned dashboard must not carry a database id"
        ids = [p["id"] for p in d["panels"]]
        assert len(ids) == len(set(ids)), "duplicate panel ids in " + path
        for p in d["panels"]:
            g = p["gridPos"]
            assert 0 <= g["x"] and g["x"] + g["w"] <= 24, (path, p["title"])
        for var in d["templating"]["list"]:
            assert var["datasource"]["uid"] == DS_UID


def test_every_panel_uses_the_provisioned_datasource():
    for path in DASHBOARDS:
        d = json.load(open(path))
        for p in d["panels"]:
            if p["type"] == "row":
                continue
            assert p["datasource"]["uid"] == DS_UID, (path, p["title"])
            assert p["targets"], (path, p["title"])
            for t in p["targets"]:
                assert t["datasource"]["uid"] == DS_UID
                assert t["expr"].strip()


def test_datasource_provisioning_matches_the_dashboards():
    ds = yaml.safe_load(_text(GRAFANA, "provisioning", "datasources", "flowfirst.yml"))["datasources"][0]
    assert ds["uid"] == DS_UID and ds["type"] == "prometheus" and ds["access"] == "proxy"
    assert ds["url"] == "__PROMETHEUS_URL__", "setup_grafana.sh substitutes this placeholder"
    assert ds["editable"] is False


def test_dashboard_provider_points_at_the_directory_the_script_fills():
    prov = yaml.safe_load(_text(GRAFANA, "provisioning", "dashboards", "flowfirst.yml"))["providers"][0]
    assert prov["options"]["path"] == "/var/lib/grafana/dashboards/flowfirst"
    assert "/var/lib/grafana/dashboards/flowfirst" in _text(MON, "setup_grafana.sh")
    assert prov["allowUiUpdates"] is False


def test_flowfirst_series_used_by_dashboards_are_ones_the_metrics_module_exports():
    # Series come from metrics.py and from gauges a process registers itself (process1.py).
    sources = (("src", "metrics", "metrics.py"), ("src", "application", "process1.py"))
    exported = set()
    for src in sources:
        exported |= set(re.findall(r'"(flowfirst_[a-z_]+)"', _text(ROOT, *src)))
    used = set()
    for path in DASHBOARDS:
        for _, t in _targets(json.load(open(path))):
            expr = re.sub(r'"[^"]*"', '""', t["expr"])      # drop label values such as queue="flowfirst_dead_letters"
            used |= set(re.findall(r"\bflowfirst_[a-z_]+", expr))
    assert used, "the pipeline dashboard must query FlowFirst series"
    assert used <= exported, "unknown series: %s" % sorted(used - exported)


def test_flowfirst_label_values_used_by_dashboards_exist():
    from src.metrics import metrics
    counters = [k for names in metrics.EXPECTED_COUNTERS.values() for k in names]
    events = {m.group(1) for m in (re.match(r"^flow\d+_(.+)$", k) for k in counters) if m}
    processes = set(metrics.EXPECTED_COUNTERS)
    for path in DASHBOARDS:
        for title, t in _targets(json.load(open(path))):
            for ev in re.findall(r'event(?:!=|=~|=)"([a-z_]+)"', t["expr"]):
                assert ev in events, (title, ev)
            for pr in re.findall(r'process="([a-z_]+)"', t["expr"]):
                assert pr in processes, (title, pr)


def test_job_labels_used_by_dashboards_exist_in_prometheus_yml():
    jobs = {sc["job_name"] for sc in yaml.safe_load(_text(MON, "prometheus.yml"))["scrape_configs"]}
    for path in DASHBOARDS:
        for title, t in _targets(json.load(open(path))):
            for job in re.findall(r'job="([a-z_]+)"', t["expr"]):
                assert job in jobs, (title, job)


def test_no_dashboard_embeds_credentials_or_hosts():
    for path in DASHBOARDS:
        text = open(path).read().lower()
        for bad in ("password", "secret", "api_key", "http://", "https://"):
            assert bad not in text, (path, bad)


def test_setup_grafana_script_core_elements():
    path = os.path.join(MON, "setup_grafana.sh")
    text = _text(MON, "setup_grafana.sh")
    assert os.access(path, os.X_OK)
    assert subprocess.run(["bash", "-n", path]).returncode == 0
    assert "https://rpm.grafana.com" in text and "gpgcheck=1" in text and "repo_gpgcheck=1" in text
    assert "sslverify=1" in text
    assert "systemctl enable grafana-server" in text
    assert "grafana-server.service.d/10-hardening.conf" in text
    assert 'source "${REPO_DIR}/scripts/secrets_lib.sh"' in text
    assert "require_strong_secret GRAFANA_ADMIN_PASSWORD" in text


def test_setup_grafana_fails_closed_and_keeps_secrets_off_the_command_line():
    text = _text(MON, "setup_grafana.sh")
    # Binding every interface is refused.
    assert '""|0.0.0.0|::|"[::]") die' in text
    # TLS on with a missing file stops the installer; there is no fallback and no verification switch.
    assert "There is no plain-text fallback" in text
    assert "min_tls_version = TLS1.2" in text
    assert "--insecure" not in text and "curl -k" not in text and "verify=False" not in text
    # Secrets are read by Grafana from files, never written into grafana.ini or passed as arguments.
    plain = text.replace("\\$", "$")
    assert "$__file{/etc/grafana/secrets/admin_password}" in plain
    assert "$__file{/etc/grafana/secrets/secret_key}" in plain
    assert "chmod 640 /etc/grafana/secrets/admin_password" in text
    assert re.search(r"echo[^\n]*\$\{?GRAFANA_ADMIN_PASSWORD", text) is None
    # Hardening defaults of the generated grafana.ini.
    for line in ("allow_sign_up = false", "[auth.anonymous]\nenabled = false", "reporting_enabled = false",
                 "allow_embedding = false", "disable_gravatar = true"):
        assert line in text


def test_setup_grafana_never_replaces_an_existing_encryption_key():
    assert "if [[ ! -s /etc/grafana/secrets/secret_key ]]" in _text(MON, "setup_grafana.sh")


def test_env_example_documents_every_setting_the_script_reads():
    env = _text(ROOT, "src", "config", ".env.example")
    read = set(re.findall(r"\bGRAFANA_[A-Z_]+\b", _text(MON, "setup_grafana.sh")))
    for name in sorted(read):
        assert re.search(r"^%s=" % name, env, re.M), "%s is not in .env.example" % name
    assert re.search(r"^GRAFANA_HOST=127\.0\.0\.1$", env, re.M), "default must not be all interfaces"
    assert re.search(r"^GRAFANA_TLS_ENABLED=false$", env, re.M)


def test_example_admin_password_is_rejected_as_weak():
    """The placeholder in .env.example must fail the same check the install scripts use."""
    from src.config import config
    env = _text(ROOT, "src", "config", ".env.example")
    value = re.search(r"^GRAFANA_ADMIN_PASSWORD=(.*)$", env, re.M).group(1)
    assert config.weak_secret_reason(value)


def test_generate_secrets_replaces_grafana_password_only_when_present(tmp_path):
    script = os.path.join(ROOT, "scripts", "generate_secrets.sh")
    base = ("RABBITMQ_PASSWORD=x\nMARIADB_PASSWORD=x\nSWIFT_KEY=x\nHAPROXY_STATS_PASS=x\n"
            "SWIFT_HASH_PATH_PREFIX=a\nSWIFT_HASH_PATH_SUFFIX=b\n")
    with_g = tmp_path / "with.env"
    with_g.write_text(base + "GRAFANA_ADMIN_PASSWORD=changeme-run-generate-secrets\n")
    without = tmp_path / "without.env"
    without.write_text(base)
    outputs = []
    for f in (with_g, without):
        r = subprocess.run(["bash", script, "--write", "--file", str(f)], capture_output=True, text=True)
        assert r.returncode == 0, r.stderr
        outputs.append(r.stdout)
    new = re.search(r"^GRAFANA_ADMIN_PASSWORD=(.*)$", with_g.read_text(), re.M).group(1)
    assert re.fullmatch(r"[0-9a-f]{32}", new)
    assert "GRAFANA" not in without.read_text()
    assert new not in outputs[0], "a generated secret must never be printed"
