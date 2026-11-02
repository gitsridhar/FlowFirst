"""Static checks for Prometheus setup script, configuration, and rule definitions."""
import os
import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MON_DIR = os.path.join(ROOT, "monitoring")


def test_setup_prometheus_script_exists_and_contains_core_elements():
    script_path = os.path.join(MON_DIR, "setup_prometheus.sh")
    assert os.path.isfile(script_path), "setup_prometheus.sh missing"
    text = open(script_path).read()

    assert "prometheus.service" in text
    assert "/etc/prometheus/prometheus.yml" in text
    assert "/var/lib/prometheus" in text
    assert "promtool check config" in text
    assert "--web.enable-lifecycle" in text
    assert "useradd" in text and "prometheus" in text
    assert "LimitNOFILE=65536" in text
    assert "systemctl enable --now prometheus" in text


def test_prometheus_yml_parses_and_has_required_jobs():
    config_path = os.path.join(MON_DIR, "prometheus.yml")
    assert os.path.isfile(config_path), "prometheus.yml missing"
    data = yaml.safe_load(open(config_path).read())

    assert "scrape_configs" in data
    jobs = [sc["job_name"] for sc in data["scrape_configs"]]
    assert "flowfirst" in jobs
    assert "rabbitmq" in jobs
    assert "rabbitmq_queues" in jobs
    assert "mysqld" in jobs
    assert "node" in jobs

    assert "rule_files" in data
    assert any("flowfirst.rules.yml" in str(r) for r in data["rule_files"])
    assert any("infrastructure.rules.yml" in str(r) for r in data["rule_files"])


def test_flowfirst_rules_yaml_structure():
    rules_path = os.path.join(MON_DIR, "flowfirst.rules.yml")
    assert os.path.isfile(rules_path), "flowfirst.rules.yml missing"
    data = yaml.safe_load(open(rules_path).read())

    assert "groups" in data
    alert_names = []
    for group in data["groups"]:
        for rule in group.get("rules", []):
            if "alert" in rule:
                alert_names.append(rule["alert"])

    assert "FlowFirstProcessDown" in alert_names
    assert "FlowFirstNoActiveInstance" in alert_names
    assert "FlowFirstMessagesFailing" in alert_names
    assert "FlowFirstPipelineStalled" in alert_names
    assert "FlowFirstDeadLetters" in alert_names


def test_infrastructure_rules_yaml_structure():
    rules_path = os.path.join(MON_DIR, "infrastructure.rules.yml")
    assert os.path.isfile(rules_path), "infrastructure.rules.yml missing"
    data = yaml.safe_load(open(rules_path).read())

    assert "groups" in data
    alert_names = []
    for group in data["groups"]:
        for rule in group.get("rules", []):
            if "alert" in rule:
                alert_names.append(rule["alert"])

    assert "GaleraClusterSizeLow" in alert_names
    assert "GaleraNodeNotSynced" in alert_names
    assert "MariaDbExporterDown" in alert_names
    assert "RabbitMqNodeDown" in alert_names
    assert "NodeDiskFillingUp" in alert_names
