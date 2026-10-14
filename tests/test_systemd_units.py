"""Static checks on the systemd units and the shared sandbox drop-in. They cannot prove the sandbox works (that needs
a real systemd; see ReadMe Step 4c) but they stop the hardening from being silently removed or contradicted."""
import glob
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SD = os.path.join(ROOT, "systemd")


def settings(path):
    out = {}
    for line in open(path):
        line = line.strip()
        if line and not line.startswith(("#", "[")) and "=" in line:
            key, value = line.split("=", 1)
            out.setdefault(key, []).append(value)
    return out


def test_hardening_file_has_the_core_settings():
    h = settings(os.path.join(SD, "flowfirst-hardening.conf"))
    assert h["ProtectSystem"] == ["strict"]
    assert h["NoNewPrivileges"] == ["true"]
    assert h["CapabilityBoundingSet"] == [""]
    assert h["MemoryDenyWriteExecute"] == ["true"]
    assert h["PrivateTmp"] == ["true"]
    assert set(h["RestrictAddressFamilies"][0].split()) == {"AF_INET", "AF_INET6", "AF_UNIX"}
    assert h["UMask"] == ["0027"]


def test_no_unit_weakens_or_duplicates_the_sandbox():
    for unit in glob.glob(os.path.join(SD, "flowfirst-process*.service")):
        s = settings(unit)
        for key in ("ProtectSystem", "ProtectHome", "NoNewPrivileges", "CapabilityBoundingSet", "RestrictAddressFamilies"):
            assert key not in s, "%s sets %s itself; it belongs in flowfirst-hardening.conf" % (os.path.basename(unit), key)


def test_all_five_units_exist_and_keep_their_essentials():
    units = sorted(glob.glob(os.path.join(SD, "flowfirst-process*.service")))
    assert len(units) == 5
    for unit in units:
        s = settings(unit)
        assert s["User"] == ["flowuser"] and s["LimitNOFILE"] == ["65536"] and s["LogsDirectory"] == ["flowfirst"], unit
        assert re.match(r".*-m src\.application\.process[1-5]$", s["ExecStart"][0]), unit


def test_logs_directory_is_the_only_writable_path_and_matches_default_log_dir():
    # ProtectSystem=strict makes everything else read-only; the default LOG_DIR must be what LogsDirectory= creates.
    h = open(os.path.join(SD, "flowfirst-hardening.conf")).read()
    assert "ReadWritePaths" not in re.sub(r"(?m)^#.*$", "", h)
    gt = open(os.path.join(ROOT, "src", "gt", "gt.py")).read()
    assert '"/var/log/flowfirst"' in gt


def test_installer_installs_the_drop_in_and_parses():
    text = open(os.path.join(SD, "install_services.sh")).read()
    assert "10-hardening.conf" in text and "flowfirst-hardening.conf" in text
    assert "ProtectHome=read-only" in text           # the /home and /root exception
