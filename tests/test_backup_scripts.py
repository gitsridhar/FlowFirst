"""Backup/restore scripts: encryption round trip, tamper and wrong-passphrase detection, safety refusals.
Needs bash and openssl on the machine; the database, ZooKeeper and RabbitMQ parts need real servers and were tested
by hand (see ReadMe, Backup and Restore)."""
import os
import shutil
import subprocess

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
pytestmark = pytest.mark.skipif(not (shutil.which("bash") and shutil.which("openssl")), reason="needs bash and openssl")


def sh(script, *, cwd=ROOT, env=None):
    p = subprocess.run(["bash", "-c", script], cwd=cwd, capture_output=True, text=True, timeout=60,
                       env=dict(os.environ, **(env or {})))
    return p.returncode, p.stdout + p.stderr


@pytest.fixture
def work(tmp_path):
    pw = tmp_path / "pass"
    pw.write_text("a-long-enough-test-passphrase")
    pw.chmod(0o600)
    return tmp_path, pw


def lib(cmd):
    return "source scripts/backup_lib.sh; " + cmd


def test_encrypt_decrypt_round_trip(work):
    d, pw = work
    (d / "plain").write_text("secret row \u65e5\u672c\u8a9e\n")
    code, out = sh(lib(f"bk_encrypt {pw} {d}/plain {d}/enc && bk_decrypt {pw} {d}/enc {d}/back"))
    assert code == 0, out
    assert (d / "back").read_text() == (d / "plain").read_text()
    assert b"secret row" not in (d / "enc").read_bytes()


def test_wrong_passphrase_fails(work):
    d, pw = work
    (d / "plain").write_text("x" * 100)
    other = d / "other"
    other.write_text("a-different-long-passphrase")
    other.chmod(0o600)
    assert sh(lib(f"bk_encrypt {pw} {d}/plain {d}/enc"))[0] == 0
    assert sh(lib(f"bk_decrypt {other} {d}/enc {d}/back 2>/dev/null"))[0] != 0


def test_damaged_file_does_not_decrypt_to_the_original(work):
    d, pw = work
    original = os.urandom(4000)
    (d / "plain").write_bytes(original)
    sh(lib(f"bk_encrypt {pw} {d}/plain {d}/enc"))
    data = bytearray((d / "enc").read_bytes())
    data[len(data) // 2] ^= 0xFF
    (d / "enc").write_bytes(bytes(data))
    sh(lib(f"bk_decrypt {pw} {d}/enc {d}/back 2>/dev/null"))
    assert not (d / "back").exists() or (d / "back").read_bytes() != original


@pytest.mark.parametrize("content,mode,fragment", [
    ("short", 0o600, "shorter than 16"),
    ("a-long-enough-test-passphrase", 0o644, "must not be accessible"),
    ("", 0o600, "empty"),
])
def test_bad_passphrase_files_are_refused(tmp_path, content, mode, fragment):
    f = tmp_path / "p"
    f.write_text(content)
    f.chmod(mode)
    code, out = sh(lib(f"bk_check_passfile {f}"))
    assert code != 0 and fragment in out


def test_backup_requires_out_and_passphrase_file():
    assert sh("./scripts/backup_flowfirst.sh")[0] != 0
    assert sh("./scripts/backup_flowfirst.sh --out /nonexistent-x")[0] != 0


def test_backup_rejects_unknown_part(work):
    d, pw = work
    code, out = sh(f"./scripts/backup_flowfirst.sh --out {d} --passphrase-file {pw} --only nonsense")
    assert code != 0 and "unknown part" in out


def test_restore_refuses_in_place_without_confirmation(work):
    d, pw = work
    (d / "bk").mkdir()
    code, out = sh(f"./scripts/restore_flowfirst.sh --from {d}/bk --passphrase-file {pw} --db --in-place")
    assert code != 0 and "--yes-overwrite" in out


def test_restore_refuses_extracting_into_root_or_nonempty_dir(work):
    d, pw = work
    (d / "bk").mkdir()
    (d / "full").mkdir()
    (d / "full" / "x").write_text("1")
    assert "refusing" in sh(f"./scripts/restore_flowfirst.sh --from {d}/bk --passphrase-file {pw} --extract-config /")[1]
    assert "EMPTY" in sh(f"./scripts/restore_flowfirst.sh --from {d}/bk --passphrase-file {pw} --extract-config {d}/full")[1]


def test_verify_fails_without_manifest(work):
    d, pw = work
    (d / "bk").mkdir()
    code, out = sh(f"./scripts/restore_flowfirst.sh --from {d}/bk --passphrase-file {pw} --verify")
    assert code != 0 and "MANIFEST" in out


def test_restore_into_the_live_database_name_is_refused(work):
    d, pw = work
    (d / "bk").mkdir()
    env = d / "env"
    env.write_text("MARIADB_USER=u\nMARIADB_PASSWORD=pppppppppppppppp\nMARIADB_DB=livedb\n")
    env.chmod(0o600)
    code, out = sh(f"./scripts/restore_flowfirst.sh --from {d}/bk --passphrase-file {pw} --db --into-database livedb",
                   env={"FLOWFIRST_ENV_FILE": str(env)})
    assert code != 0 and ("live database" in out or "not installed" in out)


def test_zk_restore_rejects_foreign_document():
    p = subprocess.run(["python3", "-c",
                        "import sys,types; sys.path.insert(0,'scripts'); "
                        "sys.modules['src.zookeeper']=types.ModuleType('src.zookeeper'); "
                        "z=types.ModuleType('zk'); z._CONFIG_DEFAULTS={}; sys.modules['src.zookeeper'].zk=z; "
                        "sys.modules['src.zookeeper.zk']=z; import zk_config_backup as m; "
                        "print(m.restore({'format': 2, 'config': {}}))"],
                       cwd=ROOT, capture_output=True, text=True, timeout=30)
    assert p.stdout.strip() == "1"
