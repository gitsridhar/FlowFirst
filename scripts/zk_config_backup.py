#!/usr/bin/env python3
"""Dump or restore the ZooKeeper runtime configuration (/flowfirst/config/*) as JSON.

    python3 scripts/zk_config_backup.py dump                 JSON on stdout
    python3 scripts/zk_config_backup.py restore < file.json  write the values back (only the known keys)

Only /flowfirst/config is worth backing up: it holds operator-chosen tuning values. Leader election, the registry,
health and dedup znodes are rebuilt by the running processes. Uses the project's ZooKeeper client, so TLS and the
ensemble addresses come from .env like everywhere else. Prints nothing but JSON on stdout (logs go to stderr).
"""
import json
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
logging.basicConfig(stream=sys.stderr, level=logging.WARNING)

from src.zookeeper import zk  # noqa: E402


def dump() -> dict:
    client = zk.get_client()
    values = {}
    for key in sorted(zk._CONFIG_DEFAULTS):
        path = "%s/%s" % (zk._CONFIG_ROOT, key)
        if client.exists(path):
            values[key] = client.get(path)[0].decode()
    return {"format": 1, "config": values}


def restore(doc: dict) -> int:
    if not isinstance(doc, dict) or doc.get("format") != 1 or not isinstance(doc.get("config"), dict):
        print("not a FlowFirst ZooKeeper config backup", file=sys.stderr)
        return 1
    written = 0
    for key, value in doc["config"].items():
        if zk.write_config(key, value):                 # refuses keys that are not known runtime settings
            written += 1
        else:
            print("skipped unknown key: %s" % key, file=sys.stderr)
    print("restored %d value(s)" % written, file=sys.stderr)
    return 0


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in ("dump", "restore"):
        print(__doc__, file=sys.stderr)
        return 2
    try:
        if sys.argv[1] == "dump":
            json.dump(dump(), sys.stdout, indent=2, sort_keys=True)
            sys.stdout.write("\n")
            return 0
        return restore(json.load(sys.stdin))
    finally:
        try:
            zk.close_client()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
