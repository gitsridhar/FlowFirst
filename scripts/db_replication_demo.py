#!/usr/bin/env python3
"""FlowFirst demo: write rows to MariaDB Galera and read them back from every node.

Shows the multi-master behaviour of the cluster with plain SQL, without the REST API or RabbitMQ:
a row written through ANY node is certified and applied on ALL nodes, and can be read from any of them.
It uses its own table `replication_demo`, so pipeline data (processed_messages, devices) is never touched.

    python3 scripts/db_replication_demo.py                      full demo (same as 'run')
    python3 scripts/db_replication_demo.py run --keep           ...and leave the rows in place
    python3 scripts/db_replication_demo.py write --node mgmtnode2 --message "hello from mgmtnode2"
    python3 scripts/db_replication_demo.py read  --node mgmtnode3
    python3 scripts/db_replication_demo.py read  --node all --run-id <id>
    python3 scripts/db_replication_demo.py cleanup [--drop]

Where to run it: on mgmtnode1, mgmtnode2, mgmtnode3, remotenode1 or any machine that can reach TCP 3306 on the Galera nodes and has
the same `.env` (MARIADB_* and NODE1_IP..NODE3_IP). Remote Node 1 runs no MariaDB, but can run this script as a client.

--node / --nodes accept: mgmtnode1 | mgmtnode2 | mgmtnode3 (taken from NODE1_IP..NODE3_IP), vip (MARIADB_HOST, i.e. the
HAProxy load balancer), or a literal host/IP. Writes through `vip` land on whichever node HAProxy picks.
Connections honour TLS_MARIADB_ENABLED through src.tls.tls (fails closed). The password is never printed.
"""
import argparse
import os
import sys
import time
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import pymysql  # noqa: E402
from pymysql.cursors import DictCursor  # noqa: E402

from src.config.config import (  # noqa: E402
    MARIADB_DB,
    MARIADB_HOST,
    MARIADB_PASSWORD,
    MARIADB_PORT,
    MARIADB_USER,
    NODE1_IP,
    NODE2_IP,
    NODE3_IP,
    NODE_NAME,
)
from src.tls.tls import mysql_ssl  # noqa: E402

TABLE = "replication_demo"
DEFAULT_NODES = "mgmtnode1,mgmtnode2,mgmtnode3"
DEFAULT_TIMEOUT_S = 10.0
POLL_INTERVAL_S = 0.05

DDL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id          BIGINT AUTO_INCREMENT PRIMARY KEY,
        run_id      VARCHAR(36)  NOT NULL,
        entry_id    VARCHAR(36)  NOT NULL UNIQUE,
        written_via VARCHAR(64)  NOT NULL,
        server_host VARCHAR(64)  NULL,
        client_node VARCHAR(64)  NULL,
        message     VARCHAR(255) NULL,
        written_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
        INDEX idx_run (run_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


# ----------------------------------------------------------------------------- pure helpers (unit-tested)

def named_targets():
    """Label -> host for the Galera members and the load-balanced VIP."""
    return {"mgmtnode1": NODE1_IP, "mgmtnode2": NODE2_IP, "mgmtnode3": NODE3_IP, "vip": MARIADB_HOST}


def resolve_target(spec, targets=None):
    """('mgmtnode2') -> ('mgmtnode2', '<NODE2_IP>'); a literal host/IP is used as its own label."""
    spec = (spec or "").strip()
    if not spec:
        raise ValueError("empty node name")
    targets = targets if targets is not None else named_targets()
    if spec in targets:
        return spec, targets[spec]
    return spec, spec


def resolve_targets(csv, targets=None):
    """'mgmtnode1,mgmtnode2' -> [(label, host), ...] without duplicates (same host listed twice is kept once)."""
    out, seen = [], set()
    for part in (csv or "").split(","):
        if not part.strip():
            continue
        label, host = resolve_target(part, targets)
        if host not in seen:
            seen.add(host)
            out.append((label, host))
    if not out:
        raise ValueError("no nodes given")
    return out


def render_table(rows, columns):
    """Fixed-width text table. rows: list of dicts, columns: list of (key, heading)."""
    cells = [[str(r.get(k, "")) if r.get(k) is not None else "" for k, _ in columns] for r in rows]
    widths = [max([len(h)] + [len(c[i]) for c in cells]) for i, (_, h) in enumerate(columns)]
    line = "  ".join(h.ljust(widths[i]) for i, (_, h) in enumerate(columns))
    out = [line, "  ".join("-" * w for w in widths)]
    out += ["  ".join(c[i].ljust(widths[i]) for i in range(len(columns))) for c in cells]
    return "\n".join(out)


def missing_entries(expected_ids, seen_ids):
    """Entry ids that were written but are not visible yet (sorted, for stable output)."""
    return sorted(set(expected_ids) - set(seen_ids))


# ----------------------------------------------------------------------------- database access

def connect(host, database=True):
    """One short-lived autocommit connection to ONE specific node (no failover to other nodes, on purpose)."""
    return pymysql.connect(
        host=host,
        port=MARIADB_PORT,
        user=MARIADB_USER,
        password=MARIADB_PASSWORD,
        database=MARIADB_DB if database else None,
        cursorclass=DictCursor,
        autocommit=True,
        connect_timeout=5,
        ssl=mysql_ssl(),
    )


def server_identity(conn):
    """Which server answered: OS hostname and (when Galera is on) wsrep_node_name / cluster state."""
    with conn.cursor() as cur:
        cur.execute("SELECT @@hostname AS host")
        host = cur.fetchone()["host"]
        cur.execute("SHOW VARIABLES LIKE 'wsrep_node_name'")
        row = cur.fetchone()
        wsrep_name = row["Value"] if row else ""
        cur.execute("SHOW STATUS LIKE 'wsrep_cluster_size'")
        row = cur.fetchone()
        size = row["Value"] if row else "n/a"
    return {"host": host, "wsrep_node_name": wsrep_name or "(wsrep off)", "cluster_size": size}


def ensure_table(host):
    """DDL is replicated by Galera (total order isolation), so running it on one node creates it on all."""
    conn = connect(host)
    try:
        with conn.cursor() as cur:
            cur.execute(DDL)
    finally:
        conn.close()


def write_entry(label, host, run_id, message):
    """INSERT one row through the given node. Returns a dict describing what was written and where."""
    entry_id = str(uuid.uuid4())
    conn = connect(host)
    try:
        ident = server_identity(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"INSERT INTO {TABLE} (run_id, entry_id, written_via, server_host, client_node, message) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (run_id, entry_id, label, ident["host"], NODE_NAME, message),
            )
            row_id = cur.lastrowid
    finally:
        conn.close()
    return {"id": row_id, "entry_id": entry_id, "via": label, "server": ident["host"], "message": message}


def read_entries(host, run_id=None, limit=20):
    """SELECT rows through the given node. Returns (server identity, rows)."""
    conn = connect(host)
    try:
        ident = server_identity(conn)
        sql = (f"SELECT id, run_id, entry_id, written_via, server_host, client_node, message, "
               f"DATE_FORMAT(written_at, '%%H:%%i:%%s.%%f') AS written_at FROM {TABLE}")
        params = []
        if run_id:
            sql += " WHERE run_id = %s"
            params.append(run_id)
        sql += " ORDER BY id DESC LIMIT %s"
        params.append(int(limit))
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = list(cur.fetchall())
    finally:
        conn.close()
    return ident, rows


def wait_until_visible(host, run_id, expected_ids, timeout_s):
    """Poll one node until every expected entry is visible. Returns (rows, seconds_waited, missing_ids)."""
    start = time.time()
    while True:
        _, rows = read_entries(host, run_id, limit=1000)
        missing = missing_entries(expected_ids, [r["entry_id"] for r in rows])
        waited = time.time() - start
        if not missing or waited >= timeout_s:
            return rows, waited, missing
        time.sleep(POLL_INTERVAL_S)


# ----------------------------------------------------------------------------- commands

COLUMNS = [("id", "id"), ("written_via", "written via"), ("server_host", "server"), ("client_node", "client"),
           ("message", "message"), ("written_at", "written at")]


def cmd_run(args):
    nodes = resolve_targets(args.nodes)
    run_id = str(uuid.uuid4())
    print("== FlowFirst Galera write/read demo ==")
    print(f"run id : {run_id}")
    print(f"nodes  : {', '.join(f'{label} ({host})' for label, host in nodes)}")
    print(f"client : {NODE_NAME}")

    print("\n[1] Create the demo table on the first node (replicated to the others by Galera)")
    ensure_table(nodes[0][1])
    print(f"    table `{TABLE}` is ready")

    print("\n[2] Write one row through EACH node")
    written = []
    for label, host in nodes:
        entry = write_entry(label, host, run_id, f"hello from {NODE_NAME} via {label}")
        written.append(entry)
        print(f"    wrote id={entry['id']:<6} via {label:<6} (server {entry['server']})")
    expected = [w["entry_id"] for w in written]

    print("\n[3] Read the rows back from EVERY node")
    ok = True
    for label, host in nodes:
        rows, waited, missing = wait_until_visible(host, run_id, expected, args.timeout)
        ident, _ = read_entries(host, run_id, limit=1)
        print(f"\n    -- {label} ({host}) server={ident['host']} wsrep={ident['wsrep_node_name']} "
              f"cluster_size={ident['cluster_size']} -- {len(rows)}/{len(expected)} rows, visible after {waited * 1000:.0f} ms")
        print("    " + render_table(sorted(rows, key=lambda r: r["id"]), COLUMNS).replace("\n", "\n    "))
        if missing:
            ok = False
            print(f"    MISSING on {label}: {len(missing)} row(s) not visible within {args.timeout:.0f}s")

    print("\n[4] Result")
    if ok:
        print(f"    PASS: all {len(expected)} rows written through {len(nodes)} different nodes are readable on every node.")
        print("    Note the `id` values: Galera gives each node its own auto_increment offset and steps by the")
        print("    cluster size, so ids written through different nodes differ and have gaps (for example 4, 5, 6).")
    else:
        print("    FAIL: replication is incomplete. Run mariadb-galera/check_galera_status.sh on a node.")

    if args.keep:
        print(f"\n    Rows kept. Read them again with:  read --node all --run-id {run_id}")
    else:
        deleted = delete_run(nodes[0][1], run_id)
        print(f"\n    Cleaned up {deleted} demo row(s) (use --keep to leave them for inspection).")
    return 0 if ok else 1


def delete_run(host, run_id):
    conn = connect(host)
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {TABLE} WHERE run_id = %s", (run_id,))
            return cur.rowcount
    finally:
        conn.close()


def cmd_write(args):
    label, host = resolve_target(args.node)
    ensure_table(host)
    run_id = args.run_id or str(uuid.uuid4())
    entry = write_entry(label, host, run_id, args.message)
    print(f"wrote id={entry['id']} entry_id={entry['entry_id']} via {label} ({host}) server={entry['server']}")
    print(f"run id: {run_id}")
    return 0


def cmd_read(args):
    nodes = resolve_targets(DEFAULT_NODES) if args.node == "all" else [resolve_target(args.node)]
    for label, host in nodes:
        ident, rows = read_entries(host, args.run_id, args.limit)
        print(f"-- {label} ({host}) server={ident['host']} wsrep={ident['wsrep_node_name']} "
              f"cluster_size={ident['cluster_size']} -- {len(rows)} row(s)")
        if rows:
            print(render_table(rows, COLUMNS))
        print()
    return 0


def cmd_cleanup(args):
    _, host = resolve_target(args.node or "mgmtnode1")
    conn = connect(host)
    try:
        with conn.cursor() as cur:
            if args.drop:
                cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
                print(f"dropped table `{TABLE}` (on all nodes, via Galera)")
            else:
                cur.execute(f"DELETE FROM {TABLE}")
                print(f"deleted {cur.rowcount} row(s) from `{TABLE}`")
    finally:
        conn.close()
    return 0


def build_parser():
    p = argparse.ArgumentParser(description="Write to and read from the FlowFirst MariaDB Galera cluster across nodes.")
    sub = p.add_subparsers(dest="cmd")

    r = sub.add_parser("run", help="full demo: write through every node, read from every node (default)")
    r.add_argument("--nodes", default=DEFAULT_NODES, help="comma list of mgmtnode1,mgmtnode2,mgmtnode3,vip or hosts (default: %(default)s)")
    r.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_S, help="seconds to wait for a row to appear on a node")
    r.add_argument("--keep", action="store_true", help="leave the demo rows in the table")
    r.set_defaults(func=cmd_run)

    w = sub.add_parser("write", help="insert one row through one node")
    w.add_argument("--node", required=True, help="mgmtnode1|mgmtnode2|mgmtnode3|vip or a host")
    w.add_argument("--message", default="hello from the FlowFirst demo")
    w.add_argument("--run-id", default=None, help="group the row under an existing run id")
    w.set_defaults(func=cmd_write)

    rd = sub.add_parser("read", help="list the newest rows as seen by one node (or all)")
    rd.add_argument("--node", default="all", help="mgmtnode1|mgmtnode2|mgmtnode3|vip|all or a host (default: all)")
    rd.add_argument("--run-id", default=None)
    rd.add_argument("--limit", type=int, default=20)
    rd.set_defaults(func=cmd_read)

    c = sub.add_parser("cleanup", help="delete all demo rows (or drop the table)")
    c.add_argument("--node", default="mgmtnode1")
    c.add_argument("--drop", action="store_true", help="drop the table instead of emptying it")
    c.set_defaults(func=cmd_cleanup)
    return p


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.cmd:
        args = parser.parse_args(["run"] + list(argv if argv is not None else sys.argv[1:]))
    if not MARIADB_PASSWORD:
        print("ERROR: MARIADB_PASSWORD is not set (put it in .env).", file=sys.stderr)
        return 2
    try:
        return args.func(args)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    except pymysql.err.MySQLError as e:
        print(f"ERROR: database operation failed: {e.args[0] if e.args else type(e).__name__} "
              f"{e.args[1] if len(e.args) > 1 else ''}".rstrip(), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
