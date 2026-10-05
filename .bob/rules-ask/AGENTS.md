# Project Documentation Context (Non-Obvious Only)

## There are no tests, no CI, no linter config
No `pytest`, no `unittest`, no `tox.ini`, no `.flake8`, no `mypy.ini`. Validation is manual
via `scripts/test_flows_and_inspect_db.sh` and the 44-scenario playbook in `ReadMe.md`.

## `gt.py` is the project's concurrency layer — not a thin import wrapper
It owns the global `GreenPool`, named greenthread registry, `stop_event`, metrics counters, and
`get_rmq_connection_with_retry()`. Every process depends on it; it is not optional infrastructure.

## `config.py` does runtime construction of host lists
`RABBITMQ_HOSTS` and `MARIADB_HOSTS` are built from `NODE1_IP..NODE3_IP` at import time unless
the env vars are explicitly set. Reading `RABBITMQ_HOST` alone misses the multi-node failover path.

## ZooKeeper `/flowfirst` tree does not exist until the first process connects
Setup scripts only install ZooKeeper. The znode tree (`/flowfirst/election`, `/flowfirst/config`,
etc.) is lazily created by `zk.get_client()` → `_ensure_base_paths()` on first process startup.

## Process 5 runs exclusively on Node 4 (remote worker)
`systemd/install_services.sh --remote` installs only `flowfirst-process5.service` on Node 4.
Nodes 1–3 run processes 1–4 only. Never install process5 on core cluster nodes.

## `ReadMe.md` (capital R, no extension conflict) is the canonical ops reference
44 numbered scenarios cover every failure mode, network partition, rolling reboot, and ZK/Galera
recovery. It is the primary runbook. `ReadMe` (no extension) is also present but `ReadMe.md` is
the one with full content.

## HAProxy is Pacemaker-managed — never start `haproxy.service` manually on cluster nodes
`setup_haproxy.sh` generates config and validates syntax only. HAProxy starts automatically when
Pacemaker assigns the VIP to a node. Manual start conflicts with Pacemaker resource management.

## `config.get_connection()` returns a pika connection with a non-standard `_connected_to` attribute
This attribute (`conn._connected_to`) surfaces which RabbitMQ node was actually chosen in the
multi-host failover path. It is not part of pika's public API — it is attached by `config.py`.
