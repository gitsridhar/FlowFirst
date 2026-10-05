# Project Architecture Rules (Non-Obvious Only)

## Leader election is per-process, not per-node
Each of the 5 processes runs its own independent ZooKeeper `Election`. On a 3-node cluster, one
node is P1-leader, a different node may be P2-leader, etc. Only the elected node per process
actively consumes or publishes; the others stand by.

## Each flow has its own isolated queue chain — no shared queues between flows
Flow 1, 2, and 3 use entirely separate queue names. A message published to a flow-1 queue can
never be consumed by a flow-2 or flow-3 consumer by design.

## Flow 3 crosses the cluster boundary through RabbitMQ (not direct TCP)
P1 on cluster nodes publishes to `flow3_p1_to_p5`; P5 on Node 4 consumes it. P5 connects to
the same RabbitMQ pool used by the cluster — there is no separate broker on Node 4.

## ZooKeeper dedup barrier is the only double-insert protection
There is no database-level idempotency guard beyond the `UNIQUE` constraint on `message_id`.
The `ON DUPLICATE KEY UPDATE` in `insert_processed_message()` is the fallback; the ZK dedup
znode check is the primary barrier that prevents the Galera wsrep certification retry path.

## `SWIFT_ENABLED=false` silently skips all object storage calls — it does not raise
Every `swift_storage.py` function checks `SWIFT_ENABLED` at the top and returns a failure tuple
without connecting. Process 4 treats a Swift failure as non-fatal (logs it, still acks the message).

## MariaDB schema is initialised by Process 4 at startup, not by a migration tool
`db.init_database()` runs `CREATE TABLE IF NOT EXISTS` on every P4 startup. There is no migration
framework. Schema changes require manual `ALTER TABLE` on the Galera cluster.

## eventlet monkey-patch must precede pika/kazoo/pymysql imports across the entire process
The patch is global to the Python process. A library imported before `gt.monkey_patch()` retains
blocking socket behaviour and will stall the entire greenthread cooperative loop when it does I/O.

## Process 1's HTTP server uses `eventlet.listen()`, not `HTTPServer.server_bind()`
The socket is created by `eventlet.listen((host, port))` and assigned to `httpd.socket` after
constructing `HTTPServer` with `bind_and_activate=False`. Using normal `HTTPServer` bind would
create a blocking socket that bypasses the eventlet loop.

## Hot-reloadable config values live in ZooKeeper, not in `.env`
`flow2_high_threshold`, `flow1_counter_step`, and `flow2_scale_factor` are read from
`/flowfirst/config/*` znodes via `ConfigWatcher.DataWatch`. Changes via `POST /zk/config`
propagate to all running processes within seconds without restart.
