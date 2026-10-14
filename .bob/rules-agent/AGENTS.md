# Project Coding Rules (Non-Obvious Only)

## Mandatory import order in every processX.py
```python
import gt
gt.monkey_patch()   # ← FIRST — before any other import
# then stdlib, then pika, config, zk, swift_storage
```
Breaking this order causes pika/kazoo/pymysql to use blocking (non-cooperative) I/O.

## RabbitMQ connections — never share across greenthreads
Every consumer greenthread calls `gt.get_rmq_connection_with_retry(label)` to get its own
`BlockingConnection`. Sharing a connection between greenthreads silently corrupts message delivery.

## Consumer greenthread inner loop — `gt.sleep(0)` is mandatory
```python
while not gt.is_stopping():
    conn.process_data_events(time_limit=1)
    gt.sleep(0)   # ← omitting this starves sibling greenthreads
```

## ZK dedup barrier before every DB insert (Process 4)
```python
if not zklib.check_and_mark_processed(msg_id):
    ch.basic_ack(...)
    return   # skip insert — duplicate delivery
```

## `setup_queues(channel)` before first publish or consume
All 9 queues must be declared durable before use. Call `config.setup_queues(ch)` immediately after
opening any channel.

## `.env` values must be plain literals
`python-dotenv` silently passes `${VAR}` unexpanded. `config.py`'s `_int_env`/`_str_env` helpers
warn to stderr but fall back to defaults — the `.env` file itself must be fixed.

## `RABBITMQ_HOSTS` and `MARIADB_HOSTS` must stay blank in `.env`
`config.py` builds them from `NODE*_IP` at runtime. Setting them overrides the dynamic list.

## Port split: process1 binds 8082, HAProxy fronts 8080
Never pass `API_PORT` (8080) to `HTTPServer`. Use `API_BACKEND_PORT` (8082).

## MariaDB deadlock errors (1213/1205) are expected — already retried in `db.py`
Do not add outer retry logic; `insert_processed_message()` already retries 3× with backoff.

## Logging convention
- All modules (`process*.py`, `gt.py`, `zk.py`, `swift_storage.py`) use the `logging` module with a named logger
  (`log = logging.getLogger("flowfirst.pN")` in process files). Do not use `print()` for runtime messages.
- Use `log.warning` for degraded modes/duplicates, `log.error(..., exc_info=True)` inside `except` blocks, `log.debug` for per-item detail.
- Output reaches the journal AND `LOG_DIR/<process>.log` (rotating) only because `gt.monkey_patch()` calls `gt.configure_logging()`
  (`LOG_LEVEL` default INFO; `LOG_DIR` default `/var/log/flowfirst`, blank = journal only; `LOG_MAX_BYTES`, `LOG_BACKUP_COUNT`;
  pika/kazoo held at WARNING). Never add a second `basicConfig`.
  A new entry point must call `gt.monkey_patch()` first or its log lines are lost. Units set `PYTHONUNBUFFERED=1` and `LogsDirectory=flowfirst`.

## Secrets and message safety
- **Secrets fail closed:** `config.check_secrets()` (run at import) and `scripts/secrets_lib.sh` (install scripts) refuse an empty, short (`SECRET_MIN_LENGTH`, default 12) or well-known sample `RABBITMQ_PASSWORD`, `MARIADB_PASSWORD`, `SWIFT_KEY` (if Swift is enabled) and `HAPROXY_STATS_PASS`. Never print a secret value, never reintroduce a default password in code, and keep the two weak-value lists in step. `ALLOW_DEFAULT_SECRETS=true` is the only opt-out (local Docker dev). Generate values with `./scripts/generate_secrets.sh --write`.
- **Messages must not vanish silently:** publishers use confirms (`setup_queues()` calls `confirm_delivery()`) with `mandatory=True`; rejected messages (`nack requeue=False`) are dead-lettered by the `flowfirst-dlx` policy (`scripts/setup_rabbitmq_dlx.sh`) into `flowfirst_dead_letters`, inspected/replayed with `scripts/replay_dead_letters.py`. Do not add queue arguments to the pipeline queues (it breaks `queue_declare` on running clusters); use policies.

- **Admin authorisation:** `POST /zk/config` is allowed only for certificate CNs in `ADMIN_ALLOWED_CNS` (blank = nobody; no TLS = refused unless `ALLOW_UNAUTHENTICATED_ADMIN=true`, dev only). Identity logic is in `src/application/authz.py`; `X-SSL-Client-CN` is trusted only when the connection itself came from `ADMIN_TRUSTED_PROXY_CNS`. Any new state-changing endpoint must call `self._require_admin()`; never read the header without `authz.resolve_identity`.

- **Dependencies are pinned:** `requirements.txt` is generated (exact versions + hashes) from `requirements.in` by `scripts/update_requirements.sh`; never edit it by hand and install with `pip install --require-hashes -r requirements.txt`. To add or change a dependency edit `requirements.in`, regenerate, and commit both files.

- **Systemd sandbox:** the five unit files keep only the service definition; the sandbox lives in `systemd/flowfirst-hardening.conf`, installed as `<unit>.service.d/10-hardening.conf` by `install_services.sh`. The process may write only `/var/log/flowfirst` (+ private `/tmp`), has no capabilities, and may open only TCP/UDP/unix sockets. Code that needs a new writable path, a new socket family, `exec`, or a privileged call must change that file and be re-tested with `systemd-analyze security` and a real start; never loosen it silently. Per-node exceptions go in a `99-local.conf` drop-in.
- **Metrics:** `src/metrics/metrics.py` exposes `gt.metrics_counter` as Prometheus text on `METRICS_PORT_BASE` + process number (off unless `METRICS_ENABLED=true`). Call `metrics.start("processN")` first thing in `main()`. A new counter that matters operationally must be added to `EXPECTED_COUNTERS` (so it starts at 0) and, if it is a failure, be named `flow<N>_failed` so the alerts see it; `tests/test_metrics.py` checks that every seeded name is incremented somewhere. Metric output must never contain message content, credentials or hostnames of other systems. Alerts live in `monitoring/*.rules.yml` and are tested with `promtool test rules`; changing a metric name means changing them and `ReadMe` (Monitoring and Alerting).
- **CRL lifetime:** with `TLS_CRL_FILE` set, every component that loads the CRL (Process 1, the Python clients, HAProxy, MariaDB `ssl_crl`) refuses ALL certificates once the CRL passes its `nextUpdate` (tested). Never add a revocation path that skips `security/refresh_crl.sh` / `list_certs.sh` monitoring, and keep the preflight `revocation list` check. MariaDB needs `FLUSH SSL` after a new CRL is installed.
- **`/health` is liveness only:** HAProxy probes `GET /health` every 3 s, so it must answer from local state and make NO call to ZooKeeper, RabbitMQ, MariaDB or Swift (a slow dependency would otherwise mark every API node down at once). Dependency state belongs on `/ready` (local state only), `/zk/health`, `/swift/status` and in the metrics. Never point HAProxy at `/ready`: publisher connections are re-opened by the next publish, so a node taken out of rotation would never reconnect. `tests/test_process1_http.py` stubs a hanging ZooKeeper to enforce this.
- **Rejected messages:** a handler that fails does `log.error(...)`, `gt.metrics_counter["flow<N>_failed"] += 1`, then `basic_nack(requeue=False)`, in that order. Without the `flowfirst-dlx` policy RabbitMQ DISCARDS such a message (tested), so never rely on a nack for retry, never add `requeue=True` (poison messages would loop), and never declare the pipeline queues with `x-dead-letter-*` arguments (existing queues would refuse the declaration; the policy is the mechanism). Every publish keeps `mandatory=True` and publisher confirms; `mandatory=False` silently drops unroutable messages (tested).

- **Backups are encrypted and verified:** `scripts/backup_flowfirst.sh` / `restore_flowfirst.sh` (helpers in `backup_lib.sh`, ZooKeeper part in `zk_config_backup.py`) never write plaintext secrets to disk, need `--passphrase-file` (mode 600, 16+ chars), and the exit code is non-zero if any part failed. Back up state, not messages: queue contents are not backed up on purpose. Anything that restores must refuse to overwrite without an explicit flag. Passwords go to the MariaDB client through a mode-600 option file, never on the command line.

- **Production readiness:** `scripts/preflight_check.py` (read-only, prints no secret) is the per-node check; keep it in step when you add a setting that has an unsafe value (add a PASS/WARN/FAIL line). The ReadMe chapter "Production Rollout Run-Book" is the procedure; it states that it has not been run on a real cluster, so update it when it is. Rotating a password means changing it in the service as well as in `.env`.

- **Tests gate changes:** run `./scripts/ci_check.sh` before committing (pytest + ruff are in `requirements-dev.txt`, dev/CI hosts only). Add a test with every new rule in `authz.py`, `config.py` checks, `tls.py` or the Process 1 request handling. Tests that import `process1` must run it in a subprocess (`tests/_p1_harness.py`) because `gt.monkey_patch()` must never run inside pytest. Change dependencies only in `requirements*.in`, then run `scripts/update_requirements.sh`.

- **Publishing from Process 1 goes only through `_publish(routing_key, body)`.** It round-robins over `API_PUBLISH_CONNECTIONS` `_PublishSlot` objects, each with its own connection, channel (confirms on, via `setup_queues`) and semaphore. Never publish on a shared channel or call `basic_publish` directly in `process1.py`.

- **Request limits and generic errors:** Process 1 enforces `API_MAX_BODY_BYTES` (413), `API_MAX_BATCH_COUNT`, `API_MAX_LIST_LIMIT` and a per-connection `API_REQUEST_TIMEOUT_S`. Read POST bodies only through `_read_json_body()` and return errors only through `_fail()`, which logs the exception under a `request_id` and sends the client a generic message. Never put `str(e)`, a path, a URL or a host into a response body (`API_EXPOSE_ERROR_DETAILS` is the dev-only switch). Client-supplied numbers go through `int()`/`float()` inside the `ValueError`->400 handler.

## TLS (module `src/tls/tls.py`, tooling in `security/`)
- All TLS switches (`TLS_API_ENABLED`, `TLS_RABBITMQ_ENABLED`, `TLS_MARIADB_ENABLED`, `TLS_ZK_ENABLED`, `TLS_SWIFT_ENABLED`) default to `false`.
  Build client TLS only through `src.tls.tls` (`client_context`, `server_context`, `mysql_ssl`, `zk_kwargs`, `swift_kwargs`); do not hand-roll `ssl` contexts in process files.
- **Fail closed:** when a switch is on and a certificate/key/CA is missing or unreadable (or `SWIFT_AUTH_URL` is `http://`), raise at start. Never fall back to plain text, and never add an "insecure"/`verify=False`/`CERT_NONE` option.
- Never commit `pki/`, `*.key` or `*.pem` (they are in `.gitignore`). The CA key stays on the admin host; nodes receive only `ca.crt`, `ca.crl` and their own leaf keys (`security/install_certs.sh`).
- Process 1 keeps `API_BACKEND_PORT` (8082) and serves HTTPS there when `TLS_API_ENABLED=true`; HAProxy on 8080 requires client certificates and re-encrypts to it with `sni` + `verifyhost` on each backend `server` line (backends are addressed by IP).
- Change HAProxy only through `haproxy/setup_haproxy.sh` / `haproxy.cfg.template`, validate with `haproxy -c`, and never start `haproxy.service` directly (Pacemaker owns it).
- Revocation (`TLS_CRL_FILE`) is enforced by HAProxy and Process 1 only; RabbitMQ and ZooKeeper do not enforce it and MariaDB is unverified. Document limits honestly (ReadMe Scenario 63 L) and do not claim otherwise.
- TLS `.env` values follow the same rule as all others: literal paths, no `${VAR}`.
