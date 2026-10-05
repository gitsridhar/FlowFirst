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
- `process*.py`: use `print(f"[PX][gt] ...")` — no `logging` calls
- `gt.py`, `zk.py`, `swift_storage.py`: use `logging` module with named loggers
