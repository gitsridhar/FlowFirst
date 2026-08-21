import json
import time
import pymysql
from pymysql.cursors import DictCursor
from config import (
    MARIADB_HOST,
    MARIADB_PORT,
    MARIADB_USER,
    MARIADB_PASSWORD,
    MARIADB_DB,
    MARIADB_HOSTS,
)


def get_db_connection(max_retries: int = 3, retry_delay: float = 1.0):
    """
    Create and return a connection to MariaDB Galera Cluster.
    Supports retry with backoff for cluster node transitions.
    """
    # Build list of potential connection targets from environment
    hosts_to_try = []
    if MARIADB_HOST:
        hosts_to_try.append(MARIADB_HOST)
    if MARIADB_HOSTS:
        for h in MARIADB_HOSTS.split(","):
            cleaned = h.strip()
            if cleaned and cleaned not in hosts_to_try:
                hosts_to_try.append(cleaned)
    if not hosts_to_try:
        hosts_to_try = ["localhost"]

    last_error = None
    for attempt in range(max_retries):
        for host in hosts_to_try:
            try:
                conn = pymysql.connect(
                    host=host,
                    port=MARIADB_PORT,
                    user=MARIADB_USER,
                    password=MARIADB_PASSWORD,
                    database=MARIADB_DB,
                    cursorclass=DictCursor,
                    autocommit=True,
                    connect_timeout=3,
                )
                return conn
            except Exception as e:
                last_error = e
        time.sleep(retry_delay)

    raise ConnectionError(f"Failed to connect to MariaDB Galera cluster across targets {hosts_to_try}: {last_error}")


def init_database():
    """Ensure database schema is created on the Galera Cluster."""
    conn = pymysql.connect(
        host=MARIADB_HOST,
        port=MARIADB_PORT,
        user=MARIADB_USER,
        password=MARIADB_PASSWORD,
        autocommit=True,
        connect_timeout=5,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{MARIADB_DB}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
            cursor.execute(f"USE `{MARIADB_DB}`;")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS processed_messages (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    message_id VARCHAR(64) NOT NULL UNIQUE,
                    flow_id INT NOT NULL,
                    item_id INT NOT NULL,
                    initial_data VARCHAR(255),
                    counter_value INT NULL,
                    metric_value DECIMAL(10, 2) NULL,
                    examined_status VARCHAR(50) NULL,
                    verified_by VARCHAR(50) NULL,
                    history_trail JSON NOT NULL,
                    raw_payload JSON NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_flow_item (flow_id, item_id),
                    INDEX idx_created_at (created_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            )
    finally:
        conn.close()


def insert_processed_message(data: dict) -> int:
    """
    Insert a message payload received by Process 4 into MariaDB Galera cluster.
    Handles deadlocks/lock-wait errors gracefully with automatic retry (Galera WSREP certification).
    """
    max_retries = 3
    for attempt in range(max_retries):
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                sql = """
                    INSERT INTO processed_messages (
                        message_id,
                        flow_id,
                        item_id,
                        initial_data,
                        counter_value,
                        metric_value,
                        examined_status,
                        verified_by,
                        history_trail,
                        raw_payload
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        counter_value = VALUES(counter_value),
                        metric_value = VALUES(metric_value),
                        examined_status = VALUES(examined_status),
                        verified_by = VALUES(verified_by),
                        history_trail = VALUES(history_trail),
                        raw_payload = VALUES(raw_payload);
                """
                cursor.execute(
                    sql,
                    (
                        data.get("message_id"),
                        data.get("flow"),
                        data.get("item_id"),
                        data.get("initial_data"),
                        data.get("counter"),
                        data.get("value"),
                        data.get("examined_status"),
                        data.get("verified_by"),
                        json.dumps(data.get("history", [])),
                        json.dumps(data),
                    ),
                )
                return cursor.lastrowid
        except pymysql.err.OperationalError as op_err:
            # 1213: Deadlock, 1205: Lock wait timeout (common during concurrent multi-master wsrep conflict)
            if op_err.args[0] in (1213, 1205) and attempt < max_retries - 1:
                time.sleep(0.2 * (attempt + 1))
                continue
            raise
        finally:
            if conn:
                conn.close()
