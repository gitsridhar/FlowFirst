import json
import time
import pymysql
from pymysql.cursors import DictCursor
from src.tls.tls import mysql_ssl
from src.config.config import (
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
                    ssl=mysql_ssl(),
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
        ssl=mysql_ssl(),
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
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS devices (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    device_id VARCHAR(36) NOT NULL UNIQUE,
                    device_name VARCHAR(100) NOT NULL,
                    device_type VARCHAR(50) NOT NULL,
                    location VARCHAR(200),
                    firmware_version VARCHAR(20) DEFAULT '1.0.0',
                    tags JSON,
                    status VARCHAR(20) NOT NULL DEFAULT 'active',
                    owner_team VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    INDEX idx_device_type (device_type),
                    INDEX idx_status (status),
                    INDEX idx_owner_team (owner_team)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                """
            )
    finally:
        conn.close()


def insert_processed_message(data: dict) -> int:
    """
    Insert a message payload received by DB Updater (Process 4) into MariaDB Galera cluster.
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


def upsert_device(data: dict) -> int:
    """Insert or update a device row in the devices table.

    Uses ON DUPLICATE KEY UPDATE so repeated calls with the same device_id are idempotent.
    Returns the lastrowid (insert) or the existing row id (update — MariaDB returns 0 for no-op,
    or the new id for an actual update, but callers should treat the return as informational only).
    """
    max_retries = 3
    for attempt in range(max_retries):
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                sql = """
                    INSERT INTO devices (
                        device_id, device_name, device_type, location,
                        firmware_version, tags, status, owner_team
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        device_name      = VALUES(device_name),
                        device_type      = VALUES(device_type),
                        location         = VALUES(location),
                        firmware_version = VALUES(firmware_version),
                        tags             = VALUES(tags),
                        status           = VALUES(status),
                        owner_team       = VALUES(owner_team);
                """
                cursor.execute(
                    sql,
                    (
                        data.get("device_id"),
                        data.get("device_name"),
                        data.get("device_type"),
                        data.get("location"),
                        data.get("firmware_version", "1.0.0"),
                        json.dumps(data.get("tags")) if data.get("tags") is not None else None,
                        data.get("status", "active"),
                        data.get("owner_team"),
                    ),
                )
                return cursor.lastrowid
        except pymysql.err.OperationalError as op_err:
            if op_err.args[0] in (1213, 1205) and attempt < max_retries - 1:
                time.sleep(0.2 * (attempt + 1))
                continue
            raise
        finally:
            if conn:
                conn.close()


def soft_delete_device(device_id: str) -> int:
    """Set status='decommissioned' for the given device_id.

    Returns the number of rows affected (0 if device_id does not exist).
    """
    max_retries = 3
    for attempt in range(max_retries):
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute(
                    "UPDATE devices SET status = 'decommissioned' WHERE device_id = %s;",
                    (device_id,),
                )
                return cursor.rowcount
        except pymysql.err.OperationalError as op_err:
            if op_err.args[0] in (1213, 1205) and attempt < max_retries - 1:
                time.sleep(0.2 * (attempt + 1))
                continue
            raise
        finally:
            if conn:
                conn.close()


def query_devices(filters: dict) -> list:
    """Query devices by optional filters.

    Supported filter keys:
        device_type       — exact match (VARCHAR)
        status            — exact match (VARCHAR)
        owner_team        — exact match (VARCHAR)
        location_contains — substring search (LIKE '%x%')

    Returns a list of dicts (DictCursor rows).
    """
    conditions = []
    params = []

    if filters.get("device_type"):
        conditions.append("device_type = %s")
        params.append(filters["device_type"])
    if filters.get("status"):
        conditions.append("status = %s")
        params.append(filters["status"])
    if filters.get("owner_team"):
        conditions.append("owner_team = %s")
        params.append(filters["owner_team"])
    if filters.get("location_contains"):
        conditions.append("location LIKE %s")
        params.append(f"%{filters['location_contains']}%")

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    sql = f"SELECT * FROM devices {where_clause} ORDER BY created_at DESC LIMIT 1000;"

    max_retries = 3
    for attempt in range(max_retries):
        conn = None
        try:
            conn = get_db_connection()
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                return cursor.fetchall()
        except pymysql.err.OperationalError as op_err:
            if op_err.args[0] in (1213, 1205) and attempt < max_retries - 1:
                time.sleep(0.2 * (attempt + 1))
                continue
            raise
        finally:
            if conn:
                conn.close()
