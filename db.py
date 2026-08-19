import json
import pymysql
from pymysql.cursors import DictCursor
from config import (
    MARIADB_HOST,
    MARIADB_PORT,
    MARIADB_USER,
    MARIADB_PASSWORD,
    MARIADB_DB,
)


def get_db_connection():
    """Create and return a connection to MariaDB."""
    return pymysql.connect(
        host=MARIADB_HOST,
        port=MARIADB_PORT,
        user=MARIADB_USER,
        password=MARIADB_PASSWORD,
        database=MARIADB_DB,
        cursorclass=DictCursor,
        autocommit=True,
    )


def init_database():
    """Ensure database schema is created."""
    conn = pymysql.connect(
        host=MARIADB_HOST,
        port=MARIADB_PORT,
        user=MARIADB_USER,
        password=MARIADB_PASSWORD,
        autocommit=True,
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
    """Insert a message payload received by Process 4 into MariaDB."""
    conn = get_db_connection()
    try:
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
    finally:
        conn.close()
