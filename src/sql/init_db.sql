-- Initialize database and tables for FlowFirst pipeline
CREATE DATABASE IF NOT EXISTS flowfirst_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE flowfirst_db;

-- Processed Messages Table
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
