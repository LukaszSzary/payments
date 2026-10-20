CREATE TABLE IF NOT EXISTS processed_orders (
    order_id     VARCHAR(64) PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL
    );