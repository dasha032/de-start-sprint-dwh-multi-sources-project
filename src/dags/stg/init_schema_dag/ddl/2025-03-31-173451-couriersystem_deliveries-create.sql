CREATE TABLE IF NOT EXISTS stg.couriersystem_deliveries (
        order_id VARCHAR,
        delivery_id VARCHAR,
        courier_id VARCHAR NOT NULL,
        delivery_ts TIMESTAMP,
        rate INTEGER,
        tip_sum NUMERIC(14, 2),
        PRIMARY KEY (order_id, delivery_id)
);