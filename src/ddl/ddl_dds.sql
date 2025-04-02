ALTER TABLE dds.dm_orders ADD COLUMN courier_id int;

ALTER TABLE dds.dm_orders ADD COLUMN delivery_id int;

CREATE INDEX idx_dm_orders_courier_id ON dds.dm_orders (courier_id);

CREATE TABLE IF NOT EXISTS dds.dm_couriers (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR NOT NULL UNIQUE,
    courier_name VARCHAR NOT NULL
);


ALTER TABLE dds.dm_orders ADD CONSTRAINT fk_dm_orders_courier_id FOREIGN KEY(courier_id) REFERENCES dds.dm_couriers(id);


CREATE TABLE IF NOT EXISTS dds.dm_deliveries (
    id SERIAL PRIMARY KEY,
    delivery_id VARCHAR NOT NULL UNIQUE,
    delivery_ts TIMESTAMP NOT NULL,
    rate INTEGER CHECK (rate BETWEEN 1 AND 5),
    tip_sum NUMERIC(14,2) DEFAULT 0
);

ALTER TABLE dds.dm_orders ADD CONSTRAINT fk_dm_orders_delivery_id FOREIGN KEY(delivery_id) REFERENCES dds.dm_deliveries(id);


