CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR NOT NULL,
    courier_name VARCHAR NOT NULL,
    settlement_year INTEGER NOT NULL CHECK (settlement_year BETWEEN 2000 AND 2100),
    settlement_month INTEGER NOT NULL CHECK (settlement_month BETWEEN 1 AND 12),
    orders_count INTEGER NOT NULL CHECK (orders_count >= 0),
    orders_total_sum NUMERIC(14, 2) NOT NULL CHECK (orders_total_sum >= 0),
    rate_avg NUMERIC(3, 2) NOT NULL CHECK (rate_avg BETWEEN 1 AND 5),
    order_processing_fee NUMERIC(14, 2) NOT NULL CHECK (order_processing_fee >= 0),
    courier_order_sum NUMERIC(14, 2) NOT NULL CHECK (courier_order_sum >= 0),
    courier_tips_sum NUMERIC(14, 2) NOT NULL CHECK (courier_tips_sum >= 0),
    courier_reward_sum NUMERIC(14, 2) NOT NULL CHECK (courier_reward_sum >= 0),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT dm_courier_ledger_unique UNIQUE (courier_id, settlement_year, settlement_month)
);

COMMENT ON TABLE cdm.dm_courier_ledger IS 'Витрина выплат курьерам по месяцам';
COMMENT ON COLUMN cdm.dm_courier_ledger.id IS 'Уникальный идентификатор записи';
COMMENT ON COLUMN cdm.dm_courier_ledger.courier_id IS 'ID курьера';
COMMENT ON COLUMN cdm.dm_courier_ledger.courier_name IS 'ФИО курьера';
COMMENT ON COLUMN cdm.dm_courier_ledger.settlement_year IS 'Год отчетного периода';
COMMENT ON COLUMN cdm.dm_courier_ledger.settlement_month IS 'Месяц отчетного периода (1-12)';
COMMENT ON COLUMN cdm.dm_courier_ledger.orders_count IS 'Количество заказов за период';
COMMENT ON COLUMN cdm.dm_courier_ledger.orders_total_sum IS 'Общая стоимость заказов';
COMMENT ON COLUMN cdm.dm_courier_ledger.rate_avg IS 'Средний рейтинг курьера';
COMMENT ON COLUMN cdm.dm_courier_ledger.order_processing_fee IS 'Комиссия за обработку заказов (25%)';
COMMENT ON COLUMN cdm.dm_courier_ledger.courier_order_sum IS 'Сумма за доставленные заказы';
COMMENT ON COLUMN cdm.dm_courier_ledger.courier_tips_sum IS 'Сумма чаевых';
COMMENT ON COLUMN cdm.dm_courier_ledger.courier_reward_sum IS 'Итоговая сумма к выплате';
COMMENT ON COLUMN cdm.dm_courier_ledger.created_at IS 'Дата создания записи';
COMMENT ON COLUMN cdm.dm_courier_ledger.updated_at IS 'Дата обновления записи';

CREATE INDEX idx_dm_courier_ledger_courier_id ON cdm.dm_courier_ledger (courier_id);
CREATE INDEX idx_dm_courier_ledger_settlement_date ON cdm.dm_courier_ledger (settlement_year, settlement_month);
CREATE INDEX idx_dm_courier_ledger_courier_settlement ON cdm.dm_courier_ledger (courier_id, settlement_year, settlement_month);