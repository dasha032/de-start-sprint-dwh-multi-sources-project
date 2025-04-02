# Проектирование витрины выплат курьерам (dm_courier_ledger)

## 1. Поля витрины

| Поле | Тип | Описание | Источник данных |
|------|-----|----------|----------------|
| id | SERIAL | Уникальный идентификатор записи | Автоинкремент |
| courier_id | VARCHAR | ID курьера | dds.dm_couriers |
| courier_name | VARCHAR | ФИО курьера | dds.dm_couriers |
| settlement_year | INTEGER | Год отчетного периода | Из dm_timestamps |
| settlement_month | INTEGER | Месяц отчетного периода (1-12) | Из dm_timestamps |
| orders_count | INTEGER | Количество доставленных заказов | Агрегация delivery_id |
| orders_total_sum | NUMERIC(14,2) | Общая сумма заказов | dds.dm_deliveries |
| rate_avg | NUMERIC(3,2) | Средний рейтинг курьера | dds.dm_deliveries |
| order_processing_fee | NUMERIC(14,2) | Комиссия за обработку (25%) | Расчет: orders_total_sum * 0.25 |
| courier_order_sum | NUMERIC(14,2) | Сумма за доставку | Расчет по рейтингу |
| courier_tips_sum | NUMERIC(14,2) | Сумма чаевых | dds.dm_deliveries|
| courier_reward_sum | NUMERIC(14,2) | Итоговая выплата | Расчет: courier_order_sum + courier_tips_sum * 0.95 |

## 2. Таблицы DDS слоя

### Существующие таблицы:
- `dds.dm_orders` (есть)
  - order_id, order_status, timestamp_id
- `dds.dm_timestamps` (есть)
  - id, ts, year, month, day

### Необходимые новые таблицы:
1. **dds.dm_couriers** (нужно создать)
   - id (PK)
   - courier_id 
   - courier_name

2. **dds.dm_deliveries** (нужно создать)
   - id (PK)
   - delivery_id
   - delivery_ts
   - rate
   - tip_sum


## 3. Сущности для загрузки из API

### Из API /couriers (GET):
```json
{
  "_id": "courier_id",
  "name": "courier_name"
}
```
### Из API /deliveries (GET):
```json
{
  "order_id": "order_id",
  "delivery_id": "delivery_id",
  "courier_id": "courier_id",
  "delivery_ts": "delivery_ts",
  "rate": "rate",
  "tip_sum": "tip_sum"
}```