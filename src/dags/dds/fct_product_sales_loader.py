from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Dict
from logging import Logger

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from pydantic import BaseModel

class BonusTransaction(BaseModel):
    id: int
    event_value: str
    order_id: str
    event_ts: datetime

class SaleObj(BaseModel):
    product_id: int
    order_id: int
    count: int
    price: Decimal
    total_sum: Decimal
    bonus_payment: Decimal
    bonus_grant: Decimal

class BonusRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get_bonus_transactions(self, last_loaded_id: int, limit: int) -> List[BonusTransaction]:
        """Получает бонусные транзакции для обработки"""
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT 
                    id,
                    event_value,
                    (event_value::json->>'order_id') as order_id,
                    event_ts
                FROM stg.bonussystem_events
                WHERE event_type = 'bonus_transaction'
                  AND id > %(last_loaded_id)s
                ORDER BY id
                --LIMIT %(limit)s;
                """,
                {
                    "last_loaded_id": last_loaded_id,
                    "limit": limit
                }
            )
            return [
                BonusTransaction(
                    id=row[0],
                    event_value=row[1],
                    order_id=row[2],
                    event_ts=row[3]
                )
                for row in cur.fetchall()
            ]

class SaleDestRepository:
    def insert_sale(self, conn: Connection, sale: SaleObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.fct_product_sales(
                    product_id, 
                    order_id, 
                    count, 
                    price, 
                    total_sum, 
                    bonus_payment, 
                    bonus_grant
                )
                VALUES (
                    %(product_id)s,
                    %(order_id)s,
                    %(count)s,
                    %(price)s,
                    %(total_sum)s,
                    %(bonus_payment)s,
                    %(bonus_grant)s
                )
                ON CONFLICT (product_id, order_id) DO UPDATE SET
                    count = EXCLUDED.count,
                    price = EXCLUDED.price,
                    total_sum = EXCLUDED.total_sum,
                    bonus_payment = EXCLUDED.bonus_payment,
                    bonus_grant = EXCLUDED.bonus_grant;
                """,
                sale.dict()
            )

class SaleLoader:
    WF_KEY = "fct_product_sales_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1500

    def __init__(self, pg_origin: PgConnect, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.bonus_repo = BonusRepository(pg)
        self.dds = SaleDestRepository()
        self.settings_repo = DdsEtlSettingsRepository()
        self.log = log

    def get_product_id(self, conn: Connection, product_key: str, order_ts: datetime) -> Optional[int]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id 
                FROM dds.dm_products 
                WHERE product_id = %(product_key)s
                  AND active_from <= %(order_ts)s
                  AND active_to > %(order_ts)s
                """,
                {
                    "product_key": product_key,
                    "order_ts": order_ts
                }
            )
            result = cur.fetchone()
            return result[0] if result else None

    def get_order_id(self, conn: Connection, order_key: str) -> Optional[int]:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id 
                FROM dds.dm_orders 
                WHERE order_key = %(order_key)s
                """,
                {"order_key": order_key}
            )
            result = cur.fetchone()
            return result[0] if result else None

    def parse_transaction(self, conn: Connection, transaction: BonusTransaction) -> List[SaleObj]:
        try:
            data = str2json(transaction.event_value)
            order_id = self.get_order_id(conn, transaction.order_id)
            
            if not order_id:
                self.log.error(f"Order {transaction.order_id} not found in dm_orders")
                return []

            sales = []
            for product in data.get('product_payments', []):
                product_id = self.get_product_id(
                    conn,
                    product['product_id'],
                    transaction.event_ts
                )
                
                if not product_id:
                    self.log.error(f"Product {product['product_id']} not found for order {transaction.order_id}")
                    continue

                sales.append(SaleObj(
                    product_id=product_id,
                    order_id=order_id,
                    count=product['quantity'],
                    price=Decimal(product['price']).quantize(Decimal('0.00000')),
                    total_sum=Decimal(product['product_cost']).quantize(Decimal('0.00000')),
                    bonus_payment=Decimal(product['bonus_payment']).quantize(Decimal('0.00000')),
                    bonus_grant=Decimal(product['bonus_grant']).quantize(Decimal('0.00000'))
                ))
            
            return sales
        
        except Exception as e:
            self.log.error(f"Error parsing transaction {transaction.id}: {str(e)}")
            return []

    def load_sales(self):
        with self.pg.connection() as conn:
            wf_setting = self.settings_repo.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            transactions = self.bonus_repo.get_bonus_transactions(last_loaded, self.BATCH_LIMIT)
            
            if not transactions:
                self.log.info("No new bonus transactions to process")
                return

            # Обрабатываем каждую транзакцию
            processed_count = 0
            for transaction in transactions:
                sales = self.parse_transaction(conn, transaction)
                for sale in sales:
                    self.dds.insert_sale(conn, sale)
                    processed_count += 1

            # Обновляем прогресс
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(t.id for t in transactions)
            self.settings_repo.save_setting(
                conn,
                self.WF_KEY,
                json2str(wf_setting.workflow_settings)
            )

            self.log.info(f"Processed {processed_count} sales from {len(transactions)} transactions")
            self.log.info(f"Last processed ID: {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")