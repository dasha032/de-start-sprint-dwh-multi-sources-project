from datetime import datetime, date, time
from decimal import Decimal

from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class RawProductObj(BaseModel):
    id: int
    object_value: str
    update_ts: datetime
    restaurant_id: int

class ProductObj(BaseModel):
    restaurant_id: int
    product_id: str
    product_name: str
    product_price: Decimal
    active_from: datetime
    active_to: datetime = datetime(2099, 12, 31)




class ProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, product_threshold: int) -> List[RawProductObj]:
        with self._db.client().cursor(row_factory=class_row(RawProductObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        o.id,
                        o.object_value,
                        o.update_ts,
                        r.id as restaurant_id
                    FROM stg.ordersystem_orders o
                    JOIN dds.dm_restaurants r ON r.restaurant_id = (o.object_value::json->'restaurant'->>'id')::varchar
                    WHERE o.id > %(threshold)s
                """, {
                    "threshold": product_threshold,

                }
            )
            objs = cur.fetchall()
        return objs


class ProductDestRepository:

    def insert_product(self, conn: Connection, product: ProductObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_products(
                    product_id, 
                    product_name, 
                    product_price, 
                    active_from, 
                    active_to,
                    restaurant_id
                )
                VALUES (
                    %(product_id)s,
                    %(product_name)s,
                    %(product_price)s,
                    %(active_from)s,
                    %(active_to)s,
                    %(restaurant_id)s
                )
                ON CONFLICT (product_id) DO UPDATE
                SET
                    product_name = EXCLUDED.product_name,
                    product_price = EXCLUDED.product_price,
                    active_to = EXCLUDED.active_to,
                    restaurant_id = EXCLUDED.restaurant_id;
                """,
                {
                    "product_id": product.product_id,
                    "product_name": product.product_name,
                    "product_price": product.product_price,
                    "active_from": product.active_from,
                    "active_to": product.active_to,
                    "restaurant_id": product.restaurant_id
                },
            )


class ProductLoader:
    WF_KEY = "example_products_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1500 # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ProductsOriginRepository(pg_dest)
        self.dds = ProductDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def parse_product_data(self, object_value: str, restaurant_id: int, update_ts: datetime):
        """Парсим JSON из object_value и извлекаем данные пользователя"""
        products = []

        for item in object_value["order_items"]:
            product = ProductObj(
                restaurant_id=restaurant_id,
                product_id=item["id"],
                product_name=item["name"],
                product_price=Decimal(item["price"]),
                active_from=update_ts
            )
            products.append(product)
        return products


    def load_products(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_products(last_loaded)
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            
            for order in load_queue:
                order_data = str2json(order.object_value)
                products = self.parse_product_data(
                    order_data,
                    order.restaurant_id,
                    order.update_ts
                )
                for product in products:
                    self.dds.insert_product(conn, product)
                

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
