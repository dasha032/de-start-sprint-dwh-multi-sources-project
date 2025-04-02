from datetime import datetime, date, time
from decimal import Decimal
from typing import List, Optional

from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class RawOrderObj(BaseModel):
    id: int
    object_value: str
    update_ts: datetime
    courier_id: str 
    delivery_id: str 
    delivery_ts: datetime  



class OrderObj(BaseModel):
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int
    courier_id: int
    delivery_id: int


class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, order_threshold: int) -> List[RawOrderObj]:
        with self._db.client().cursor(row_factory=class_row(RawOrderObj)) as cur:
            cur.execute(
                """
                WITH order_data AS (
                    SELECT 
                        o.id, 
                        o.object_value, 
                        o.update_ts,
                        d.delivery_id,
                        d.courier_id,
                        d.delivery_ts,
                        d.rate,
                        d.tip_sum
                    FROM stg.ordersystem_orders o
                    LEFT JOIN stg.couriersystem_deliveries d ON o.object_id = d.order_id
                    WHERE 
                        o.id > %(threshold)s AND
                        o.object_value::json->>'final_status' IN ('CLOSED', 'CANCELLED')
                    ORDER BY o.id ASC
                    LIMIT %(batch_limit)s
                )
                SELECT 
                    od.id,
                    od.object_value,
                    od.update_ts,
                    od.courier_id,
                    od.delivery_id,
                    od.delivery_ts,
                    od.rate,
                    od.tip_sum
                FROM order_data od
                LEFT JOIN stg.couriersystem_couriers c ON od.courier_id = c.id
                """,
                {
                    "threshold": order_threshold,
                    "batch_limit": 1500
                }
            )
            objs = cur.fetchall()
        return objs


class OrderDestRepository:

    def insert_order(self, conn: Connection, order: OrderObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_orders(
                    order_key, 
                    order_status, 
                    restaurant_id, 
                    timestamp_id, 
                    user_id,
                    courier_id,
                    delivery_id
                )
                VALUES (
                    %(order_key)s,
                    %(order_status)s,
                    %(restaurant_id)s,
                    %(timestamp_id)s,
                    %(user_id)s,
                    %(courier_id)s,
                    %(delivery_id)s
                )
                ON CONFLICT (order_key) DO UPDATE
                SET
                    order_status = EXCLUDED.order_status,
                    restaurant_id = EXCLUDED.restaurant_id,
                    timestamp_id = EXCLUDED.timestamp_id,
                    user_id = EXCLUDED.user_id;
                    courier_id = EXCLUDED.courier_id
                    delivery_id = EXCLUDED.delivery_id;
                """,
                order.dict()
            )


class OrderLoader:
    WF_KEY = "example_orders_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1500 # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OrdersOriginRepository(pg_dest)
        self.dds = OrderDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log


    def get_restaurant_id(self, conn: Connection, source_restaurant_id: str) -> Optional[int]:
        """Находит ID ресторана в dds.dm_restaurants по ID из источника"""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM dds.dm_restaurants WHERE restaurant_id = %s",
                [source_restaurant_id]
            )
            result = cur.fetchone()
            return result[0] if result else None

    def get_timestamp_id(self, conn: Connection, ts: datetime) -> Optional[int]:
        """Находит ID временной метки в dds.dm_timestamps"""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM dds.dm_timestamps WHERE ts = %s",
                [ts]
            )
            result = cur.fetchone()
            return result[0] if result else None

    def get_user_id(self, conn: Connection, source_user_id: str) -> Optional[int]:
        """Находит ID пользователя в dds.dm_users по ID из источника"""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM dds.dm_users WHERE user_id = %s",
                [source_user_id]
            )
            result = cur.fetchone()
            return result[0] if result else None
        
    def get_courier_id(self, conn: Connection, source_courier_id: str) -> Optional[int]:
        """Находит ID курьера в dds.dm_couriers по ID из источника"""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM dds.dm_couriers WHERE courier_id = %s",
                [source_courier_id]
            )
            result = cur.fetchone()
            return result[0] if result else None
        
        
    def get_delivery_id(self, conn: Connection, source_delivery_id: str) -> Optional[int]:
        """Находит ID доставки в dds.dm_deliveries по ID из источника"""
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM dds.dm_deliveries WHERE delivery_id = %s",
                [source_delivery_id]
            )
            result = cur.fetchone()
            return result[0] if result else None
        

    def parse_order_data(self, conn: Connection, raw_order: RawOrderObj):
        """Парсит заказ и находит все связанные ID"""
        try:
            data = str2json(raw_order.object_value)
            
            # Получаем все необходимые ID
            restaurant_id = self.get_restaurant_id(conn, data["restaurant"]["id"])
            timestamp_id = self.get_timestamp_id(conn, datetime.strptime(data["date"], "%Y-%m-%d %H:%M:%S"))
            user_id = self.get_user_id(conn, data["user"]["id"])
            courier_id = self.get_courier_id(conn, raw_order.courier_id)
            delivery_id = self.get_delivery_id(conn, raw_order.delivery_id)
            
            if not all([restaurant_id, timestamp_id, user_id]):
                self.log.error(f"Missing references for order {data['_id']}")
                return None
                
            return OrderObj(
                order_key=data["_id"],
                order_status=data["final_status"],
                restaurant_id=restaurant_id,
                timestamp_id=timestamp_id,
                user_id=user_id,
                courier_id=courier_id,
                delivery_id=delivery_id
            )
        except Exception as e:
            self.log.error(f"Failed to parse order: {e}")
            return None


    def load_orders(self):
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
            load_queue = self.origin.list_orders(last_loaded)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            
            for order in load_queue:
                order_obj = self.parse_order_data(conn, order)
                if order_obj:
                    self.dds.insert_order(conn, order_obj)
                
            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
