from datetime import datetime
from decimal import Decimal

from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class RawDeliveryObj(BaseModel):
    delivery_id: str
    delivery_ts: datetime
    rate: int
    tip_sum: Decimal


class DeliveryObj(BaseModel):
    delivery_id: str
    delivery_ts: datetime
    rate: int
    tip_sum: Decimal




class DeliveryOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliverys(self, delivery_threshold: int) -> List[RawDeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(RawDeliveryObj)) as cur:
            cur.execute(
                """
                    SELECT delivery_id, delivery_ts, rate, tip_sum
                    FROM stg.couriersystem_deliveries
                    --WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    --ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    ; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": delivery_threshold,

                }
            )
            objs = cur.fetchall()
        return objs


class DeliveryDestRepository:

    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(delivery_id, delivery_ts, rate, tip_sum)
                    VALUES (%(delivery_id)s, %(delivery_ts)s,%(rate)s, %(tip_sum)s )
                    --ON CONFLICT (id) DO UPDATE
                    --SET
                        --delivery_id = EXCLUDED.delivery_id;
                """,
                {
                    "delivery_id": delivery.delivery_id,
                    "delivery_ts": delivery.delivery_ts,
                    "rate": delivery.rate,
                    "tip_sum": delivery.tip_sum,

                },
            )


class DeliveryLoader:
    WF_KEY = "example_deliveries_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1500 

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveryOriginRepository(pg_dest)
        self.dds = DeliveryDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            with conn.cursor() as cur:
                cur.execute(
                    """
                        WITH inserted_deliveries AS (
                            INSERT INTO dds.dm_deliveries (
                                delivery_id,
                                delivery_ts,
                                rate,
                                tip_sum
                            )
                            SELECT 
                                d.delivery_id,
                                d.delivery_ts,
                                d.rate,
                                d.tip_sum
                            FROM stg.couriersystem_deliveries d
                            WHERE d.delivery_ts > %(last_loaded_ts)s
                            ORDER BY d.delivery_ts
                            LIMIT %(batch_limit)s
                            ON CONFLICT (delivery_id) DO UPDATE SET
                                delivery_ts = EXCLUDED.delivery_ts,
                                rate = EXCLUDED.rate,
                                tip_sum = EXCLUDED.tip_sum
                            RETURNING delivery_ts
                        )
                        SELECT MAX(delivery_ts) FROM inserted_deliveries
                    """,
                    {
                        "last_loaded": last_loaded,
                        "batch_limit": self.BATCH_LIMIT
                    }
                )
                result = cur.fetchone()
                max_ts = result[0] if result[0] else last_loaded
            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max_ts
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
