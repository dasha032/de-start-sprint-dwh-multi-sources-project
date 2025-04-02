from datetime import datetime

from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class RawCourierObj(BaseModel):
    id: str
    name: str


class CourierObj(BaseModel):
    courier_id: str
    courier_name: str




class CouriersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers(self, courier_threshold: int) -> List[RawCourierObj]:
        with self._db.client().cursor(row_factory=class_row(RawCourierObj)) as cur:
            cur.execute(
                """
                    SELECT id, name
                    FROM stg.couriersystem_couriers
                    --WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    --ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    ; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": courier_threshold,

                }
            )
            objs = cur.fetchall()
        return objs


class CourierDestRepository:

    def insert_courier(self, conn: Connection, courier: CourierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(courier_id, courier_name)
                    VALUES (%(courier_id)s, %(courier_name)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        courier_name = EXCLUDED.courier_name;
                """,
                {
                    "courier_id": courier.courier_id,
                    "courier_name": courier.courier_name

                },
            )


class CourierLoader:
    WF_KEY = "example_couriers_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1500 

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CouriersOriginRepository(pg_dest)
        self.dds = CourierDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
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
                        WITH inserted_couriers AS (
                            INSERT INTO dds.dm_couriers (
                                courier_id,
                                courier_name
                            )
                            SELECT 
                                c.id,
                                c.name
                            FROM stg.couriersystem_couriers c
                            WHERE c.id > %(last_loaded)s
                            ORDER BY c.id
                            LIMIT %(batch_limit)s
                            ON CONFLICT (courier_id) DO UPDATE SET
                                courier_name = EXCLUDED.courier_name
                            RETURNING courier_id
                        )
                        SELECT MAX(courier_id) FROM inserted_couriers
                    """,
                    {
                        "last_loaded": last_loaded,
                        "batch_limit": self.BATCH_LIMIT
                    }
                )
                result = cur.fetchone()
                max_id = result[0] if result[0] else last_loaded

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max_id
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
