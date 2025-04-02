from datetime import datetime, date, time

from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class RawTimestampObj(BaseModel):
    id: int
    object_value: str

class TimestampObj(BaseModel):
    ts: datetime
    year: int
    month: int
    day: int
    date: date
    time: time




class TimestampsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(self, timestamp_threshold: int) -> List[RawTimestampObj]:
        with self._db.client().cursor(row_factory=class_row(RawTimestampObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_value
                    FROM stg.ordersystem_orders
                    WHERE 
                    id > %(threshold)s AND 
                    object_value::json->>'final_status' IN ('CLOSED', 'CANCELLED') --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    ; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": timestamp_threshold,

                }
            )
            objs = cur.fetchall()
        return objs


class TimestampDestRepository:

    def insert_timestamp(self, conn: Connection, timestamp: TimestampObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, date, time)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s);
                """,
                {
                    "ts": timestamp.ts,
                    "year": timestamp.year,
                    "month": timestamp.month,
                    "day": timestamp.day,
                    "date": timestamp.date,
                    "time": timestamp.time
                },
            )


class TimestampLoader:
    WF_KEY = "example_timestamps_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1500 # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = TimestampsOriginRepository(pg_dest)
        self.dds = TimestampDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def parse_order_data(self, object_value: str):
        """Парсим JSON из object_value и извлекаем данные пользователя"""

        data = str2json(object_value)
        if data["final_status"] in ("CLOSED", "CANCELLED"):
            return datetime.strptime(data["date"], "%Y-%m-%d %H:%M:%S")


    def load_timestamps(self):
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
            load_queue = self.origin.list_timestamps(last_loaded)
            self.log.info(f"Found {len(load_queue)} timestamps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for raw_timestamp in load_queue:
                dt = self.parse_order_data(raw_timestamp.object_value)
                timestamp = TimestampObj(

                        ts=dt,
                        year=dt.year,
                        month=dt.month,
                        day=dt.day,
                        date=dt.date(),
                        time=dt.time()
                    )
                self.dds.insert_timestamp(conn, timestamp)


            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
