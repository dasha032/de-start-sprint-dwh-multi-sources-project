from logging import Logger
from typing import List
import requests

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel, Field

class CourierObj(BaseModel):
    id: str = Field(..., alias="_id")
    name: str

    class Config:
        allow_population_by_field_name = True

class CouriersApiRepository:
    def __init__(self, api_endpoint: str, headers: dict) -> None:
        self.api_endpoint = api_endpoint
        self.headers = headers

    def list_couriers(self) -> List[CourierObj]:
        url = f'https://{self.api_endpoint}/couriers'
        
       
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        
        return [CourierObj(**courier) for courier in response.json()]

class CourierDestRepository:
    def insert_courier(self, conn: Connection, courier: CourierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.couriersystem_couriers(id, name)
                VALUES (%(id)s, %(name)s)
                ON CONFLICT (id) DO UPDATE
                SET
                    name = EXCLUDED.name;
                """,
                {
                    "id": courier.id,
                    "name": courier.name
                },
            )

class CourierLoader:
    WF_KEY = "couriers_api_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, api_endpoint: str, headers: dict, pg_dest: PgConnect, log) -> None:
        self.pg_dest = pg_dest
        self.api = CouriersApiRepository(api_endpoint, headers)
        self.stg = CourierDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: ""})

            load_queue = self.api.list_couriers()
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for courier in load_queue:
                self.stg.insert_courier(conn, courier)

            # Сохраняем последний загруженный ID
            last_id = max([courier.id for courier in load_queue])
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_id
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")