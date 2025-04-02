from datetime import datetime
from logging import Logger
from typing import List
import requests

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class DeliveryObj(BaseModel):
    order_id: str
    delivery_id: str
    courier_id: str
    delivery_ts: str
    rate: int
    tip_sum: float

class DeliveriesApiRepository:
    def __init__(self, api_endpoint: str, headers: dict) -> None:
        self.api_endpoint = api_endpoint
        self.headers = headers

    def list_deliveries(self, last_loaded_ts: str = None) -> List[DeliveryObj]:
        url = f'https://{self.api_endpoint}/deliveries'
        params = {'limit': 100, 'sort_field': 'date', 'sort_direction': 'asc'}
        
        if last_loaded_ts:
            dt = datetime.strptime(last_loaded_ts, '%Y-%m-%d %H:%M:%S.%f')
            params['from'] = dt.strftime('%Y-%m-%d %H:%M:%S')
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        
        return [DeliveryObj(**delivery) for delivery in response.json()]

class DeliveryDestRepository:
    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.couriersystem_deliveries(
                    order_id, delivery_id, courier_id, 
                    delivery_ts, rate, tip_sum
                )
                VALUES (%(order_id)s, %(delivery_id)s, %(courier_id)s, 
                        %(delivery_ts)s, %(rate)s, %(tip_sum)s)
                ON CONFLICT (order_id, delivery_id) DO UPDATE
                SET
                    courier_id = EXCLUDED.courier_id,
                    delivery_ts = EXCLUDED.delivery_ts,
                    rate = EXCLUDED.rate,
                    tip_sum = EXCLUDED.tip_sum;
                """,
                delivery.dict(),
            )

class DeliveryLoader:
    WF_KEY = "deliveries_api_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, api_endpoint: str, headers: dict, pg_dest: PgConnect, log) -> None:
        self.pg_dest = pg_dest
        self.api = DeliveriesApiRepository(api_endpoint, headers)
        self.stg = DeliveryDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_TS_KEY: None})

            last_loaded_ts = wf_setting.workflow_settings.get(self.LAST_LOADED_TS_KEY)
            load_queue = self.api.list_deliveries(last_loaded_ts)
            
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for delivery in load_queue:
                self.stg.insert_delivery(conn, delivery)

            # Сохраняем последнюю временную метку
            if load_queue:
                last_ts = max([delivery.delivery_ts for delivery in load_queue])
                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_ts
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

                self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")