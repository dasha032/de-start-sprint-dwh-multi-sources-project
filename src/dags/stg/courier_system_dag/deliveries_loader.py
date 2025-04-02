from datetime import datetime, timedelta
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

    def list_deliveries(
    self,
    last_loaded_ts: Optional[str] = None,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    limit: int = 100,
    offset: int = 0
) -> List[DeliveryObj]
        url = f'https://{self.api_endpoint}/deliveries'
        params = {
        'limit': limit,
        'offset': offset,
        'sort_field': 'date',
        'sort_direction': 'asc'
    }
        
        if last_loaded_ts:
            dt = datetime.strptime(last_loaded_ts, '%Y-%m-%d %H:%M:%S.%f')
            params['from'] = dt.strftime('%Y-%m-%d %H:%M:%S')
        

        if from_date:
            params['from'] = from_date.strftime('%Y-%m-%d %H:%M:%S')
        if to_date:
            params['to'] = to_date.strftime('%Y-%m-%d %H:%M:%S')
        
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        
        deliveries = response.json()
        
        return [DeliveryObj(**delivery) for delivery in deliveries]

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

    def load_deliveries(self, execution_date=None):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_TS_KEY: None})

            last_loaded_ts = wf_setting.workflow_settings.get(self.LAST_LOADED_TS_KEY)
            if execution_date:
                from_date = execution_date
                to_date = execution_date + timedelta(days=1)
                self.log.info(f"Loading deliveries for date: {from_date}")
            else:
                from_date = None
                to_date = None
                self.log.info("Loading all new deliveries without date filter")
                        
            offset = 0
            total_loaded = 0
            while True:
                load_queue = self.api.list_deliveries(
                    from_date=from_date,
                    to_date=to_date,
                    last_loaded_ts=last_loaded_ts,
                    limit=self.BATCH_LIMIT,
                    offset=offset
                )
                if not load_queue:
                    self.log.info(f"No more deliveries found for {from_date}")
                    break

                self.log.info(f"Loaded batch of {len(load_queue)} deliveries (offset {offset})")

                for raw_delivery in load_queue:
                    delivery = DeliveryObj(
                        order_id=raw_delivery.order_id,
                        delivery_id=raw_delivery.delivery_id,
                        delivery_ts=raw_delivery.delivery_ts,
                        rate=raw_delivery.rate,
                        tip_sum=raw_delivery.tip_sum
                )
                    self.stg.insert_delivery(conn, delivery)

                last_loaded_ts = max(d.delivery_ts for d in load_queue)
                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_loaded_ts.isoformat()
                self.settings_repository.save_setting(conn, self.WF_KEY, json2str(wf_setting.workflow_settings))

                offset += len(load_queue)
                total_loaded += len(load_queue)

                conn.commit()

            self.log.info(f"Total loaded for {from_date}: {total_loaded} deliveries")
