from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import List, Optional
from logging import Logger

from cdm.cdm_settings_repository import EtlSetting, CdmEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class CourierReportRecord(BaseModel):
    courier_id: str
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: Decimal
    rate_avg: Decimal
    order_processing_fee: Decimal
    courier_order_sum: Decimal
    courier_tips_sum: Decimal
    courier_reward_sum: Decimal


class CourierReportOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get_courier_data(self, last_date: date, limit: int) -> List[CourierReportRecord]:
        with self._db.client().cursor(row_factory=class_row(CourierReportRecord)) as cur:
            cur.execute(
                """
                WITH courier_stats AS (
                    SELECT
                        o.courier_id,
                        c.courier_name as courier_name,
                        EXTRACT(YEAR FROM t.ts)::int as settlement_year,
                        EXTRACT(MONTH FROM t.ts)::int as settlement_month,
                        COUNT(DISTINCT o.order_key) as orders_count,
                        SUM(fps.total_sum) as orders_total_sum,
                        AVG(d.rate) as rate_avg,
                        SUM(d.tip_sum) as courier_tips_sum
                    FROM dds.dm_orders o
                    JOIN dds.fct_product_sales fps ON o.id::text = fps.order_id::text
                    JOIN dds.dm_deliveries d ON o.delivery_id::text = d.delivery_id::text
                    JOIN dds.dm_couriers c ON o.courier_id::text = c.courier_id::text
                    JOIN dds.dm_timestamps t ON o.timestamp_id = t.id
                    WHERE o.order_status = 'CLOSED'
                        --AND t.ts > %(last_date)s::timestamp                     
                    GROUP BY o.courier_id, c.courier_name,  
                         EXTRACT(YEAR FROM t.ts), 
                         EXTRACT(MONTH FROM t.ts)
                )
                SELECT
                    courier_id,
                    courier_name,
                    settlement_year::int,
                    settlement_month::int,
                    orders_count,
                    orders_total_sum,
                    rate_avg,
                    orders_total_sum * 0.25 as order_processing_fee,
                    CASE
                        WHEN rate_avg < 4 THEN GREATEST(orders_count * 100, orders_total_sum * 0.05)
                        WHEN rate_avg < 4.5 THEN GREATEST(orders_count * 150, orders_total_sum * 0.07)
                        WHEN rate_avg < 4.9 THEN GREATEST(orders_count * 175, orders_total_sum * 0.08)
                        ELSE GREATEST(orders_count * 200, orders_total_sum * 0.10)
                    END as courier_order_sum,
                    courier_tips_sum,
                    CASE
                        WHEN rate_avg < 4 THEN GREATEST(orders_count * 100, orders_total_sum * 0.05)
                        WHEN rate_avg < 4.5 THEN GREATEST(orders_count * 150, orders_total_sum * 0.07)
                        WHEN rate_avg < 4.9 THEN GREATEST(orders_count * 175, orders_total_sum * 0.08)
                        ELSE GREATEST(orders_count * 200, orders_total_sum * 0.10)
                    END + (courier_tips_sum * 0.95) as courier_reward_sum
                FROM courier_stats
                LIMIT %(limit)s
                """,
                    {
                "last_date": last_date,
                "limit": limit
            }
            )
            return cur.fetchall()

class CourierReportDestRepository:

    def insert_courier_report(self, conn: Connection, report: CourierReportRecord) -> None:
        """Вставляет или обновляет запись в отчете о расчетах"""
        with conn.cursor() as cur:
            cur.execute(
                """
                        INSERT INTO cdm.dm_courier_ledger (
                            courier_id,
                            courier_name,
                            settlement_year,
                            settlement_month,
                            orders_count,
                            orders_total_sum,
                            rate_avg,
                            order_processing_fee,
                            courier_order_sum,
                            courier_tips_sum,
                            courier_reward_sum
                        )
                        VALUES (
                            %(courier_id)s,
                            %(courier_name)s,
                            %(settlement_year)s,
                            %(settlement_month)s,
                            %(orders_count)s,
                            %(orders_total_sum)s,
                            %(rate_avg)s,
                            %(order_processing_fee)s,
                            %(courier_order_sum)s,
                            %(courier_tips_sum)s,
                            %(courier_reward_sum)s
                        )
                        ON CONFLICT (courier_id, settlement_year, settlement_month) 
                        DO UPDATE SET
                            orders_count = EXCLUDED.orders_count,
                            orders_total_sum = EXCLUDED.orders_total_sum,
                            rate_avg = EXCLUDED.rate_avg,
                            order_processing_fee = EXCLUDED.order_processing_fee,
                            courier_order_sum = EXCLUDED.courier_order_sum,
                            courier_tips_sum = EXCLUDED.courier_tips_sum,
                            courier_reward_sum = EXCLUDED.courier_reward_sum
                """,
                report.dict()
            )

class CourierReportLoader:
    WF_KEY = "courier_report_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_date"
    BATCH_SIZE = 1000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CourierReportOriginRepository(pg_dest)
        self.cdm = CourierReportDestRepository()
        self.settings_repository = CdmEtlSettingsRepository()
        self.log = log

    def load_courier_report(self, process_date: Optional[date] = None):
        """Загружает отчет о расчетах за указанную дату или продолжает инкрементальную загрузку"""
        with self.pg_dest.connection() as conn:
            # Загружаем настройки workflow
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1}
                )



            # Получаем транзакции для обработки
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            transactions = self.origin.get_courier_data(last_loaded, self.BATCH_SIZE)
            
            if not transactions:
                self.log.info("No new transactions to process")
                return

            for record in transactions:
                self.cdm.insert_courier_report(conn, record)

            # Обновляем прогресс
            # wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(t. for t in transactions)
            # self.settings_repository.save_setting(
            #     conn,
            #     self.WF_KEY,
            #     json2str(wf_setting.workflow_settings)
            # )


            # self.log.info(f"Last processed ID: {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

