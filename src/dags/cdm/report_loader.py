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

class SettlementReportRecord(BaseModel):
    restaurant_id: str
    restaurant_name: str
    settlement_date: date
    orders_count: int
    orders_total_sum: Decimal
    orders_bonus_payment_sum: Decimal
    orders_bonus_granted_sum: Decimal
    order_processing_fee: Decimal
    restaurant_reward_sum: Decimal


class SettlementReportOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def get_settlement_data(self, settlement_date: date) -> List[SettlementReportRecord]:
        """Получает агрегированные данные для отчета по дате из DDS слоя"""
        with self._db.client().cursor(row_factory=class_row(SettlementReportRecord)) as cur:
            cur.execute(
                """
                WITH closed_orders AS (
                    SELECT 
                        o.id as order_id,
                        r.id as restaurant_id,
                        r.restaurant_name,
                        t.ts::date as order_date,
                        SUM(fps.total_sum) as order_total_sum,
                        SUM(fps.bonus_payment) as order_bonus_payment,
                        SUM(fps.bonus_grant) as order_bonus_grant
                    FROM dds.fct_product_sales fps
                    JOIN dds.dm_orders o ON fps.order_id = o.id
                    JOIN dds.dm_restaurants r ON o.restaurant_id = r.id
                    JOIN dds.dm_timestamps t ON o.timestamp_id = t.id
                    WHERE o.order_status = 'CLOSED'
                    GROUP BY o.id, r.id, r.restaurant_name, t.ts::date
                )
                SELECT
                    restaurant_id,
                    restaurant_name,
                    order_date as settlement_date,
                    COUNT(order_id) as orders_count,
                    SUM(order_total_sum) as orders_total_sum,
                    SUM(order_bonus_payment) as orders_bonus_payment_sum,
                    SUM(order_bonus_grant) as orders_bonus_granted_sum,
                    SUM(order_total_sum) * 0.25 as order_processing_fee,
                    SUM(order_total_sum) - SUM(order_bonus_payment) - (SUM(order_total_sum) * 0.25) as restaurant_reward_sum
                FROM closed_orders
                --WHERE order_date = %s
                GROUP BY restaurant_id, restaurant_name, order_date
                """,
                [settlement_date]
            )
            return cur.fetchall()

class SettlementReportDestRepository:

    def insert_settlement_report(self, conn: Connection, report: SettlementReportRecord) -> None:
        """Вставляет или обновляет запись в отчете о расчетах"""
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cdm.dm_settlement_report(
                    restaurant_id,
                    restaurant_name,
                    settlement_date,
                    orders_count,
                    orders_total_sum,
                    orders_bonus_payment_sum,
                    orders_bonus_granted_sum,
                    order_processing_fee,
                    restaurant_reward_sum
                )
                VALUES (
                    %(restaurant_id)s,
                    %(restaurant_name)s,
                    %(settlement_date)s,
                    %(orders_count)s,
                    %(orders_total_sum)s,
                    %(orders_bonus_payment_sum)s,
                    %(orders_bonus_granted_sum)s,
                    %(order_processing_fee)s,
                    %(restaurant_reward_sum)s
                )
                ON CONFLICT (restaurant_id, settlement_date) DO UPDATE SET
                    restaurant_name = EXCLUDED.restaurant_name,
                    orders_count = EXCLUDED.orders_count,
                    orders_total_sum = EXCLUDED.orders_total_sum,
                    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                    order_processing_fee = EXCLUDED.order_processing_fee,
                    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum
                """,
                report.dict()
            )

class SettlementReportLoader:
    WF_KEY = "settlement_report_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_date"

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = SettlementReportOriginRepository(pg_dest)
        self.cdm = SettlementReportDestRepository()
        self.settings_repository = CdmEtlSettingsRepository()
        self.log = log

    def load_settlement_report(self, process_date: Optional[date] = None):
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
            transactions = self.origin.get_settlement_data(last_loaded)
            
            if not transactions:
                self.log.info("No new bonus transactions to process")
                return

            for record in transactions:
                self.cdm.insert_settlement_report(conn, record)

            # Обновляем прогресс
            # wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(t. for t in transactions)
            # self.settings_repository.save_setting(
            #     conn,
            #     self.WF_KEY,
            #     json2str(wf_setting.workflow_settings)
            # )


            # self.log.info(f"Last processed ID: {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")

