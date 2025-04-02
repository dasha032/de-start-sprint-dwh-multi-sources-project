import logging

import pendulum
from airflow.decorators import dag, task
from cdm.report_loader import SettlementReportLoader
from cdm.dm_courier_ledger_loader import CourierReportLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2025, 3, 25, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_cdm_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="report_load")
    def load_report():
        # создаем экземпляр класса, в котором реализована логика.
        report_loader = SettlementReportLoader(origin_pg_connect, dwh_pg_connect, log)
        report_loader.load_settlement_report()  # Вызываем функцию, которая перельет данные.

    # Задание для проекта
    @task(task_id="courier_report_load")
    def load_courier_report():
        # создаем экземпляр класса, в котором реализована логика.
        courier_report_loader = CourierReportLoader(origin_pg_connect, dwh_pg_connect, log)
        courier_report_loader.load_courier_report()  # Вызываем функцию, которая перельет данные.



    # Инициализируем объявленные таски.
    report_task = load_report()
    
    courier_report_task = load_courier_report()

    # Далее задаем последовательность выполнения тасков.
    [report_task, courier_report_task]


cdm_dag = sprint5_example_cdm_dag()
