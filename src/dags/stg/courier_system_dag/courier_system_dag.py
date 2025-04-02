import logging
import requests

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from stg.courier_system_dag.couriers_loader import CourierLoader
from stg.courier_system_dag.deliveries_loader import DeliveryLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)
nickname = 'daria_kolbasina'
cohort = '3'
api_token = Variable.get('X_API_KEY')

headers = {
    "X-API-KEY": api_token,
    "X-Nickname": nickname,
    "X-Cohort": cohort
}

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2025, 3, 31, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)

def sprint5_stg_courier_system_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    api_endpoint = Variable.get("API_ENDPOINT")

    @task(task_id="couriers_load")
    def load_couriers():
        loader = CourierLoader(api_endpoint, headers, dwh_pg_connect, log)
        loader.load_couriers()

    @task(task_id="deliveries_load") 
    def load_deliveries(**kwargs):
        loader = DeliveryLoader(api_endpoint, headers, dwh_pg_connect, log)
        loader.load_deliveries()

    # Инициализируем объявленные таски.
    couriers_dict = load_couriers()
    deliveries_dict = load_deliveries()

    # Далее задаем последовательность выполнения тасков.
    [couriers_dict, deliveries_dict]  # type: ignore


stg_courier_system_dag = sprint5_stg_courier_system_dag()
