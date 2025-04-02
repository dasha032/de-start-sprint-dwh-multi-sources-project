import logging

import pendulum
from airflow.decorators import dag, task
from dds.users_loader import UserLoader
from dds.restaurants_loader import RestaurantLoader
from dds.timestamps_loader import TimestampLoader
from dds.products_loader import ProductLoader
from dds.orders_loader import OrderLoader
from dds.fct_product_sales_loader import SaleLoader
from dds.couriers_loader import CourierLoader
from dds.deliveries_loader import DeliveryLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2025, 3, 25, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_dds_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="users_load")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()  # Вызываем функцию, которая перельет данные.

        # Таск для загрузки ресторанов
    @task(task_id="restaurants_load")
    def load_restaurants():
        restaurant_loader = RestaurantLoader(origin_pg_connect, dwh_pg_connect, log)
        restaurant_loader.load_restaurants()

    @task(task_id="timestamps_load")
    def load_timestamps():
        timestamps_loader = TimestampLoader(origin_pg_connect, dwh_pg_connect, log)
        timestamps_loader.load_timestamps()

    # Задание для проекта
    @task(task_id="couriers_load")
    def load_couriers():
        couriers_loader = CourierLoader(origin_pg_connect, dwh_pg_connect, log)
        couriers_loader.load_couriers()

    @task(task_id="deliveries_load")
    def load_deliveries():
        deliveries_loader = DeliveryLoader(origin_pg_connect, dwh_pg_connect, log)
        deliveries_loader.load_deliveries()        

    @task(task_id="products_load")
    def load_products():
        products_loader = ProductLoader(origin_pg_connect, dwh_pg_connect, log)
        products_loader.load_products()


    @task(task_id="orders_load")
    def load_orders():
        orders_loader = OrderLoader(origin_pg_connect, dwh_pg_connect, log)
        orders_loader.load_orders()

    @task(task_id="product_sales_load")
    def load_product_sales():
        product_sales_loader = SaleLoader(origin_pg_connect, dwh_pg_connect, log)
        product_sales_loader.load_sales()


    # Инициализируем объявленные таски.
    users_task = load_users()
    restaurants_task = load_restaurants()
    timestamps_task = load_timestamps()

    couriers_task = load_couriers()
    deliveries_task = load_deliveries()
    
    products_task = load_products()
    orders_task = load_orders()
    product_sales_task = load_product_sales()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    [users_task, restaurants_task, timestamps_task, couriers_task, deliveries_task] >> products_task >> orders_task >> product_sales_task # type: ignore


dds_dag = sprint5_example_dds_dag()
