
import logging
import pendulum
from airflow.decorators import dag, task,task_group
from lib import ConnectionBuilder
from psycopg.rows import class_row

from dds.loader import UserLoader,RestaurantLoader,TimestampLoader,ProductLoader,OrderLoader
from dds.dds_group import dds_load_group


log = logging.getLogger(__name__)

@dag(
    schedule_interval=None,  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example','test'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True, # Остановлен/запущен при появлении. Сразу запущен.
    dag_id = 'dds_layer_dag'
)
def dds_load_dag():
    dds_load_group()
    
dag = dds_load_dag()


if __name__ == "__main__":
   dag.test()