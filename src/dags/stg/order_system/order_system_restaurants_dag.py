import logging
import pendulum
from airflow.decorators import dag, task,task_group
from airflow.models.variable import Variable
from stg.order_system.pg_saver import PgSaver
from stg.order_system.loader import RestaurantLoader, UserLoader, OrderLoader
from stg.order_system.reader import RestaurantReader, UserReader,OrderReader
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  
from stg.order_system.order_system_restaurants_group import generator

from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'mongo', 'stg', 'origin','test'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True,  # Остановлен/запущен при появлении. Сразу запущен.
    dag_id = "order_system_load"
)
def sprint5_example_stg_order_system_restaurants():
    generator()
        
order_stg_dag = sprint5_example_stg_order_system_restaurants()  # noqa
