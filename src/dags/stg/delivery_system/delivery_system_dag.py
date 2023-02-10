

import logging
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder

from stg.delivery_system.loader import DeliverySystemDestRep, DeliverySystemOrigin
from airflow.providers.http.hooks.http import HttpHook
from stg.delivery_system.delivery_system_group import delivery_task_group

log = logging.getLogger(__name__)

APIs = ['couriers','deliveries','restaurants']

@dag(
    schedule_interval=None, 
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'Postgre','test'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True, # Остановлен/запущен при появлении. Сразу запущен.
    dag_id = 'delivery_system_load'
)
def project_stg_delivery_system_ranks_dag():
    delivery_task_group()
    
stg_delivery_system_ranks_dag = project_stg_delivery_system_ranks_dag()

if __name__ == "__main__":
   stg_delivery_system_ranks_dag.test()

