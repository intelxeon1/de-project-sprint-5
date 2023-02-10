

import logging
import pendulum
from airflow.decorators import dag, task, task_group
from lib import ConnectionBuilder
from psycopg.rows import class_row
from pydantic import BaseModel
from stg.bonus_system.event_loader import EventLoader
from stg.bonus_system.ranks_loader import RankLoader
from stg.bonus_system.user_loader import UserLoader
from airflow.utils.task_group import TaskGroup
from stg.bonus_system.bonus_system_ranks_group import generator





log = logging.getLogger(__name__)

@dag(
    schedule_interval=None,  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'Postgre','test'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True, # Остановлен/запущен при появлении. Сразу запущен.
    dag_id = 'bonus_system_load'
)
def sprint5_example_stg_bonus_system_ranks_dag():
    generator()
    
stg_bonus_system_ranks_dag = sprint5_example_stg_bonus_system_ranks_dag()

if __name__ == "__main__":
   stg_bonus_system_ranks_dag.test()

