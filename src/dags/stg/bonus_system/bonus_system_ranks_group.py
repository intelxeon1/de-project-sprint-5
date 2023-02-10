

import logging
import pendulum
from airflow.decorators import dag, task, task_group
from lib import ConnectionBuilder
from psycopg.rows import class_row
from pydantic import BaseModel
from stg.bonus_system.event_loader import EventLoader
from stg.bonus_system.ranks_loader import RankLoader
from stg.bonus_system.user_loader import UserLoader


log = logging.getLogger(__name__)

def generator():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="ranks_load")
    def load_ranks():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_ranks()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.  

    @task(task_id = 'users_load')
    def load_users():
       user_loader = UserLoader(origin_pg_connect,dwh_pg_connect,log)
       user_loader.load_users()

    @task(task_id = 'events_load')
    def load_events():
        ev_loader = EventLoader(origin_pg_connect,dwh_pg_connect,log)
        ev_loader.load_events()
      
    events_task = load_events()
    user_load_task = load_users()
    ranks_dict = load_ranks()
    [ ranks_dict, user_load_task, events_task ]
   
def group_generator():   
    @task_group(group_id='bonus_group')
    def bonus_group():
        generator()    
    stg_bonus_system_group = bonus_group()
    return stg_bonus_system_group


