
import logging
import pendulum
from airflow.decorators import dag
from stg.bonus_system.bonus_system_ranks_group import group_generator as bonus_group_generator
from stg.order_system.order_system_restaurants_group import group_generator as order_group_generator
from dds.dds_group import dds_load_group 
from stg.delivery_system.delivery_system_group import delivery_task_group
from ods.delivery_system_group import project_ods_delivery_group  as ods_del_group
from cdm.cdm_calc_group import cdm_calculation_group


log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False, # Остановлен/запущен при появлении. Сразу запущен.
    dag_id = 'pipeline_dag'
)
def pipeline_dag():
    bonus_gp = bonus_group_generator()
    order_gp = order_group_generator()
    deliv_gp = delivery_task_group()    
    ods_deliv_group = ods_del_group()    
    dds_gp_or_bon = dds_load_group()    
    cdm_gp = cdm_calculation_group()
    
    
    deliv_gp >> order_gp >> bonus_gp >> ods_deliv_group >> dds_gp_or_bon >> cdm_gp
    
dag = pipeline_dag()

if __name__ == "__main__":
   dag.test()