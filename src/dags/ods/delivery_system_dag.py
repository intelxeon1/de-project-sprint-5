
import logging
import pendulum
from airflow.decorators import dag
from ods.delivery_system_group import project_ods_delivery_group

log = logging.getLogger(__name__)

@dag(
    schedule_interval=None,  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'Postgre','test'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True, # Остановлен/запущен при появлении.
    dag_id = 'ods_delivery_system_load'
)
def project_ods_delivery_system_ranks_dag():
    project_ods_delivery_group()        
    
ods_delivery_system_ranks_dag = project_ods_delivery_system_ranks_dag()

if __name__ == "__main__":
   ods_delivery_system_ranks_dag.test()

