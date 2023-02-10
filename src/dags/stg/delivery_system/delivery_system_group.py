

import logging
import pendulum
from airflow.decorators import dag, task,task_group
from lib import ConnectionBuilder

from stg.delivery_system.loader import DeliverySystemDestRep, DeliverySystemOrigin
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

APIs = ['couriers','deliveries','restaurants']


@task_group(group_id='delivery_system')
def delivery_task_group():
    http_hook = HttpHook(method='GET',http_conn_id = 'HTTP_ORIGIN_DELIVERY_SYSTEM_CONNECTION')
   
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    

    
    def make_task(end_point:str):
        @task(task_id=f'{end_point}_load')
        def task_load(end_point:str):
            loader = DeliverySystemOrigin(http_hook,end_point,{},log)
            writer = DeliverySystemDestRep(dwh_pg_connect,end_point,log)
            
            data = loader.get_data()
            writer.insert_data(data)
        return task_load(end_point) 
    courier_task = make_task('couriers')
    delivery_task = make_task('deliveries')
    rest_task = make_task('restaurants')
    delivery_task >> [courier_task , rest_task]

