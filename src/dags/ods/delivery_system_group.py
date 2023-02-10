

import logging
import pendulum
from airflow.decorators import dag, task,task_group
from lib import ConnectionBuilder

from stg.delivery_system.loader import DeliverySystemDestRep, DeliverySystemOrigin
from airflow.providers.http.hooks.http import HttpHook
from ods.ods_loader import CouriersLoader,DeliveriesLoader,RestaurantsLoader

log = logging.getLogger(__name__)



@task_group(group_id='ods_delivery_group')
    

def project_ods_delivery_group():   
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")    
    
    @task(task_id="couriers_load")
    def couriers_load():
        loader = CouriersLoader(dwh_pg_connect,log)
        loader.load()
    
    @task(task_id="deliveries_load")
    def deliveries_load():
        loader = DeliveriesLoader(dwh_pg_connect,log)
        loader.load()
        
    @task(task_id="restaurants_load")
    def restaurants_load():
        loader = RestaurantsLoader(dwh_pg_connect,log)
        loader.load()
        
    [ couriers_load(), deliveries_load(), restaurants_load() ]
        

    


