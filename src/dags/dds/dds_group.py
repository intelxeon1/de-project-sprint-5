
import logging
import pendulum
from airflow.decorators import dag, task,task_group
from lib import ConnectionBuilder
from psycopg.rows import class_row

from dds.loader import UserLoader,RestaurantLoader,TimestampLoader,ProductLoader,OrderLoader,FactLoader,CourierLoad,FactDelivery


log = logging.getLogger(__name__)

@task_group(group_id='dds_loader')
def dds_load_group():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    
    @task(task_id = 'users_load')
    def load_users():
        log.info("start")
        user_loader = UserLoader(dwh_pg_connect,log)
        user_loader.load()
            
        
    @task(task_id = 'rest_load')
    def load_rests():
        log.info("start")
        rest_load = RestaurantLoader(dwh_pg_connect,log)
        rest_load.load()
    
    
    @task(task_id = 'timestamps_load')
    def load_ts():
        log.info("start")       
        ts_load = TimestampLoader(dwh_pg_connect,log)
        ts_load.load()
    
    
    @task(task_id = 'product_load')
    def load_products():
        log.info("start")        
        ts_products = ProductLoader(dwh_pg_connect,log)
        ts_products.load()
                    
    @task(task_id = 'order_load')
    def load_orders():
        log.info("start")        
        order_loader = OrderLoader(dwh_pg_connect,log)
        order_loader.load()
    
    @task(task_id = 'fact_sales_load')
    def fact_sales():
        log.info('start')        
        fact_sales_loader = FactLoader(dwh_pg_connect,log)
        fact_sales_loader.load()
            
    @task(task_id = 'couriers_load')
    def couriers_load():
        log.info('start')        
        courier_loader = CourierLoad(dwh_pg_connect,log)
        courier_loader.load()
    
    @task(task_id = 'deliveries_load')
    def deliveries_load():
        log.info('start')       
        delivery_loader = FactDelivery(dwh_pg_connect,log)
        delivery_loader.load()
    
    orders_load_task = load_orders()
    products_load_task = load_products()
    ts_load_task = load_ts()
    rest_load_task = load_rests()
    user_load_task = load_users()
    fact_load_task = fact_sales()    
    courier_load_task = couriers_load()    
    delivery_load_task = deliveries_load()
       
    [ user_load_task,rest_load_task,ts_load_task,courier_load_task ] >> products_load_task >> orders_load_task >> fact_load_task >> delivery_load_task

