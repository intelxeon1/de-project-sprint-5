import logging
import pendulum
from airflow.decorators import dag, task,task_group
from airflow.models.variable import Variable
from stg.order_system.pg_saver import PgSaver
from stg.order_system.loader import RestaurantLoader, UserLoader, OrderLoader
from stg.order_system.reader import RestaurantReader, UserReader,OrderReader
from airflow.operators.trigger_dagrun import TriggerDagRunOperator  

from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)



def generator():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Получаем переменные из Airflow.
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task(task_id = "load_rests")
    def load_restaurants():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = RestaurantReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

        
    @task(task_id = "load_users")
    def load_users():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = UserReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = UserLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    
    
    
    @task(task_id = "load_orders")
    def load_orders():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем подключение у MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = OrderReader(mongo_connect)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = OrderLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    order_loader = load_orders()
    user_loader = load_users()
    restaurant_loader = load_restaurants()
    [restaurant_loader,user_loader,order_loader]

def group_generator():
    @task_group(group_id='order_group')
    def order_group():
        generator()    
    
    return order_group()
        
