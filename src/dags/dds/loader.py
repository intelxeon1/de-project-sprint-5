from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime




class BaseDDSLoader():
    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self.pg_dwh = pg_dwh
        self.settings_repository = DDSEtlSettingsRepository()
        self.log = log
        self.WF_KEY = ""
        self.LAST_LOADED_ID_KEY = ""
                
        self.sql_new_threshold = ""
        self.sql_update = ""
        self.sql_pre_loaded = ""
    
    
    def __get_new_threshold(self,con:Connection,threshold):        
        cur = con.execute(query=self.sql_new_threshold,params={"threshold": threshold})
        result = cur.fetchone()
        if result is not None and result != '!None':
            return result[0]
        else:
            return None
        
    
    def __update(self,con:Connection,threshold,new_threshold) -> int: 
        cur = con.execute(self.sql_update,{"threshold":threshold,"new_threshold":new_threshold})    
        return cur.rowcount
    
    def __pre_load(self,con:Connection):                
        cur = con.execute(self.sql_pre_loaded)
        return cur.rowcount
    
    def load(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dwh.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: datetime(1000,1,1)})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            new_threshold = self.__get_new_threshold(conn,last_loaded)
            if new_threshold is None:
                self.log.info("No data to load")
                return
            
            if self.sql_pre_loaded != '' :
                self.log.info("Excecute preload statement")
                row_count =self.__pre_load(conn)
                self.log.info(f"Preload updates rows {row_count}")
            rows = self.__update(conn,last_loaded,new_threshold)
            
            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = new_threshold
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)            
            
            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
            self.log.info(f'Updated rows {rows}')

class UserLoader(BaseDDSLoader):
    
    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        super().__init__(pg_dwh,log)
        
        self.WF_KEY = "dds_dm_user"
        self.LAST_LOADED_ID_KEY = "last_loaded_id"
                
        self.sql_new_threshold = """
                select max(or2.update_ts)
                from stg.ordersystem_users or2 where or2.update_ts > %(threshold)s                
                """
        self.sql_update = """
                with new_v as (                       
                select  object_id, object_value::json->>'name' as new_user_name,
                object_value::json->>'login' as new_user_login 
                from stg.ordersystem_users ou 
                where ou.update_ts > %(threshold)s and ou.update_ts <= %(new_threshold)s
                ),
                joined  as (
                select object_id, new_user_name,new_user_login, id, user_name, user_login
                    from new_v left outer join dds.dm_users du on
                    new_v.object_id = du.user_id  
                ),
                _all_ as (
                    select 
                        coalesce(id,nextval('dds.dm_users_id_seq'::regclass)) as id,
                        object_id as user_id,
                        new_user_name as user_name,
                        new_user_login as user_login
                        from joined 			
                )
                insert into dds.dm_users
                select * from _all_
                ON CONFLICT (id) DO UPDATE
                        set 
                        user_login = EXCLUDED.user_login,
                        user_name = EXCLUDED.user_name

        """
    
class RestaurantLoader(BaseDDSLoader):
    
    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        super().__init__(pg_dwh,log)
        
        self.WF_KEY = "dds_dm_rest"
        self.LAST_LOADED_ID_KEY = "last_loaded_id"        
        
        self.sql_new_threshold = """
                select max(or2.update_ts)
                from stg.ordersystem_restaurants or2 where or2.update_ts > %(threshold)s                
                """
        self.sql_update = """
            with 
                new_values as (
                    select object_id, object_value::json->>'name' as new_name, or2.update_ts  from stg.ordersystem_restaurants or2
                    where or2.update_ts > %(threshold)s and update_ts <= %(new_threshold)s
                ),                
                joined as (
                    select dr.id , nv.object_id, nv.new_name,nv.update_ts, dr.restaurant_id ,dr.restaurant_name, dr.active_to, dr.active_from 
                    from new_values nv left outer join dds.dm_restaurants dr on dr.restaurant_id  = nv.object_id
                ),
                init as (
                    select nextval('dds.dm_restaurants_id_seq'::regclass) as id,object_id  as restaurant_id, new_name as restaurant_name, '2022-01-01'::timestamp as active_from, '2100-01-01'::timestamp as active_to
                    from joined where id is null
                ),
                up as (
                    select id,restaurant_id, restaurant_name,active_from,update_ts - '0.001 seconds'::interval   as active_to from joined where 
                    active_from < update_ts  and active_to > update_ts and new_name <> restaurant_name
                ),
                ins as (
                    select nextval('dds.dm_restaurants_id_seq'::regclass) as id,
                    restaurant_id, new_name as restaurant_name,update_ts as active_from,active_to from joined where 
                    active_from < update_ts  and active_to > update_ts  and new_name <> restaurant_name
                ),
                ex_point as (
                    select id,restaurant_id, new_name as restaurant_name,active_from, active_to from joined where 
                    active_from = update_ts and new_name <> restaurant_name
                ),

                _all_ as 
                (
                    select * from init union all
                    select * from up union all
                    select * from ins union all
                    select * from ex_point
                )
                insert into dds.dm_restaurants 
                select * from _all_
                ON CONFLICT (id) DO UPDATE
                        set 
                        restaurant_name = EXCLUDED.restaurant_name,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to
        
        
        """
    
class TimestampLoader(BaseDDSLoader):
    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        super().__init__(pg_dwh,log)
        
        self.WF_KEY = "dds_dm_timestamp"
        self.LAST_LOADED_ID_KEY = "last_loaded_id"     
        
        self.sql_new_threshold = """
            select now()::timestamp
            """
            
        self.sql_update = """        
        with cte as (
            select distinct (oo.object_value::json->>'date')::timestamp as ts from stg.ordersystem_orders oo             
            order by 1 ),
            new_values as (
            select ts, extract('year' from ts) as year,extract('month' from ts) as month,
                        extract('day' from ts) as day,ts::time as "time", ts::date as "date"
                        from cte where not exists (select 1 from dds.dm_timestamps dt where ts= cte.ts )
            )
            insert into dds.dm_timestamps (ts,"year","month","day","time","date")
            select * from new_values            
        """

class ProductLoader(BaseDDSLoader):
    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        super().__init__(pg_dwh,log)
        
        self.WF_KEY = "dds_dm_product"
        self.LAST_LOADED_ID_KEY = "last_loaded_id"
        
        self.sql_new_threshold = """
            select max(or2.update_ts)
            from stg.ordersystem_restaurants or2 where or2.update_ts > %(threshold)s                
            """           

        self.sql_update = """
            with 
            new_values as (
                select * from stg.ordersystem_restaurants or2
                cross join lateral json_to_recordset(object_value::json->'menu') as x(_id varchar ,name text,price numeric(14,2))
            	where or2.update_ts > %(threshold)s and or2.update_ts <= %(new_threshold)s
            ), 
            pls_res as (
                select new_values._id as new_id, new_values.name as new_name, price as new_price, update_ts,
                    dr.id as rid, object_id, dr.active_from as rest_act_from, dr.active_to  as rest_act_to
                    from  new_values left join dds.dm_restaurants dr on new_values.object_id = dr.restaurant_id and 
                    dr.active_from <= update_ts and update_ts < dr.active_to 
            ),
            joined as (
                select dp.id,new_id,new_name,new_price,update_ts,rid,
                product_id,product_name, product_price,rid as restaurant_id,object_id, active_from,active_to,
                rest_act_from,rest_act_to
                from pls_res left outer join dds.dm_products dp on
                pls_res.new_id = dp.product_id and pls_res.rid = dp.restaurant_id 
            ),
            init as (
                select nextval('dds.dm_products_id_seq'::regclass) as id,
                restaurant_id, new_id as product_id, new_name as product_name,new_price as product_price, 
                rest_act_from as active_from , rest_act_to as active_to
                from joined where product_id is null
            ),
            up as (
                select id,restaurant_id, product_id, product_name,product_price,active_from,update_ts - '0.001 seconds'::interval   as active_to 
                from joined where 
                active_from < update_ts  and active_to > update_ts and new_name <> product_name or new_price <> product_price
            ),
            ins as (
                select nextval('dds.dm_products_id_seq'::regclass) as id, rid as restaurand_id, new_id as product_id,
                new_name as product_name,new_price as product_price, update_ts as active_from,active_to 
                from joined where 
                active_from < update_ts  and active_to > update_ts  and new_name <> product_name or new_price <> product_price
            ),
            ex_point as (
                select id,restaurant_id, new_id as product_id, new_name as product_name,new_price as product_price, 
                update_ts as active_from,active_to
                from joined
                where 
                active_from = update_ts and new_name <> product_name or new_price <> product_price
            ),
            _all_ as 
            (
                select * from init union all
                select * from up union all
                select * from ins union all
                select * from ex_point
            )
            insert into dds.dm_products 
            select * from _all_
            ON CONFLICT (id) DO UPDATE
                    set restaurant_id = EXCLUDED.restaurant_id,
                        product_id = EXCLUDED.product_id,
                        product_name = EXCLUDED.product_name,
                        product_price = EXCLUDED.product_price,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to
        
        
        """
        
class OrderLoader(BaseDDSLoader):
    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        super().__init__(pg_dwh,log)
        
        self.WF_KEY = "dds_dm_order"
        self.LAST_LOADED_ID_KEY = "last_loaded_id"
        
        self.sql_new_threshold = """
            select max(or2.update_ts)
            from stg.ordersystem_orders or2 where or2.update_ts > %(threshold)s                
            """           

        self.sql_update = """
            with cte as (
                select object_id  as new_order_key, object_value::json->'user'->>'id' as new_user_id,
                    object_value::json->>'final_status' as new_order_status, (object_value::json->>'date')::timestamp as created_date,
                    object_value::json->'restaurant'->>'id' as new_restaurant_id,
                    update_ts from stg.ordersystem_orders oo 
                    where update_ts > %(threshold)s and update_ts <= %(new_threshold)s
                ),
                cte3 as ( select dr.id as new_restaurand_id, du.id  as new_user_id, dt.id as new_timestamp_id,
                    new_order_key,new_order_status   from cte left outer join dds.dm_restaurants dr on cte.new_restaurant_id = dr.restaurant_id and dr.active_from <= update_ts and dr.active_to > update_ts 
                                            left outer join dds.dm_users du on cte.new_user_id = du.user_id 
                                            left outer join dds.dm_timestamps dt on cte.created_date = dt.ts 
                ),
                cte4 as (
                    select 	* from cte3 left outer join dds.dm_orders do2 on order_key = cte3.new_order_key
                ),
                ins as (
                    select nextval('dds.dm_orders_id_seq'::regclass) as id,			
                            new_user_id as user_id,
                            new_restaurand_id as restaurant_id,
                            new_timestamp_id as timestamp_id,
                            new_order_key as order_key,
                            new_order_status as order_status
                            from cte4 where id is null
                ),
                upd as 
                (
                        select id,			
                            new_user_id as user_id,
                            new_restaurand_id as restaurant_id,
                            new_timestamp_id as timestamp_id,
                            new_order_key as order_key,
                            new_order_status as order_status
                            from cte4 where id is not null
                ),
                _all_ as (
                    select * from ins union all select * from upd
                )
                insert into dds.dm_orders
                select * from _all_ 
                ON CONFLICT (id) DO update 
                    set user_id = EXCLUDED.user_id,
                        restaurant_id = EXCLUDED.restaurant_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        order_key = EXCLUDED.order_key,	 
                        order_status = EXCLUDED.order_key

        """

class FactLoader(BaseDDSLoader):
    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        super().__init__(pg_dwh,log)
        
        self.WF_KEY = "dds_fact_sales"
        self.LAST_LOADED_ID_KEY = "last_loaded_id"     
        
        self.sql_new_threshold = """
            select now()::timestamp
            """
        self.sql_update = """        
            with cte as (
                select object_id,update_ts, x.* from stg.ordersystem_orders oo 
                            cross join lateral json_to_recordset(object_value::json->'order_items') as x(id varchar,price numeric(14,2),quantity int)
                )
                ,
                cte2 as (
                select event_value::json->>'order_id' as order_key, x.* from stg.bonussystem_events  be
                            cross join lateral json_to_recordset(event_value::json->'product_payments') as x(product_id varchar,bonus_payment numeric(14,2),bonus_grant numeric(14,2) )
                    where event_type = 'bonus_transaction'
                ),
                cte3 as (
                select * from cte left outer join cte2 on cte.object_id = cte2.order_key and cte.id = cte2.product_id
                ),
                --select * from cte3
                cte4 as (
                select 	cte3.id as product_key, do2.id as order_id,do2.restaurant_id, quantity as count,price,price*quantity as total_sum,bonus_payment,bonus_grant,
                update_ts,cte3.object_id from cte3 
                        left outer join dds.dm_orders do2 on cte3.object_id = do2.order_key 		
                ),
                cte5 as (
                    select cte4.*,dp.id as product_id  from cte4 left outer join dds.dm_products dp on cte4.product_key = dp.product_id and dp.restaurant_id = cte4.restaurant_id 	  
                    and dp.active_from <= update_ts and dp.active_to > update_ts 
                )

                insert into dds.fct_product_sales (product_id,order_id,count,price,total_sum,bonus_payment,bonus_grant)
                select product_id ,order_id ,count,price,total_sum ,coalesce(bonus_payment,0) as bonus_payment,coalesce(bonus_grant,0) as bonus_grant  from cte5		        
        """
        self.sql_pre_loaded = "delete from dds.fct_product_sales"
    
class CourierLoad(BaseDDSLoader):
    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        super().__init__(pg_dwh,log)
        
        self.WF_KEY = "dds_couriers"
        self.LAST_LOADED_ID_KEY = "last_loaded_ts"     
        
        self.sql_new_threshold = """
                select max(updated_ts)
                from ods.deliverysystem_couriers where updated_ts > %(threshold)s                
                """            
        self.sql_update = """
            insert into dds.dm_couriers (courier_key,name)
            select object_id as courier_key, name from ods.deliverysystem_couriers odc
                where updated_ts > %(threshold)s and updated_ts <= %(new_threshold)s
            on conflict (courier_key) do update  set 
                courier_key = EXCLUDED.courier_key,
                name = EXCLUDED.name	
        """      
        

class FactDelivery(BaseDDSLoader):
    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        super().__init__(pg_dwh,log)
        
        self.WF_KEY = "dds_fct_delivery"
        self.LAST_LOADED_ID_KEY = "last_loaded_ts"     
        
        self.sql_new_threshold = """
                select max(updated_ts)
                from ods.deliverysystem_deliveries where updated_ts > %(threshold)s                
                """            
        self.sql_update = """
                with new_data as (
                    select dd.delivery_id as delivery_key,
                        dc.id as courier_id,		   
                        do2.id  as order_id,
                        rate,
                        sum,
                        dd.tip_sum	
                        from ods.deliverysystem_deliveries dd 
                        left outer join dds.dm_couriers dc on dd.courier_id = dc.courier_key 
                        left outer join dds.dm_orders do2  on dd.order_id = do2.order_key
                        where updated_ts > %(threshold)s and updated_ts <= %(new_threshold)s
                ),
                ins as (
                    select nextval('dds.fct_delivery_id_seq'::regclass) as id,* from new_data where not exists ( select 1 from dds.fct_delivery fd where fd.delivery_key = new_data.delivery_key)
                ),
                joined as (
                    select fd2.id,new_data.* from new_data inner join dds.fct_delivery fd2 on new_data.delivery_key = fd2.delivery_key
                    
                ),
                un as (
                    select * from ins union all
                    select * from joined
                )
                insert into dds.fct_delivery 
                select * from un 
                on conflict (id) do update set
                id  = EXCLUDED.id ,
                delivery_key = EXCLUDED.delivery_key	,
                courier_id = EXCLUDED.courier_id,
                order_id = EXCLUDED.order_id,
                rate = EXCLUDED.rate,
                sum = EXCLUDED.sum,
                tip_sum = EXCLUDED.tip_sum
        """              
