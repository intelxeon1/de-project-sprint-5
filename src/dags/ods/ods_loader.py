from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect, EtlSettingsRepository
from lib.dict_util import json2str
from psycopg import Connection,sql
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime
import json




class BaseODSLoader():
    def __init__(self, pg_dwh: PgConnect, log: Logger,entity:str,sql_update:str) -> None:
        self._pg_dwh = pg_dwh
        self._settings_repository =EtlSettingsRepository("ods")
        self._log = log
        
        self._entity = f"deliverysystem_{entity}"
        self._WF_KEY = f"ods_{self._entity}"
        self._LAST_LOADED_ID_KEY = "last_loaded_ts"
                
        self._sql_update = sql.SQL(sql_update)
                
        self._sql_new_threshold = sql.SQL("""
                select max(updated_ts)
                from ods.{}
                """).format(sql.Identifier(self._entity))
    
    
    def __get_new_threshold(self,con:Connection):        
        cur = con.execute(query=self._sql_new_threshold)
        result = cur.fetchone()
        if result is not None and result != '!None':
            return result[0]
        else:
            return None
        
    
    def __update(self,con:Connection) -> int: 
        cur = con.execute(self._sql_update)
        return cur.rowcount       
    
    def load(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self._pg_dwh.connection() as conn:
           
            rows = self.__update(conn)
            if rows > 0:
                new_threshold = self.__get_new_threshold(conn)
                self._settings_repository.save_setting(conn, self._WF_KEY, json.dumps({self._LAST_LOADED_ID_KEY: str(new_threshold)})) 
            
            self._log.info(f"Load finished.")
            self._log.info(f'Updated rows {rows}')

class CouriersLoader():       
        
    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self._sql_update = ("""            
                with new_data as (select dr.object_value->>'_id' as object_id, dr.object_value->>'name' as name from stg.deliverysystem_couriers dr 	
                except
                select object_id,name from ods.deliverysystem_couriers dc )
                insert into ods.deliverysystem_couriers (object_id,name,updated_ts)
                select *,now() as updated_ts from new_data
                ON CONFLICT (object_id) do update set
                    name = EXCLUDED.name,
                    updated_ts = EXCLUDED.updated_ts 
        """)
        
        self._loader = BaseODSLoader(pg_dwh,log,"couriers",self._sql_update)        
        
    def load(self):
        self._loader.load()



class DeliveriesLoader():
    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self._sql_update = ("""            
                    with new_data as (
                    select dr.object_value->>'order_id'::varchar(24) as object_id, (dr.object_value->>'order_ts'::text)::timestamp as order_ts ,
                    dr.object_value->>'delivery_id'::varchar(24) as delivery_id,

                    dr.object_value->>'courier_id' as courier_id, dr.object_value->>'address' as address,(dr.object_value->>'delivery_ts'::text)::timestamp,
                    (dr.object_value->>'rate'::text)::int4 as rate,(dr.object_value->>'sum'::text)::numeric(17,2) as "sum", (dr.object_value->>'tip_sum'::text)::numeric(17,2) as tip_sum

                    from stg.deliverysystem_deliveries dr 	
                    except
                    select order_id,order_ts,delivery_id,courier_id,address,delivery_ts,rate,sum,tip_sum
                    from  ods.deliverysystem_deliveries dc )
                    insert into ods.deliverysystem_deliveries (order_id,order_ts,delivery_id,courier_id,address,delivery_ts,rate,sum,tip_sum,updated_ts)
                    select *,now() as updated_ts from new_data
                    ON CONFLICT (delivery_id) do update set
                    order_id	= EXCLUDED.order_id	,
                    order_ts	= EXCLUDED.order_ts	,
                    courier_id	= EXCLUDED.courier_id	,
                    delivery_id	= EXCLUDED.delivery_id	,
                    address	= EXCLUDED.address	,
                    delivery_ts	= EXCLUDED.delivery_ts	,
                    rate	= EXCLUDED.rate	,
                    sum	= EXCLUDED.sum	,
                    tip_sum	= EXCLUDED.tip_sum	,
                    updated_ts	= EXCLUDED.updated_ts	
        """)
        
        self._loader = BaseODSLoader(pg_dwh,log,"deliveries",self._sql_update)        
        
    def load(self):
        self._loader.load()
        
        

class RestaurantsLoader():       
        
    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:
        self._sql_update = ("""            
            with new_data as (select dr.object_value->>'_id' as object_id, dr.object_value->>'name' as name from stg.deliverysystem_restaurants dr 	
            except
            select object_id,name from ods.deliverysystem_restaurants dc )
            insert into ods.deliverysystem_restaurants (object_id,name,updated_ts)
            select *,now() as updated_ts from new_data
            ON CONFLICT (object_id) do update set
                name = EXCLUDED.name,
                updated_ts = EXCLUDED.updated_ts 
        """)
        
        self._loader = BaseODSLoader(pg_dwh,log,"restaurants",self._sql_update)        
        
    def load(self):
        self._loader.load()        