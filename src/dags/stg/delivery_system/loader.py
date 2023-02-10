
import requests
from typing  import Dict,Union,List,Literal
from typing_extensions import TypeAlias
from decimal import Decimal
from lib import PgConnect
from airflow.providers.http.hooks.http import HttpHook
from stg.stg_settings_repository import StgEtlSettingsRepository, EtlSetting
import datetime
import json
from psycopg import sql
from logging import Logger
import time
from airflow.exceptions import AirflowException


class DeliverySystemOrigin:   
    
    DEFAULT_PARAMS = {'sort_field': 'id', 'sort_direction': 'asc'}

    def __init__(self,hook:HttpHook,end_point:str,headers:Dict[str,str],log: Logger):
        self._end_point = end_point
        self._hook = hook
        self._headers= headers
        self._log = log       
        
    def get_data(self,params:Dict[str,Union[str,int]]={},limit=50) -> List[Dict[str,Union[str,int,Decimal]]]:
        data = []
        req_offset = 0
        req_limit = limit
        
        request_params = self.DEFAULT_PARAMS or params
        
        empty_list = False
        while empty_list!=True:
            request_params['limit'] = req_limit   
            request_params['offset'] = req_offset
            for i in range(10): 
                try:
                    response = self._hook.run(endpoint=self._end_point,data=request_params,headers=self._headers)                       
                    if response.ok:
                        pack = response.json()                    
                        if pack==[]:
                            empty_list = True
                        else:
                            packet_count = req_offset / req_limit + 1                    
                            self._log.info(f"API: {self._end_point}. Getting packets: {packet_count}")                    
                            data.extend(pack)
                    else:
                        print('Error in API call')
                    req_offset += req_limit    
                except AirflowException  as e:                    
                    self._log.info(f"Error while getting data from api")
                    self._log.info(str(e))
                    self._log.info(f"Next attemp in 10 seconds")
                    time.sleep(10)
        return data
        
class DeliverySystemDestRep:   
    LAST_LOADED_ID_KEY = "last_loaded_ts"
    SQL_INSERT_STATEMENT = """ 
                INSERT INTO stg.{} (object_value)
                VALUES (%s)
                """
    SQL_TRUNCATE_STATEMENT = """TRUNCATE TABLE stg.{}"""
    def __init__( self, pg: PgConnect,end_point:str,log: Logger) -> None:
        self._db = pg        
        self._settings_repository = StgEtlSettingsRepository()
        self._workflowkey = f""
        self._WF_KEY = f"delivery_system_{end_point}_stg"
        self._end_point = end_point
        self._log = log
        
    def insert_data(self,data: List[Dict[str,Union[str,int,Decimal]]])->None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                self._log.info(f'{self._end_point}: Start of writing data...')
                args = [ ( json.dumps(row,ensure_ascii = False), ) for row in data]
                tab_name = f"deliverysystem_{self._end_point}"
                cur.execute(sql.SQL(self.SQL_TRUNCATE_STATEMENT).format(sql.Identifier(tab_name)))
                cur.executemany(
                    sql.SQL(self.SQL_INSERT_STATEMENT).format(sql.Identifier(tab_name)),
                    args
                )
                self._log.info(f'{self._end_point} Data has written successufully.')
            
            self._log.info(f'{self._end_point} Write settings')                
            ts = datetime.datetime.now()            
            self._settings_repository.save_setting(conn, self._WF_KEY, json.dumps({self.LAST_LOADED_ID_KEY: str(ts)}))
            
            
