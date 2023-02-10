from logging import Logger
from typing import List

from dds.dds_settings_repository import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection, sql
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime
from cdm.cdm_const import SETTLEMENT_REPORT_SQL,COURIER_LEDGER_SQL




class BaseCDMLoader():
    def __init__(self, pg_dwh: PgConnect, log: Logger, sql_update:str) -> None:
        self._pg_dwh = pg_dwh
        self._sql_update = sql_update
        self._log = log
    
    def __update(self,con:Connection) -> int: 
        cur = con.execute(self._sql_update)    
        return cur.rowcount    
    
    def calc(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        rows = 0
        with self._pg_dwh.connection() as conn:            
            rows = self.__update(conn)          
            
        self._log.info(f"Load finished")
        self._log.info(f'Updated rows {rows}')

class SettlementReportCalc():
    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:                  
        self._loader = BaseCDMLoader(pg_dwh,log,SETTLEMENT_REPORT_SQL)
        
    def load(self):
        self._loader.calc()
        

class CourierLedgerCalc():
    def __init__(self, pg_dwh: PgConnect, log: Logger) -> None:                  
        self._loader = BaseCDMLoader(pg_dwh,log,COURIER_LEDGER_SQL)
        
    def load(self):
        self._loader.calc()          