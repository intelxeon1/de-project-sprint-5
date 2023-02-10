
import logging
from airflow.decorators import task,task_group
from lib import ConnectionBuilder
from cdm.cdm_loader import SettlementReportCalc

log = logging.getLogger(__name__)
@task_group(group_id='cdm_calculaion')
def cdm_calculation_group():   
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")    
    
    @task(task_id="settlement_report")
    def settlemnt_report():
        calc = SettlementReportCalc(dwh_pg_connect,log)
        calc.load()
        
    @task(task_id="courier_ledger")
    def courier_report():
        calc = SettlementReportCalc(dwh_pg_connect,log)
        calc.load()
        
    [ settlemnt_report() , courier_report()]
        

    


 