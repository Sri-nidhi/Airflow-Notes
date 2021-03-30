from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from std_scripts.omega_plugin import ArchiveFileOperator,OmegaFileSensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime,timedelta
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from io import StringIO
import os
import numpy as np
import re
pd.options.mode.chained_assignment = None
import time
from datetime import datetime
import datetime as dt
import calendar
from std_scripts.std_functions import *


import yaml
import papermill as pm


#addr_format = StreetAddressFormatter()

default_args = {
    'owner': 'srinidhi',
    'depends_on_past': False,
    'start_date': datetime(2017, 6, 26),
    'provide_context': True,
    'retries': 10,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': task_fail_slack_alert
}
dag_config = Variable.get("inc_load", deserialize_json=True)
inc_file_lastrun = Variable.get("inc_file_lastrun")
inc_load = int(Variable.get("inc_load_flag"))
init_df = pd.DataFrame()

def to_datetime(date_string):
    return datetime.strptime(date_string, "%Y-%m-%d")



filepath = dag_config["inc_sourcePath"]
filepattern = dag_config["filePattern"]
start_date = to_datetime(dag_config["start_date"])
end_date = to_datetime(dag_config["end_date"])
current_db = dag_config["current_db"]




dag = DAG(
    'data_ingestion',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1)



def process_file(**kwargs):
    ti = kwargs['ti']
    file_to_process = ti.xcom_pull(key='file_name', task_ids='file_sensor')
    print(inc_load, (inc_load == 1), ((inc_load == 1) or (inc_file_lastrun != file_to_process)))
    if((inc_load == 1) or (inc_file_lastrun != file_to_process)):        
        return 'prepare_the_data'
    else:
        return 'stop_task'
        

path =  '/home/ubuntu/Jupyter/'
def papermill(**kwargs):
    ti = kwargs['ti']
    file_to_process = ti.xcom_pull(key='file_name', task_ids='file_sensor')
    pm.execute_notebook(
   path+'ingestion.ipynb',
   path+'Output_Log.ipynb',
   parameters = dict(filepath= filepath, file_to_process = file_to_process))
    Variable.set("inc_file_lastrun", file_to_process)
    print('Data Pushed to Source')

import glob
import os

def fetch_latest_file(**context):
    list_of_files = glob.glob(filepath+'*'+filepattern+'*.xlsx') # * means all if need specific format then *.csv
    latest_file = max(list_of_files, key=os.path.getmtime)
    latest_file = str(latest_file).replace(filepath,'')
    context['task_instance'].xcom_push('file_name', latest_file)
    return True


trigger_task = TriggerDagRunOperator(
    task_id="trigger_model_run",
    trigger_dag_id="model_run",  # Ensure this equals the dag_id of the DAG to trigger
    dag=dag,
)

success_task = PythonOperator(task_id='sucess_alert', python_callable=task_success_slack_alert, dag=dag)
    
sensor_task = PythonOperator(task_id='file_sensor', python_callable=fetch_latest_file,provide_context=True, dag=dag)
stop_op = DummyOperator(task_id='stop_task', dag=dag)
process_task = BranchPythonOperator(task_id='process_the_file', python_callable=process_file,provide_context=True, dag=dag)
refresh_call = PythonOperator(task_id='prepare_the_data', python_callable=papermill, dag=dag)
sensor_task >> process_task >> [refresh_call, stop_op] 
refresh_call >> trigger_task >> success_task
