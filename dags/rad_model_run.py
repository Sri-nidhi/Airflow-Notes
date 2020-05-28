from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator,BranchPythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.http_operator import SimpleHttpOperator
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

default_args = {
    'owner': 'srinidhi',
    'depends_on_past': False,
    'start_date': datetime(2017, 6, 26),
    'provide_context': True,
    'retries': 10,
    'retry_delay': timedelta(seconds=30),
    'on_failure_callback': task_fail_slack_alert
}


main_dag = DAG(
    'rad_model_run',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1)


import yaml
import papermill as pm
task = {}
path =  '/home/ubuntu/Jupyter/RAD_UAT/'
# path '/home/ubuntu/Jupyter/RAD/' for prod
def papermill(**kwargs):
    pm.execute_notebook(
   path+kwargs['input'],
   path+'Test.ipynb')

def create_sub_dag(parent_dag_name, child_dag_name, n, default_args):
    dag = DAG(
'%s.%s' % (parent_dag_name, child_dag_name),
schedule_interval=None,
default_args=default_args,
)

    with dag:
        for i in range(len(n)):
            task[i] = PythonOperator(task_id= str(n[i].replace('.ipynb','').replace(" ","")), python_callable=papermill,op_kwargs={'input': n[i]}, dag=dag)
            if  i != 0:
                task[i-1].set_downstream(task[i])

        return dag

sub_dag = {}
           
with open("/home/ubuntu/airflow/dags/items.yaml", 'r') as stream:
    m = yaml.safe_load(stream)
    for j in range(0,len(m)):
        k = ''.join(m[j])
        v =  list(m[j].values())
        sub_dag[j] = SubDagOperator(subdag = create_sub_dag('rad_model_run',k , v[0],default_args),task_id=k,default_args=default_args, dag=main_dag)
        if j != 0:
            sub_dag[j-1].set_downstream(sub_dag[j])

    final_procedure = PythonOperator(task_id='proc_call', python_callable=papermill, op_kwargs={'input':'python_interface_to_connect_postgresql_sp.ipynb'}, dag=main_dag)
    success_task = PythonOperator(task_id='sucess_alert', python_callable=task_success_slack_alert, dag=main_dag)
    sub_dag[j].set_downstream(final_procedure)
    final_procedure.set_downstream(success_task) 
 	