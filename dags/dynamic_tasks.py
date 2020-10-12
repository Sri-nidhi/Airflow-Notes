#Reads a list from YAML and creates tasks and subdags dynamically 
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
default_args = {
    'owner': 'srinidhi',
    'depends_on_past': False,
    'start_date': datetime(2017, 6, 26),
    'provide_context': True,
    'retries': 10,
    'retry_delay': timedelta(seconds=30)
}


dag = DAG(
    'adhoc_run',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    concurrency=1)

import yaml
import papermill as pm
task = {}
path =  '/home/ubuntu/Jupyter/RAD_UAT/'
def papermill(**kwargs):
    pm.execute_notebook(
   path+kwargs['input'],
   path+'Test.ipynb')

with open("/home/ubuntu/airflow/dags/rad_adhoc_items.yaml", 'r') as stream:
    m = yaml.safe_load(stream)
    for j in range(0,len(m)):
        #print(m[j])
        task[j] = PythonOperator(task_id= str(m[j].replace('.ipynb','').replace(" ","")), python_callable=papermill,op_kwargs={'input': m[j]}, dag=dag)
        if  j != 0:
            task[j-1].set_downstream(task[j])


