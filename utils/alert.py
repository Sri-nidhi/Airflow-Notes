import requests
import datetime;
import traceback
import json
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.utils.email import send_email

SLACK_CONN_ID = 'slack_alert'

def task_success_alert(context):
  msg = "Success"
  param = context['dag_run'].conf
  params = {"job_run_id": param['job_run_id'],"job_id": param['job_id'], "status_id": msg }
  status_update(param = params)
  slack_msg = """
            🆗 Integration Succeeded. 
            *Execution Time*: {exec_date}  
            *Job Url*: {job_url}
            """.format(
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
       # log_url=context.get('task_instance').log_url,
    	job_url=<job log url>,    
    )
  slack_alert(slack_msg = slack_msg, context = context)
  email_alert(status = "Succeeded")
  
    
def task_failure_alert(context):
  msg = "Failed"
  param = context['dag_run'].conf
  params = {"job_run_id": param['job_run_id'],   "job_id": param['job_id'],   "status_id": msg }
  status_update(param = params)
  slack_msg = """
            :x: Integration Failed. 
            *Execution Time*: {exec_date}  
            *Job Url*: {job_url}
            """.format(
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
       # log_url=context.get('task_instance').log_url,
    	job_url="http://d26giqbqeevfld.cloudfront.net/dataingestion",    
    )
  slack_alert(slack_msg = slack_msg, context = context)
  email_alert(status = "Failed")
  

def status_update(**kwargs):
  url = "<domain_name_api>/update_status"
  params = json.dumps(kwargs['param'])
  x = requests.post(url, data = params)
  return True

def slack_alert(**kwargs):
  slack_msg = kwargs['slack_msg']
  context = kwargs['context']
  slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
  alert = SlackWebhookOperator(
    task_id='slack_msg',
    http_conn_id='slack_alert',
    webhook_token=slack_webhook_token,
    message=slack_msg,
    channel='#velox-alerts',
    username='Workflow-Alerts')
  return alert.execute(context=context)

def email_alert(**kwargs):
  email_body = "{task_id} in {dag_id} {status}.".format(task_id = context['task_instance'].task_id,dag_id = context['task_instance'].dag_id, status = kwargs['status'])
  send_email(to=['srinidhi@goldfinchgrp.com'],subject='Job {status} at {t}'.format(t = datetime.datetime.now()),html_content=email_body, status = kwargs['status'])
