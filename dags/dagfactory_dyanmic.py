#Creates dynamic dags based on configuration from config_file yaml
from airflow import DAG
import dagfactory

dag_factory = dagfactory.DagFactory("/home/ubuntu/airflow/dags/config_file.yml")

dag_factory.generate_dags(globals())
