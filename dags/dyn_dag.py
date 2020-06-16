from airflow import DAG
import dagfactory

dag_factory = dagfactory.DagFactory("/home/ubuntu/airflow/dags/config_file.yml")

dag_factory.generate_dags(globals())
