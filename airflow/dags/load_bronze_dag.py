# dags/load_bronze_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append("/opt/project")
from ingestion.load_csv import main as load_bronze_data

with DAG(
    dag_id="load_bronze",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    load_bronze_task = PythonOperator(
        task_id="load_bronze_csv",
        python_callable=load_bronze_data
    )