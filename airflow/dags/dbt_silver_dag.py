# dags/dbt_silver_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

with DAG(
    dag_id="dbt_silver",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="load_bronze",
        external_task_id="load_bronze_csv",
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )
    dbt_run = BashOperator(
        task_id="dbt_run_silver",
        bash_command="cd /opt/project/crm_dbt && dbt run --models staging --target staging"
    )

    wait_for_bronze >> dbt_run