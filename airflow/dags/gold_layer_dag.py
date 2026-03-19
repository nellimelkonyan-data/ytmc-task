# dags/dbt_gold_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

with DAG(
    dag_id="dbt_gold",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="dbt_silver",
        external_task_id="dbt_run_silver",
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )
    dbt_run = BashOperator(
        task_id="dbt_run_gold",
        bash_command="cd /opt/project/crm_dbt && dbt run --models gold --target gold"
    )

    wait_for_silver >> dbt_run