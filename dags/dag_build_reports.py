from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {'owner': 'airflow'}

with DAG(
    dag_id='BUILD_REPORTS',
    start_date=datetime(2025, 10, 12),
    schedule="0 3 * * *",
    catchup=True,
    default_args=default_args
) as dag:

    trigger_silver = TriggerDagRunOperator(
        task_id='trigger_silver_layer',
        trigger_dag_id='INGEST_BRONZE_TO_SILVER'
    )

    trigger_gold = TriggerDagRunOperator(
        task_id='trigger_gold_layer',
        trigger_dag_id='CREATE_GOLD_TABLES'
    )

    trigger_silver >> trigger_gold