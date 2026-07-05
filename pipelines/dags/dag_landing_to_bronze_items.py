from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args={
    "owner": "rafa-dev",
    "start_date": datetime(2026,1,1),
}

with DAG(
    dag_id="landing_to_bronze_items",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["bronze", "items"]
)as dag:

    task_items_landing_to_bronze = SparkSubmitOperator(
        task_id="Landing_to_Bronze_items",
        conn_id="spark_default", #O Airflow usará esta conexão (configurada na UI)
        application="/opt/pipelines/scripts/landing_to_bronze_items.py",
        name="job-landing-to-bronze",
        verbose=True
    )

    task_items_landing_to_bronze