from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "rafa-dev",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="landing_to_bronze_customers",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["lakehouse", "medallion", "ingestion"] # Tags ajudam na organização visual da UI
) as dag:   
    
    # Orquestração do processamento de dados usando o SparkSubmitOperator
    task_landing_to_bronze = SparkSubmitOperator(
        task_id="Landing_to_Bronze_customers",
        conn_id="spark_default", #O Airflow usará esta conexão (configurada na UI) para achar o Spark Master
        application="/opt/pipelines/scripts/landing_to_bronze_customers.py",
        name="job-landing-to-bronze",
        verbose=True
    )

    # Fluxo da DAG
    task_landing_to_bronze