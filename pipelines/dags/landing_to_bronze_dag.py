import os
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Captura o endpoint do MinIO dinamicamente com um fallback para desenvolvimento local
minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

 # Configurações nativas e limpas do conector S3A para o MinIO
conf={
    "spark.hadoop.fs.s3a.endpoint": minio_endpoint,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
}

default_args = {
    "owner": "rafa-dev",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="landing_to_bronze",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["lakehouse", "medallion", "ingestion"] # Tags ajudam na organização visual da UI
) as dag:   
    
    # Orquestração do processamento de dados usando o SparkSubmitOperator
    task_landing_to_bronze = SparkSubmitOperator(
        task_id="Landing_to_Bronze",
        conn_id="spark_default", #O Airflow usará esta conexão (configurada na UI) para achar o Spark Master
        application="/opt/pipelines/scripts/landing_to_bronze_spark.py",
        name="job-landing-to-bronze",       
        conf=conf,
        verbose=True
    )

    # Fluxo da DAG
    task_landing_to_bronze