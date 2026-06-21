from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

#Coletando o end point do MinIO
minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

#Configurações Nativas e limpas do S3A para o MINIO
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
    dag_id="bronze_to_silver_customers",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:      

    task_bronze_to_silver = SparkSubmitOperator(
        task_id = "Bronze_to_Silver",
        conn_id = "spark_default",
        application = "/opt/pipelines/scripts/bronze_to_silver_customers.py",
        name = "job_bronze_to_silver",
        conf = conf,
        verbose = True
    )

    task_bronze_to_silver