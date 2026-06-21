import os
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")

# Configurações nativas e limpas do conector S3A

conf={
    "spark.hadoop.fs.s3a.endpoint": minio_endpoint,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
}

default_args={
    "owner": "rafa-dev",
    "start_date": datetime(2026,1,1),
}

with DAG(
    dag_id="landing_to_bronze_products",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["bronze", "products"]
)as dag:

    task_products_landing_to_bronze = SparkSubmitOperator(
        task_id="Landing_to_Bronze_Products",
        conn_id="spark_default", #O Airflow usará esta conexão (configurada na UI)
        application="/opt/pipelines/scripts/landing_to_bronze_products.py",
        name="job-landing-to-bronze",
        conf=conf,
        verbose=True
    )

    task_products_landing_to_bronze