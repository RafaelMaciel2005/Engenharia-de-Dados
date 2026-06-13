from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "rafa-dev",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="landing_to_bronze",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:
    
    # Injeção das configurações de infraestrutura via linha de comando
    comando_spark = """
    spark-submit \
      --conf "spark.hadoop.fs.s3a.endpoint=$MINIO_ENDPOINT" \
      --conf "spark.hadoop.fs.s3a.path.style.access=true" \
      --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
      /opt/pipelines/scripts/landing_to_bronze_spark.py
    """

    task_landing_to_bronze = BashOperator(
        task_id="Landing_to_Bronze",
        bash_command=comando_spark
    )

    # Fluxo da DAG
    task_landing_to_bronze