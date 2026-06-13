from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "rafa-dev",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="bronze_to_silver",
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
      /opt/pipelines/scripts/bronze_to_silver_spark.py
    """

    #================================================================
    # Definindo as Tasks
    #================================================================
    task_bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=comando_spark
    )

    task_bronze_to_silver