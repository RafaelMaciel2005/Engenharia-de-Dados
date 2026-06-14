from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.kaggle_to_landing import extrair_do_kaggle, carregar_para_minio

default_args = {
    'owner': 'rafa-dev',
    'start_date': datetime(2026, 1, 1),
}

with DAG(
    dag_id='kaggle_to_landing_zone',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['landing', 'kaggle', 'extract']
) as dag:
    
    task_download = PythonOperator(
        task_id="extrair_do_kaggle",
        python_callable=extrair_do_kaggle
    )
    
    task_upload = PythonOperator(
        task_id="enviar_para_minio",
        python_callable=carregar_para_minio
    )

    task_download >> task_upload