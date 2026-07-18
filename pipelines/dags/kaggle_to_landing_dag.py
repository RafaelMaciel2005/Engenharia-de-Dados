from datetime import datetime
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
    schedule_interval=None,  # disparo manual ou via DAG mestre: o dataset Olist e historico, nao ha dado novo chegando
    catchup=False,
    tags=['landing', 'kaggle', 'extract']
) as dag:

    # Extracao em duas tasks separadas de proposito: se o upload para o MinIO
    # falhar, da para re-rodar so ele, sem baixar tudo do Kaggle de novo
    # (o download e a parte lenta e sujeita a limite de API).
    task_download = PythonOperator(
        task_id="extrair_do_kaggle",
        python_callable=extrair_do_kaggle
    )

    task_upload = PythonOperator(
        task_id="enviar_para_minio",
        python_callable=carregar_para_minio
    )

    # Upload so comeca depois que o download terminar com sucesso
    task_download >> task_upload
