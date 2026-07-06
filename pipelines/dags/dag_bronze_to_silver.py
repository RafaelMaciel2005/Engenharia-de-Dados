from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config.tabelas_olist import TABELAS_OLIST

default_args = {
    "owner": "rafa-dev",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="bronze_to_silver",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["lakehouse", "medallion", "silver"],
) as dag:

    # Gera uma task por tabela a partir do registro central (config/tabelas_olist.py).
    # O script genérico aplica a regra de tratamento específica de cada tabela,
    # definida em scripts/silver_rules.py.
    for tabela in TABELAS_OLIST:
        SparkSubmitOperator(
            task_id=f"silver_{tabela['nome']}",
            conn_id="spark_default",
            application="/opt/pipelines/scripts/bronze_to_silver.py",
            application_args=[tabela["nome"]],
            name=f"job-bronze-to-silver-{tabela['nome']}",
            verbose=True,
        )
