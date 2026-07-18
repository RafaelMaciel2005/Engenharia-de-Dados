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
    schedule_interval=None,  # sem agendamento proprio: quem dispara e a DAG mestre (pipeline_completo) ou a UI
    catchup=False,           # nao tenta "recuperar" execucoes passadas desde o start_date
    tags=["lakehouse", "medallion", "silver"],
) as dag:

    # Gera uma task por tabela a partir do MESMO registro central usado na Bronze
    # (config/tabelas_olist.py) — as camadas processam exatamente as mesmas tabelas.
    # A logica de tratamento especifica de cada tabela vive em scripts/silver_rules.py.
    for tabela in TABELAS_OLIST:
        SparkSubmitOperator(
            task_id=f"silver_{tabela['nome']}",
            # Conexao com o cluster, definida via AIRFLOW_CONN_SPARK_DEFAULT no .env
            conn_id="spark_default",
            # Caminho DENTRO do container (pipelines/ montada em /opt/pipelines)
            application="/opt/pipelines/scripts/bronze_to_silver.py",
            application_args=[tabela["nome"]],
            name=f"job-bronze-to-silver-{tabela['nome']}",
            verbose=True,  # log completo do spark-submit no log da task
        )
