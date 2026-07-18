from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config.gold_objects import GOLD_OBJECTS

default_args = {
    "owner": "rafa-dev",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="silver_to_gold",
    default_args=default_args,
    schedule_interval=None,  # sem agendamento proprio: quem dispara e a DAG mestre (pipeline_completo) ou a UI
    catchup=False,           # nao tenta "recuperar" execucoes passadas desde o start_date
    tags=["lakehouse", "medallion", "gold"],
) as dag:

    # Gera uma task por objeto Gold (4 dimensoes + 3 fatos) a partir do registro
    # central (config/gold_objects.py). As tasks rodam em paralelo de proposito:
    # os fatos usam chave natural (nao surrogate key), entao nenhum objeto depende
    # de outro objeto da propria Gold — todos leem apenas da Silver.
    for objeto in GOLD_OBJECTS:
        SparkSubmitOperator(
            task_id=f"gold_{objeto['nome']}",
            # Conexao com o cluster, definida via AIRFLOW_CONN_SPARK_DEFAULT no .env
            conn_id="spark_default",
            # Caminho DENTRO do container (pipelines/ montada em /opt/pipelines)
            application="/opt/pipelines/scripts/silver_to_gold.py",
            application_args=[objeto["nome"]],
            name=f"job-silver-to-gold-{objeto['nome']}",
            verbose=True,  # log completo do spark-submit no log da task
        )
