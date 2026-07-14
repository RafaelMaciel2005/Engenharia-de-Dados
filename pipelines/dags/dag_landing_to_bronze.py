from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from config.tabelas_olist import TABELAS_OLIST

default_args = {
    "owner": "rafa-dev",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="landing_to_bronze",
    default_args=default_args,
    schedule_interval=None,  # sem agendamento: disparo manual pela UI (DAG mestre esta no roadmap)
    catchup=False,           # nao tenta "recuperar" execucoes passadas desde o start_date
    tags=["lakehouse", "medallion", "bronze"],
) as dag:

    # Gera uma task por tabela a partir do registro central (config/tabelas_olist.py).
    # Todas as tasks rodam o MESMO script generico, mudando apenas o nome da tabela.
    # Cada tabela continua isolada: se uma falhar, as outras seguem normalmente.
    for tabela in TABELAS_OLIST:
        SparkSubmitOperator(
            task_id=f"bronze_{tabela['nome']}",
            # Conexao com o cluster (spark://spark-master:7077), definida via
            # AIRFLOW_CONN_SPARK_DEFAULT no .env — nada configurado na UI.
            conn_id="spark_default",
            # Caminho DENTRO do container do Airflow (a pasta pipelines/ do repo
            # e montada em /opt/pipelines pelo docker-compose).
            application="/opt/pipelines/scripts/landing_to_bronze.py",
            # O script recebe o nome da tabela como argumento de linha de comando
            # e busca o resto da configuracao sozinho no registro central.
            application_args=[tabela["nome"]],
            name=f"job-landing-to-bronze-{tabela['nome']}",
            verbose=True,  # log completo do spark-submit no log da task (facilita debug)
        )
