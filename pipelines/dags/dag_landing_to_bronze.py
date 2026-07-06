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
    schedule_interval=None,
    catchup=False,
    tags=["lakehouse", "medallion", "bronze"],
) as dag:

    # Gera uma task por tabela a partir do registro central (config/tabelas_olist.py).
    # Todas as tasks rodam o MESMO script genérico, mudando apenas o nome da tabela.
    # Cada tabela continua isolada: se uma falhar, as outras seguem normalmente.
    for tabela in TABELAS_OLIST:
        SparkSubmitOperator(
            task_id=f"bronze_{tabela['nome']}",
            conn_id="spark_default",
            application="/opt/pipelines/scripts/landing_to_bronze.py",
            application_args=[tabela["nome"]],
            name=f"job-landing-to-bronze-{tabela['nome']}",
            verbose=True,
        )
