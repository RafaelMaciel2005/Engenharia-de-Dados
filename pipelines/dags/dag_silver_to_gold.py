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
    schedule_interval=None,
    catchup=False,
    tags=["lakehouse", "medallion", "gold"],
) as dag:

    # Gera uma task por objeto Gold (dimensões e fatos) a partir do registro central
    # (config/gold_objects.py). O script genérico constrói cada objeto aplicando a
    # regra específica definida em scripts/gold_rules.py.
    for objeto in GOLD_OBJECTS:
        SparkSubmitOperator(
            task_id=f"gold_{objeto['nome']}",
            conn_id="spark_default",
            application="/opt/pipelines/scripts/silver_to_gold.py",
            application_args=[objeto["nome"]],
            name=f"job-silver-to-gold-{objeto['nome']}",
            verbose=True,
        )
