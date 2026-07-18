# DAG mestre: roda o pipeline inteiro na ordem certa, uma camada por vez.
#
# As DAGs de camada (bronze, silver, gold) continuam existindo e podem ser
# disparadas sozinhas — util quando so uma regra da Silver mudou, por exemplo.
# Esta DAG existe para o caso "do zero ate a Gold": ela dispara cada DAG filha
# e espera terminar antes de liberar a proxima, entao uma falha em qualquer
# tabela para a corrente ali (nada de Gold construida em cima de Silver quebrada).
from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "rafa-dev",
    "start_date": datetime(2026, 1, 1),
}

with DAG(
    dag_id="pipeline_completo",
    default_args=default_args,
    schedule_interval=None,  # manual: o dataset e historico. Se um dia houver dado novo, o agendamento entra AQUI (e so aqui)
    catchup=False,
    tags=["lakehouse", "orquestracao"],
) as dag:

    def disparar(dag_filha):
        # As quatro tasks sao identicas fora o alvo, entao a criacao fica
        # centralizada aqui — qualquer ajuste de comportamento vale para todas.
        return TriggerDagRunOperator(
            task_id=f"rodar_{dag_filha}",
            trigger_dag_id=dag_filha,
            wait_for_completion=True,  # segura esta task ate a DAG filha terminar
            poke_interval=30,          # confere o status da filha a cada 30s
            reset_dag_run=True,        # re-rodar o mestre re-dispara a filha, mesmo ja existindo um run anterior
            allowed_states=["success"],
            failed_states=["failed"],  # filha falhou -> esta task falha -> as camadas seguintes nem comecam
        )

    extracao = disparar("kaggle_to_landing_zone")
    bronze = disparar("landing_to_bronze")
    silver = disparar("bronze_to_silver")
    gold = disparar("silver_to_gold")

    # A ordem e a propria arquitetura medalhao — cada camada le o que a anterior escreveu
    extracao >> bronze >> silver >> gold
