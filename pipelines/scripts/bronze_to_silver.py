# Script generico da camada Silver: le o Parquet de uma tabela na Bronze,
# aplica a regra de limpeza/tipagem especifica daquela tabela e grava na Silver.
#
# A separacao de responsabilidades aqui e proposital:
#   - ESTE arquivo so orquestra o fluxo (ler -> tratar -> validar -> escrever)
#     e e identico para as 9 tabelas;
#   - a logica especifica de cada tabela (quais colunas tratar e como) vive em
#     silver_rules.py, uma funcao por tabela.
# Para adicionar uma tabela nova: 1 linha em config/tabelas_olist.py + 1 funcao
# em silver_rules.py. Este script nao muda.
import sys
from pyspark.sql.functions import current_timestamp
from utils.spark_utils import create_spark_session
from utils.validation_checks import executar_validacoes
from utils.validation_checksum import calcular_checksum
from utils.validation_manifest import gravar_manifest
from silver_rules import TRANSFORMACOES

def main(tabela):
    print(f"Iniciando processamento Spark: Bronze to Silver ({tabela})...")

    # Confere ANTES de abrir o Spark se existe regra de tratamento para a tabela.
    # Sem esta guarda, um nome errado viraria um KeyError seco mais adiante.
    if tabela not in TRANSFORMACOES:
        raise ValueError(f"Nenhuma regra de tratamento definida para '{tabela}' em silver_rules.py")

    spark = create_spark_session(f"Bronze-to-Silver-{tabela}")

    input_path = f"s3a://bronze/olist/{tabela}"
    output_path = f"s3a://silver/olist/{tabela}"

    print(f"Lendo dados da camada Bronze: {input_path}")
    df_bronze = spark.read.parquet(input_path)

    # Aplica a regra da tabela (ver silver_rules.py) e adiciona o timestamp de
    # criacao na Silver — mesmo papel do data_processamento_bronze na camada anterior.
    tratar = TRANSFORMACOES[tabela]
    df_silver = tratar(df_bronze).withColumn("dt_criacao_silver", current_timestamp()).cache()
    # ^ .cache() porque o DataFrame sera percorrido varias vezes na sequencia
    #   (count, checksum, validacoes e escrita).

    # Validacao ANTES da escrita: aqui e onde conferimos se a limpeza funcionou
    # (nulos em chave, duplicatas, valores fora do dominio...). Check falhou ->
    # RuntimeError -> task vermelha no Airflow -> Silver permanece intacta.
    total_linhas = df_silver.count()
    checksum = calcular_checksum(df_silver)
    resultados = executar_validacoes(df_silver, "silver", tabela, spark)
    gravar_manifest("silver", tabela, df_silver, total_linhas, checksum, resultados)

    print(f"Escrevendo dados tratados em: {output_path}")
    df_silver.write.mode("overwrite").parquet(output_path)
    df_silver.unpersist()

    print("Dados gravados com sucesso na camada Silver!")
    spark.stop()

if __name__ == "__main__":
    # A DAG envia o nome da tabela via application_args do SparkSubmitOperator
    main(sys.argv[1])
