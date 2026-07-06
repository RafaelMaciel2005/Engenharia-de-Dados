import sys
from pyspark.sql.functions import current_timestamp
from utils.spark_utils import create_spark_session
from silver_rules import TRANSFORMACOES

def main(tabela):
    print(f"Iniciando processamento Spark: Bronze to Silver ({tabela})...")

    spark = create_spark_session(f"Bronze-to-Silver-{tabela}")

    input_path = f"s3a://bronze/olist/{tabela}"
    output_path = f"s3a://silver/olist/{tabela}"

    print(f"Lendo dados da camada Bronze: {input_path}")
    df_bronze = spark.read.parquet(input_path)

    # Aplica a regra de tratamento específica da tabela (ver silver_rules.py)
    tratar = TRANSFORMACOES[tabela]
    df_silver = tratar(df_bronze).withColumn("dt_criacao_silver", current_timestamp())

    print(f"Escrevendo dados tratados em: {output_path}")
    df_silver.write.mode("overwrite").parquet(output_path)

    print("Dados gravados com sucesso na camada Silver!")
    spark.stop()

if __name__ == "__main__":
    # A DAG envia o nome da tabela via application_args
    main(sys.argv[1])
