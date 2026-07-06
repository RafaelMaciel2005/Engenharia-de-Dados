import sys
from pyspark.sql.functions import current_timestamp
from utils.spark_utils import create_spark_session
from config.tabelas_olist import TABELAS_OLIST

def main(tabela):
    print(f"Iniciando processamento Spark: Landing to Bronze ({tabela})...")

    # Busca a configuração da tabela no registro central
    config = next(t for t in TABELAS_OLIST if t["nome"] == tabela)

    spark = create_spark_session(f"Landing-to-Bronze-{tabela}")

    # O * no caminho lê os CSVs de todas as partições de ingestion_date
    input_path = f"s3a://landing-zone/vendas/*/{config['arquivo']}"
    output_path = f"s3a://bronze/olist/{tabela}/"

    print(f"Lendo dados brutos de: {input_path}")
    leitor = spark.read.option("header", "true").option("sep", ",")

    # Aplica opções extras de leitura, se a tabela precisar (ex: reviews tem texto multilinha)
    for chave, valor in config.get("opcoes_csv", {}).items():
        leitor = leitor.option(chave, valor)

    df_landing = leitor.csv(input_path)

    # Bronze preserva o dado original, adicionando apenas o timestamp de processamento
    df_bronze = df_landing.withColumn("data_processamento_bronze", current_timestamp())

    print(f"Escrevendo dados em formato parquet em: {output_path}")
    df_bronze.write.mode("overwrite").parquet(output_path)

    print("Processamento concluído com sucesso!")
    spark.stop()

if __name__ == "__main__":
    # A DAG envia o nome da tabela via application_args
    main(sys.argv[1])
