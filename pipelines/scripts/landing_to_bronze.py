# Script generico da camada Bronze: le o CSV cru de uma tabela na Landing Zone
# e grava em Parquet na Bronze, sem transformar nada (Bronze preserva o original).
#
# E "generico" porque um unico script atende as 9 tabelas do dataset: a DAG
# (dag_landing_to_bronze.py) passa o nome da tabela como argumento, e o resto
# (arquivo de origem, opcoes de leitura) vem do registro central de configuracao.
import sys
from pyspark.sql.functions import current_timestamp
from utils.spark_utils import create_spark_session
from utils.validation_checks import executar_validacoes
from utils.validation_checksum import calcular_checksum
from utils.validation_manifest import gravar_manifest
from config.tabelas_olist import TABELAS_OLIST

def main(tabela):
    print(f"Iniciando processamento Spark: Landing to Bronze ({tabela})...")

    # Busca a configuracao da tabela no registro central. Se chegar um nome que
    # nao existe (erro de digitacao em uma DAG nova, por exemplo), paramos JA,
    # com mensagem clara — em vez de deixar estourar um erro generico la na frente.
    config = None
    for t in TABELAS_OLIST:
        if t["nome"] == tabela:
            config = t
            break
    if config is None:
        raise ValueError(f"Tabela '{tabela}' nao encontrada em config/tabelas_olist.py")

    spark = create_spark_session(f"Landing-to-Bronze-{tabela}")

    # O * no caminho le os CSVs de TODAS as particoes de ingestion_date de uma vez.
    # E isso que permite reprocessar a Bronze quantas vezes for preciso sem nunca
    # precisar baixar nada do Kaggle de novo (a extracao e uma etapa separada).
    input_path = f"s3a://landing-zone/vendas/*/{config['arquivo']}"
    output_path = f"s3a://bronze/olist/{tabela}/"

    print(f"Lendo dados brutos de: {input_path}")
    leitor = spark.read.option("header", "true").option("sep", ",")

    # Algumas tabelas precisam de opcoes extras de leitura. Exemplo real: o CSV
    # de reviews tem comentarios com quebra de linha e aspas DENTRO do texto —
    # sem multiLine/quote/escape, o Spark quebraria cada comentario em varios
    # registros invalidos. Essas opcoes ficam declaradas na config da tabela.
    for chave, valor in config.get("opcoes_csv", {}).items():
        leitor = leitor.option(chave, valor)

    df_landing = leitor.csv(input_path)

    # Bronze nao transforma o dado: so adiciona o timestamp de processamento,
    # que serve de rastro tecnico ("quando essa linha passou pela Bronze?").
    df_bronze = df_landing.withColumn("data_processamento_bronze", current_timestamp()).cache()
    # ^ .cache() porque este DataFrame sera percorrido varias vezes na sequencia
    #   (count, checksum, validacoes e escrita). Sem o cache, o Spark releria o
    #   CSV de origem a cada uma dessas acoes.

    # Validacao ANTES da escrita: se algum check falhar, executar_validacoes
    # levanta RuntimeError, o job morre aqui e o dado ruim nunca chega na Bronze.
    # O manifest registra a auditoria da execucao (linhas, schema, checksum, checks).
    total_linhas = df_bronze.count()
    checksum = calcular_checksum(df_bronze)
    resultados = executar_validacoes(df_bronze, "bronze", tabela, spark)
    gravar_manifest("bronze", tabela, df_bronze, total_linhas, checksum, resultados)

    print(f"Escrevendo dados em formato parquet em: {output_path}")
    df_bronze.write.mode("overwrite").parquet(output_path)
    df_bronze.unpersist()

    print("Processamento concluído com sucesso!")
    spark.stop()

if __name__ == "__main__":
    # A DAG envia o nome da tabela via application_args do SparkSubmitOperator
    main(sys.argv[1])
