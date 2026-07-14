# Script generico da camada Gold: constroi UM objeto do modelo dimensional
# (uma dimensao ou um fato) a partir de uma ou mais tabelas da Silver.
#
# Diferenca importante em relacao as camadas anteriores: Bronze e Silver sao
# 1 tabela de entrada -> 1 tabela de saida. Na Gold isso nao vale mais — um fato
# pode precisar de varias tabelas Silver (ex: fato_pedidos_itens junta orders e
# items). Por isso a config declara a LISTA de fontes de cada objeto, e a funcao
# de construcao recebe um dicionario {nome_da_fonte: DataFrame}.
import sys
from utils.spark_utils import create_spark_session
from utils.validation_checks import executar_validacoes
from utils.validation_checksum import calcular_checksum
from utils.validation_manifest import gravar_manifest
from config.gold_objects import GOLD_OBJECTS
from gold_rules import GOLD_BUILDERS

def main(objeto):
    print(f"Iniciando processamento Spark: Silver to Gold ({objeto})...")

    # Busca a configuracao do objeto no registro central, com mensagem clara
    # se o nome nao existir (mesma logica dos outros scripts do pipeline).
    config = None
    for o in GOLD_OBJECTS:
        if o["nome"] == objeto:
            config = o
            break
    if config is None:
        raise ValueError(f"Objeto '{objeto}' nao encontrado em config/gold_objects.py")

    # A config e a funcao de construcao vivem em arquivos separados; conferimos
    # que os dois estao cadastrados para este objeto antes de abrir o Spark.
    if objeto not in GOLD_BUILDERS:
        raise ValueError(f"Nenhuma funcao de construcao definida para '{objeto}' em gold_rules.py")

    spark = create_spark_session(f"Silver-to-Gold-{objeto}")

    # Le todas as tabelas Silver que este objeto precisa, montando o dicionario
    # que a funcao de construcao espera receber.
    dfs = {
        fonte: spark.read.parquet(f"s3a://silver/olist/{fonte}")
        for fonte in config["fontes"]
    }

    # Constroi a dimensao/fato aplicando a regra especifica (ver gold_rules.py).
    construir = GOLD_BUILDERS[objeto]
    df_gold = construir(dfs).cache()
    # ^ .cache() porque o DataFrame sera percorrido varias vezes na sequencia
    #   (count, checksum, validacoes e escrita).

    # Validacao ANTES da escrita. Na Gold rodam os checks mais rigorosos do
    # pipeline: unicidade do grao, integridade referencial contra a Silver e
    # row_count_equals_source — que detecta fan-out (multiplicacao silenciosa
    # de linhas em join, ver docs/desafios-tecnicos.md). Falhou -> RuntimeError
    # -> a Gold permanece intacta.
    total_linhas = df_gold.count()
    checksum = calcular_checksum(df_gold)
    resultados = executar_validacoes(df_gold, "gold", objeto, spark)
    gravar_manifest("gold", objeto, df_gold, total_linhas, checksum, resultados)

    output_path = f"s3a://gold/olist/{objeto}"
    print(f"Escrevendo objeto Gold em: {output_path}")
    df_gold.write.mode("overwrite").parquet(output_path)
    df_gold.unpersist()

    print("Objeto Gold gravado com sucesso!")
    spark.stop()

if __name__ == "__main__":
    # A DAG envia o nome do objeto via application_args do SparkSubmitOperator
    main(sys.argv[1])
