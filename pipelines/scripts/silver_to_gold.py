import sys
from utils.spark_utils import create_spark_session
from config.gold_objects import GOLD_OBJECTS
from gold_rules import GOLD_BUILDERS

def main(objeto):
    print(f"Iniciando processamento Spark: Silver to Gold ({objeto})...")

    # Busca a configuração do objeto no registro central
    config = next(o for o in GOLD_OBJECTS if o["nome"] == objeto)

    spark = create_spark_session(f"Silver-to-Gold-{objeto}")

    # Lê todas as tabelas Silver que este objeto Gold precisa (uma ou mais)
    dfs = {
        fonte: spark.read.parquet(f"s3a://silver/olist/{fonte}")
        for fonte in config["fontes"]
    }

    # Aplica a regra de construção específica (dimensão ou fato), ver gold_rules.py
    construir = GOLD_BUILDERS[objeto]
    df_gold = construir(dfs)

    output_path = f"s3a://gold/olist/{objeto}"
    print(f"Escrevendo objeto Gold em: {output_path}")
    df_gold.write.mode("overwrite").parquet(output_path)

    print("Objeto Gold gravado com sucesso!")
    spark.stop()

if __name__ == "__main__":
    # A DAG envia o nome do objeto via application_args
    main(sys.argv[1])
