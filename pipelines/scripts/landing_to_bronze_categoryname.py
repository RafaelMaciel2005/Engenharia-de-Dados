from pyspark.sql.functions import current_timestamp
from utils.spark_utils import create_spark_session

def main():
    print("Iniciando processamento Spark: Landing to Bronze...")

    spark = create_spark_session("Landing-to-Bronze-categoryname")
    
    input_path = "s3a://landing-zone/vendas/*/product_category_name_translation.csv"
    output_path = "s3a://bronze/olist/category_name/"

    print(f"Lendo dados brutos de: {input_path}")
    df_landing = spark.read.option("header", "true").option("sep", ",").csv(input_path)
    
    df_bronze = df_landing.withColumn("data_processamento_bronze", current_timestamp())

    print(f"Escrevendo dados em formato parquet em: {output_path}")
    df_bronze.write.mode("overwrite").parquet(output_path)
    
    print("Processamento concluído com sucesso!")
    spark.stop()
       
if __name__ == "__main__":
    main()