from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

def main():
    print("Iniciando processamento Spark: Landing to Bronze...")

    # SparkSession 100% limpa. 
    spark = SparkSession.builder \
        .appName("Landing-to-Bronze-Olist") \
        .getOrCreate()
    
    input_path = "s3a://landing-zone/vendas/*/olist_customers_dataset.csv"
    output_path = "s3a://bronze/olist/customers/"

    print(f"Lendo dados brutos de: {input_path}")
    df_landing = spark.read.option("header", "true").option("sep", ",").csv(input_path)
    
    df_bronze = df_landing.withColumn("data_processamento_bronze", current_timestamp())

    print(f"Escrevendo dados em formato parquet em: {output_path}")
    df_bronze.write.mode("overwrite").parquet(output_path)
    
    print("Processamento concluído com sucesso!")
    spark.stop()
       
if __name__ == "__main__":
    main()