from pyspark.sql.functions import current_timestamp, col, trim, upper, lower
from utils.spark_utils import create_spark_session

def main():

    spark = create_spark_session("Bronze-to-Silver-Olist")
    
    input_path = "s3a://bronze/olist/customers"
    output_path = "s3a://silver/olist/customers"

    print(f"Lendo dados da camada Bronze: {input_path}")

    # Lendo os dados de Bronze
    df_bronze = spark.read.parquet(input_path)

    #Tratamento dos dados
    df_silver = df_bronze \
        .withColumn("customer_id", trim(col("customer_id"))) \
        .withColumn("customer_unique_id", trim(col("customer_unique_id"))) \
        .withColumn("customer_zip_code_prefix", trim(col("customer_zip_code_prefix"))) \
        .withColumn("customer_city", lower(trim(col("customer_city")))) \
        .withColumn("customer_state", upper(trim(col("customer_state")))) \
        .withColumn("dt_criacao_silver", current_timestamp())
    
    #Salvando na camada Silver
    df_silver.write.mode("overwrite").parquet(output_path)
    print("Dados gravados com sucesso na camada Silver!")
    
if __name__ == "__main__":
    main()