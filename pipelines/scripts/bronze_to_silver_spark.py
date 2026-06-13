from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, trim

def main():
    #SparkSession limpa
    spark = SparkSession.builder \
        .appName("Bronze-to-Silver-Olist") \
        .getOrCreate()
    
    input_path = "s3a://bronze/olist/customers"
    output_path = "s3a//silver/olist/customers"

    print(f"Lendo dados da camada Bronze: {input_path}")
    df_bronze = spark.read.parquet(input_path)