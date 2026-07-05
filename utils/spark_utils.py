# Arquivo: spark_utils.py
from pyspark.sql import SparkSession

def create_spark_session(app_name="DefaultApp"):
    """Cria uma SparkSession já configurada para o MinIO local."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin_rafa") \
        .config("spark.hadoop.fs.s3a.secret.key", "admin_rafa") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()