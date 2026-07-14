# Arquivo: spark_utils.py
#
# Ponto unico de criacao da SparkSession do projeto. Todos os scripts do pipeline
# e os notebooks importam create_spark_session() daqui — assim a configuracao de
# conexao com o MinIO existe em UM lugar so. Se o endpoint ou o formato de
# autenticacao mudar um dia, muda-se apenas este arquivo.
import os
from pyspark.sql import SparkSession

def create_spark_session(app_name="DefaultApp"):
    """Cria uma SparkSession já configurada para falar com o MinIO local via s3a://.

    As credenciais vêm de variáveis de ambiente (definidas no .env e repassadas
    pelo docker-compose aos containers). Nunca ficam hardcoded no código.
    """
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    # Falha cedo e com mensagem clara se as credenciais nao estiverem no ambiente.
    # Sem esta checagem, o Spark subiria normalmente e o erro so apareceria bem
    # mais tarde, no meio do job, como um "403 Forbidden" confuso do S3A.
    if not access_key or not secret_key:
        raise RuntimeError(
            "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY nao definidos no ambiente. "
            "Confira o .env e a secao 'environment' do docker-compose.yaml."
        )

    # Sobre as configs s3a abaixo (para quem nunca mexeu com MinIO + Spark):
    #   endpoint             -> para onde o protocolo s3a:// aponta (o container do
    #                            MinIO, e nao a AWS de verdade)
    #   path.style.access    -> MinIO enderessa bucket no caminho (http://host/bucket);
    #                            a AWS usa subdominio (http://bucket.host). Sem isso, 404.
    #   impl                 -> classe Java do conector S3A (vem dos JARs hadoop-aws
    #                            baixados no Dockerfile)
    #   connection.ssl false -> o MinIO local roda em http puro, sem certificado
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
