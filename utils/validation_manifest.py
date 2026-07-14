# Arquivo: validation_manifest.py
#
# Grava um manifest.json com os metadados de cada execução do pipeline:
# quantidade de linhas, schema, checksum do conteúdo, resultado dos checks de
# qualidade e o timestamp de geração.
#
# O manifest fica no bucket "logs" do MinIO, espelhando o mesmo caminho do dado:
#   dado:     s3a://silver/olist/orders/*.parquet
#   manifest: s3a://logs/silver/olist/orders/manifest.json
#
# Para que serve na prática:
#   - auditar o que cada execução validou (os checks ficam registrados no JSON);
#   - comparar execuções: se o checksum de hoje é igual ao de ontem, o dado não
#     mudou — sem precisar reler e comparar os parquets;
#   - diagnosticar problemas: o schema gravado mostra exatamente com que colunas
#     e tipos a tabela foi escrita naquela rodada.
import json
import os
from datetime import datetime, timezone

import boto3


def _cliente_s3():
    # Conexão direta com o MinIO via boto3, usando as MESMAS variáveis de
    # ambiente que a SparkSession usa (ver utils/spark_utils.py).
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def _garantir_bucket_logs(s3):
    # Cria o bucket "logs" na primeira execução em um ambiente novo, para não
    # exigir nenhum passo manual de setup (mesma abordagem que o script de
    # extração usa com o bucket da landing-zone).
    buckets_existentes = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if "logs" not in buckets_existentes:
        s3.create_bucket(Bucket="logs")


def gravar_manifest(camada, tabela, df, total_linhas, checksum, resultados_validacao):
    # O schema vai como lista de "nome:tipo" legível (ex: "price:double") em vez
    # do objeto StructType do Spark — o manifest é para ser lido por gente,
    # não por máquina.
    manifest = {
        "camada": camada,
        "tabela": tabela,
        "linhas": total_linhas,
        "colunas": [f"{campo.name}:{campo.dataType.simpleString()}" for campo in df.schema.fields],
        "checksum": checksum,
        "validacoes": resultados_validacao,
        "gerado_em": datetime.now(timezone.utc).isoformat(),
    }

    s3 = _cliente_s3()
    _garantir_bucket_logs(s3)

    # Gravamos via boto3, e não via Spark, de propósito: é um arquivo único e
    # pequeno. Se usássemos df.write, o resultado seria uma PASTA com part-file
    # e _SUCCESS dentro — em vez de um manifest.json limpo e fácil de abrir.
    chave = f"{camada}/olist/{tabela}/manifest.json"
    s3.put_object(
        Bucket="logs",
        Key=chave,
        Body=json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"Manifest gravado em: s3a://logs/{chave}")
