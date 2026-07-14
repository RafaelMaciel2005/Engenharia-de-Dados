# Arquivo: validation_checksum.py
from pyspark.sql import functions as F

def calcular_checksum(df):
    """Calcula um checksum determinístico do conteúdo de um DataFrame.

    Como funciona:
      1. xxhash64 gera um hash por linha, misturando todas as colunas.
      2. Somamos os hashes de todas as linhas em um único número.

    Por que a SOMA: ela torna o resultado independente da ORDEM das linhas.
    O Spark não garante ordem de leitura entre execuções, então duas leituras
    do mesmo dado em ordens diferentes precisam gerar o mesmo checksum — e com
    a soma, geram. Qualquer mudança em qualquer célula, porém, muda o resultado.

    Por que xxhash64 e não um UDF em Python: é uma função nativa do Spark, roda
    inteira na JVM dos executors. Além de ser muito mais rápida, evita depender
    do Python dos workers (o cluster tem um mismatch de versão Python entre
    driver e worker documentado em docs/desafios-tecnicos.md — código que roda
    só na JVM não é afetado por ele).

    Importante: não é hash criptográfico. Serve para detectar mudança ou
    corrupção ACIDENTAL entre execuções (comparando com o checksum do manifest
    anterior), não para proteger contra adulteração intencional.
    """
    linhas_com_hash = df.withColumn("_row_hash", F.xxhash64(*df.columns))
    resultado = linhas_com_hash.agg(F.sum("_row_hash").alias("checksum")).collect()[0]
    return str(resultado["checksum"])
