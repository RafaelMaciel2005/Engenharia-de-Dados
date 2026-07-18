# Configuracao compartilhada dos testes.
#
# Os testes rodam em tres lugares diferentes (maquina local, CI do GitHub e,
# se preciso, dentro dos containers), entao o sys.path e montado dinamicamente:
# se as pastas do projeto existirem ao lado de tests/, entram no path; se nao
# (ex: testes copiados para dentro de um container), o PYTHONPATH do ambiente
# e quem resolve os imports.
import os
import sys

import pytest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for pasta in [RAIZ, os.path.join(RAIZ, "pipelines"), os.path.join(RAIZ, "pipelines", "scripts")]:
    if os.path.isdir(pasta) and pasta not in sys.path:
        sys.path.insert(0, pasta)


@pytest.fixture(scope="session")
def spark():
    """SparkSession local para os testes.

    local[1] de proposito: driver e executor no MESMO processo Python, entao os
    testes nao dependem do cluster (nem sofrem o mismatch de versao Python entre
    driver e worker documentado em docs/desafios-tecnicos.md). Tambem nao usa
    create_spark_session() do projeto, porque os testes nao precisam de MinIO —
    tudo roda com DataFrames criados em memoria.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("testes-unitarios") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    yield spark
    spark.stop()
