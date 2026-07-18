# ==============================================================================
# Imagem do Spark (master/worker) alinhada com o Python 3.11 do driver Airflow.
#
# Multi-stage: o binario do Spark vem da imagem OFICIAL apache/spark:3.5.1 (stage
# spark_dist) em vez de baixar o tarball do archive.apache.org — o mirror de
# arquivo e lento (~180 KB/s daqui) e chegava a pendurar o build. A imagem oficial
# vem do Docker Hub (CDN rapido) e fixa o artefato oficial, deixando o build
# rapido e reproduzivel. So os 2 JARs do S3A ainda sao baixados (Maven Central).
#
# Por que Python 3.11: antes a base era Ubuntu jammy, cujo python3 e 3.10 — o
# driver (Airflow) roda 3.11, entao driver e executor divergiam no minor e a
# serializacao do Spark quebrava em createDataFrame de lista local, UDF, toPandas.
# python:3.11 faz o python3 ser 3.11 nativamente. Ver docs/desafios-tecnicos.md (nº 9).
# ==============================================================================

# 1. Stage so para extrair a distribuicao do Spark 3.5.1 (com Hadoop 3.3.4 embutido)
FROM apache/spark:3.5.1 AS spark_dist

# 2. Runtime: Python 3.11 sobre Debian bookworm (mesmo minor de Python do driver)
FROM python:3.11-slim-bookworm

# Versoes dos JARs do S3A — devem casar com o Hadoop embutido no Spark (3.3.4)
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

# 3. Java 17 (mesma JDK e mesmo JAVA_HOME do Dockerfile do Airflow) + utilitarios.
#    procps entra porque a imagem slim nao traz 'ps', usado por scripts internos do Spark.
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 4. Traz a distribuicao completa do Spark da imagem oficial (sem download lento)
COPY --from=spark_dist /opt/spark /opt/spark

# 5. Variaveis do Spark. Com a base 3.11, PYSPARK_PYTHON=python3 ja resolve para o 3.11 —
#    driver e executor passam a rodar o mesmo minor de Python.
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3

# 6. JARs de integracao com o MinIO (S3A), do Maven Central (CDN rapido).
#    Flags de resiliencia: --retry recupera falha transiente; --speed-limit/--speed-time
#    abortam se a conexao travar (< 10KB/s por 30s) em vez de pendurar o build.
RUN curl -fSL --retry 5 --retry-delay 5 --connect-timeout 30 --speed-limit 10240 --speed-time 30 \
    -o $SPARK_HOME/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -fSL --retry 5 --retry-delay 5 --connect-timeout 30 --speed-limit 10240 --speed-time 30 \
    -o $SPARK_HOME/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# 7. Dependencias Python (a imagem base ja traz pip). Os wheels agora sao os de cp311.
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# 8. Usuario nao-root (boa pratica de seguranca)
RUN useradd -ms /bin/bash sparkuser && chown -R sparkuser:sparkuser /opt/spark
USER sparkuser
WORKDIR /opt/spark

# O comando padrao inicia o bash; o docker-compose substitui chamando master/worker
CMD ["/bin/bash"]
