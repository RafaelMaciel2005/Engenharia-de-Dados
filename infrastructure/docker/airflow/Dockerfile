# ==============================================================================
# Dockerfile para Apache Airflow (Alinhado com Spark 3.5.1 e MinIO)
#
# A distribuicao completa do Spark (bin/sbin/jars) e necessaria aqui porque o
# SparkSubmitOperator chama o binario spark-submit de verdade — o pyspark do
# requirements.txt (instalado via pip) so cobre a API Python, nao o launcher.
#
# O binario vem da imagem OFICIAL apache/spark:3.5.1 (stage spark_dist) em vez
# de baixar o tarball do archive.apache.org: o mirror e lento (~180 KB/s daqui)
# e chegava a pendurar o build. A imagem oficial vem do Docker Hub (CDN rapido)
# e fixa o artefato oficial. Mesmo tratamento aplicado no Dockerfile do Spark
# (infrastructure/docker/spark/Dockerfile) — ver docs/desafios-tecnicos.md (nº 9).
# ==============================================================================

# 0. Stage so para extrair a distribuicao oficial do Spark 3.5.1
FROM apache/spark:3.5.1 AS spark_dist

FROM apache/airflow:2.11.0-python3.11

USER root

# 1. Instalação do Java 17 e utilitários (wget saiu: nao baixamos mais tarball).
#    procps entra porque a imagem base do Airflow tambem nao traz 'ps' — o
#    load-spark-env.sh do spark-submit chama 'ps' e reclamava (nao fatal, so log sujo).
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Configurações Globais
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV SPARK_HOME="/opt/spark"
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

# 3. Traz a distribuicao completa do Spark da imagem oficial (sem download lento)
COPY --from=spark_dist /opt/spark ${SPARK_HOME}

# 4. JARs de integração com o MinIO (S3A), do Maven Central (CDN rápido).
#    Flags de resiliencia: --retry recupera falha transiente; --speed-limit/--speed-time
#    abortam se a conexao travar (< 10KB/s por 30s) em vez de pendurar o build.
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262
RUN curl -fSL --retry 5 --retry-delay 5 --connect-timeout 30 --speed-limit 10240 --speed-time 30 \
    -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -fSL --retry 5 --retry-delay 5 --connect-timeout 30 --speed-limit 10240 --speed-time 30 \
    -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

RUN chown -R airflow:0 ${SPARK_HOME}

# 5. Instalação de Requisitos Python
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt
