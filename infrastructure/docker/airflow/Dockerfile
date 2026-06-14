# ==============================================================================
# Dockerfile para Apache Airflow (Alinhado com Spark 3.5.1 e MinIO)
# ==============================================================================
FROM apache/airflow:2.11.0-python3.11

USER root

# 1. Instalação do Java 17 e utilitários
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        wget \
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Configurações Globais
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV SPARK_VERSION="3.5.1"
ENV HADOOP_VERSION="3"
ENV SPARK_HOME="/opt/spark"
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

# 3. Download do Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -O /tmp/spark.tgz && \
    mkdir -p /opt && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm /tmp/spark.tgz

# 4. DOWNLOAD DOS JARS DE INTEGRAÇÃO COM MINIO (S3A) PARA O DRIVER
RUN mkdir -p ${SPARK_HOME}/jars && \
    curl -sS -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -sS -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

RUN chown -R airflow:0 ${SPARK_HOME}

# 5. Instalação de Requisitos Python
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt