FROM python:3.11-slim-bookworm

# Evita prompts interativos durante a instalação de pacotes
ENV DEBIAN_FRONTEND=noninteractive

# Instalação de dependências essenciais do sistema e Java 17 (obrigatório para Spark 3.5+)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        wget \
        procps \
        && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Configuração das Variáveis de Ambiente Globais (Java e Spark)
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME="/opt/spark"
ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

# Configuração do PYTHONPATH para garantir que o Python encontre as libs do PySpark
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

# Download e instalação do Apache Spark pré-compilado com Hadoop 3
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -O /tmp/spark.tgz && \
    mkdir -p /opt && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm /tmp/spark.tgz

# ==============================================================================
# DOWNLOAD DOS JARS DE INTEGRAÇÃO COM MINIO (S3A)
# ==============================================================================
RUN mkdir -p ${SPARK_HOME}/jars && \
    curl -sS -o ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -sS -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Configura o diretório de trabalho padrão do container
WORKDIR /app

# Copia e instala os requerimentos do Python (garantindo PySpark idêntico)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Portas padrão expostas para comunicação e monitoramento da UI do Spark
EXPOSE 4040 8080 7077

# Comando padrão (pode ser sobrescrito no docker-compose para subir como Master ou Worker)
CMD ["spark-class", "org.apache.spark.deploy.master.Master"]