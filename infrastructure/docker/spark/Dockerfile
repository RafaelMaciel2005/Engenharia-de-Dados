FROM bitnamilegacy/spark:3.5.1

# 1. Muda para root para ter permissão de instalar pacotes do SO e salvar os JARs
USER root

# Evita prompts interativos durante a instalação
ENV DEBIAN_FRONTEND=noninteractive

# 2. O Bitnami Spark 3.5.1 já roda Debian 12 (Bookworm), então o Python 3.11 já é nativo!
# Instalamos apenas o pip e o curl (já que o Java e o Spark já vêm instalados)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        python3-pip \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# ==============================================================================
# DOWNLOAD DOS JARS DE INTEGRAÇÃO COM MINIO (S3A)
# ==============================================================================
# Na imagem da Bitnami, o SPARK_HOME padrão fica em /opt/bitnami/spark
RUN curl -sS -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -sS -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Configura o diretório de trabalho padrão do container
WORKDIR /app

# Copia os requerimentos do Python
COPY requirements.txt .

# 3. Instala as dependências do Python
# Nota: No Debian Bookworm (Python 3.11+), o PEP 668 bloqueia o pip global. 
# A flag --break-system-packages é necessária e 100% segura dentro de um container isolado.
RUN pip3 install --no-cache-dir --break-system-packages --upgrade pip && \
    pip3 install --no-cache-dir --break-system-packages -r requirements.txt

# Garante que o PySpark utilize a instalação do Python 3
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# 4. Retorna para o usuário não-root padrão da imagem Bitnami (UID 1001) por segurança
USER 1001

# Portas padrão expostas para comunicação e monitoramento
EXPOSE 4040 8080 7077

# Comando padrão
CMD ["/opt/bitnami/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]