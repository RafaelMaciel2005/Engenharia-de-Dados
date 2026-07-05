# 1. Imagem base leve com Java 17 (LTS)
FROM eclipse-temurin:17-jre-jammy

# 2. Definir variáveis de versão
ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3
# A versão do Hadoop AWS deve ser compatível com a versão do Hadoop embutida no Spark
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.262

# 3. Instalar dependências do sistema operacional
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    bash \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# 4. Baixar e instalar o Apache Spark
RUN curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# 5. Configurar variáveis de ambiente do Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3

# 6. Baixar Jars essenciais para comunicação com o MinIO (S3A)
# Esses jars são colocados diretamente na pasta de jars padrão do Spark
RUN wget  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar -P $SPARK_HOME/jars/ && \
    wget  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -P $SPARK_HOME/jars/

# Copiar o arquivo de dependências para dentro da imagem
COPY requirements.txt /tmp/

# Instalar as bibliotecas Python
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt    

# 7. Criar diretório de trabalho e usuário não-root (Boas práticas de segurança)
RUN useradd -ms /bin/bash sparkuser
RUN chown -R sparkuser:sparkuser /opt/spark

USER sparkuser
WORKDIR /opt/spark

# O comando padrão iniciará o bash, mas no docker-compose você substituirá
# chamando arquivos específicos de master/worker ou submetendo jobs.
CMD ["/bin/bash"]