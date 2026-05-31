FROM apache/spark:4.1.1

USER root

# Garante que a pasta de jars existe e tem as permissões corretas
RUN mkdir -p /opt/spark/jars

# Instala dependências extras e ferramentas de download
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# DOWNLOAD DOS JARS PARA S3 (Compatíveis com Spark 4.1.1 / Hadoop 3.4.x)
# 1. hadoop-aws: Conector oficial do Hadoop para S3
# 2. aws-java-sdk-bundle: Contém todas as classes do SDK da AWS necessárias
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.633/aws-java-sdk-bundle-1.12.633.jar -P /opt/spark/jars/

# Instala bibliotecas Python úteis
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Define permissões para os Jars baixados (evita problemas de leitura no container)
RUN chmod 644 /opt/spark/jars/hadoop-aws-3.4.0.jar && \
    chmod 644 /opt/spark/jars/aws-java-sdk-bundle-1.12.633.jar

# Volta para o usuário padrão do Spark (boa prática de segurança)
USER 1001
