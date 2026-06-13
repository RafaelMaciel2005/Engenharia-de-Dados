FROM apache/airflow:2.11.0-python3.11

# Switch para root para instalar dependências de sistema e infraestrutura
USER root

# 1. Instalação de dependências do sistema unificadas
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget \
        unzip \
        libaio1 \
        openjdk-17-jdk-headless \
        curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Configuração e download do Oracle Instant Client
RUN mkdir -p /opt/oracle && \
    wget https://download.oracle.com/otn_software/linux/instantclient/1925000/instantclient-basic-linux.x64-19.25.0.0.0dbru.zip -O /tmp/instantclient.zip && \
    unzip /tmp/instantclient.zip -d /opt/oracle && \
    rm /tmp/instantclient.zip

RUN echo /opt/oracle/instantclient_19_25 > /etc/ld.so.conf.d/oracle-instantclient.conf && ldconfig

# 3. Download dos JARs do Spark + AWS (ATUALIZADO PARA AWS SDK V2)
RUN mkdir -p /opt/spark-jars && \
    curl -sS -o /opt/spark-jars/hadoop-aws-3.4.0.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar && \
    curl -sS -o /opt/spark-jars/bundle-2.35.4.jar https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar

# 4. Configuração Global de JARs (Apenas bibliotecas físicas)
RUN mkdir -p /etc/spark && \
    echo "spark.jars /opt/spark-jars/hadoop-aws-3.4.0.jar,/opt/spark-jars/bundle-2.35.4.jar" >> /etc/spark/spark-defaults.conf    

# 5. Configurações de Variáveis de Ambiente Globais
ENV SPARK_CONF_DIR="/etc/spark"
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV LD_LIBRARY_PATH="/opt/oracle/instantclient_19_25:$LD_LIBRARY_PATH"
ENV PATH="$JAVA_HOME/bin:$PATH"

# Retorna para o usuário padrão do Airflow para instalar os pacotes Python
USER airflow

# 6. Atualização de gerenciadores e instalação dos pacotes Python do projeto
RUN pip install --no-cache-dir --upgrade pip
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt