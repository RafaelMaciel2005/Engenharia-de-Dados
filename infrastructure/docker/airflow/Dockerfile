FROM apache/airflow:2.11.0-python3.11

# Switch para root para instalar dependências de sistema
USER root

# Install system dependencies
# Installs wget to download the file and unzip to extract it

RUN apt-get update && apt-get install -y wget unzip libaio1 && apt-get clean
    
# Create the Oracle directory
# Creates a standard folder in to hold the driver files

RUN mkdir -p /opt/oracle

# Download and Install Instant Client
RUN wget https://download.oracle.com/otn_software/linux/instantclient/1925000/instantclient-basic-linux.x64-19.25.0.0.0dbru.zip -O /tmp/instantclient.zip && \
    unzip /tmp/instantclient.zip -d /opt/oracle && \
    rm /tmp/instantclient.zip
        

# --- AJUSTES NECESSÁRIOS PARA O ORACLE ---
# 1. Configura o Shared Library Loader do sistema para reconhecer o diretório da Oracle
RUN echo /opt/oracle/instantclient_19_25 > /etc/ld.so.conf.d/oracle-instantclient.conf && ldconfig

# 2. Define a variável que o driver 'oracledb' (thick mode) e o Linux usam para carregar as libs
ENV LD_LIBRARY_PATH="/opt/oracle/instantclient_19_25:$LD_LIBRARY_PATH"
# -----------------------------------------

USER airflow

# Upgrade do pip para garantir compatibilidade com as versões 2026
RUN pip install --no-cache-dir --upgrade pip

# Instalação das dependências solicitadas
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
