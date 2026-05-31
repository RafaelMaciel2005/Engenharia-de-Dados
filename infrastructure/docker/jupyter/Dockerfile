FROM jupyter/pyspark-notebook:python-3.11

USER root

# Copia requirements
COPY requirements.txt /tmp/requirements.txt

# Instala dependências
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER jovyan
