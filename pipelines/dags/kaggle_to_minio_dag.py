from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import zipfile
import glob


#CONFIGURAÇÕES

KAGGLE_USERNAME = "rafaelmaciel05"
KAGGLE_KEY = "9981c53b487decdbebf9238da6a40d44"
DATASET_NAME = "olistbr/brazilian-ecommerce"
ARQUIVO_DATASET = "olist_customers_dataset.csv"

CONN_ID_MINIO = "minio_default"
BUCKET_LANDING = "landing-zone"
TMP_DIR = "/opt/airflow/tmp_kaggle"

def extrair_do_kaggle():
    #Garante que o diretório base exista com segurança
    os.makedirs('/home/airflow/.config/kaggle', exist_ok=True)
  
    os.environ["KAGGLE_USERNAME"] = KAGGLE_USERNAME
    os.environ["KAGGLE_KEY"] = KAGGLE_KEY
   
    from kaggle.api.kaggle_api_extended import KaggleApi

    # Inicializa e autentica na API do Kaggle
    api = KaggleApi()
    api.authenticate()

    #Cria diretório temporário e baixa o dataset
    os.makedirs(TMP_DIR, exist_ok=True)
    print(f"Baixando dataset {DATASET_NAME} do Kaggle...")

    #Baixa apenas o arquivo específico (descompanctando automaticamente)
    api.dataset_download_file(dataset=DATASET_NAME, file_name=ARQUIVO_DATASET, path=TMP_DIR, force=True, quiet=False)

    # procura por arquivos .zip baixados e descompacta
    for zip_file in glob.glob(os.path.join(TMP_DIR, "*.zip")):
        print(f"Detectado arquivo compactado: {zip_file}. Descompactando...")
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(TMP_DIR)
        os.remove(zip_file) # Remove o zip para limpar espaço

    print("Download concluído com sucesso!")

def carregar_para_minio():
    #Instancia o Hook do Airflow usando a conexão configurada no airflow webserver 
    hook = S3Hook(aws_conn_id=CONN_ID_MINIO)

    # Usando o metodo da hook para checar e criar buckets
    if not hook.check_for_bucket(bucket_name=BUCKET_LANDING):
        hook.create_bucket(bucket_name=BUCKET_LANDING)
        print(f"Bucket '{BUCKET_LANDING}' criado com sucesso.")

    # Define os caminhos
    caminho_local = os.path.join(TMP_DIR, ARQUIVO_DATASET)
    data_atual = datetime.now().strftime("%Y-%m-%d")
    caminho_minio = f"vendas/ingestion_date={data_atual}/{ARQUIVO_DATASET}"

    # Faz o upload usando o metodo nativo do Hook
    print(f"Enviando para o MinIO via S3Hook...")      
    hook.load_file(
        filename=caminho_local,
        key=caminho_minio,
        bucket_name=BUCKET_LANDING,
        replace=True # Se o arquivo já existir lá na mesma data, ele sobrescreve
    )
    print("Upload para a Landing Zone concluído!")

    #Limpeza do lixo local
    if os.path.exists(caminho_local):
        os.remove(caminho_local)

#===========================================================================================================        
# DEFINIÇÃO DA DAG
#===========================================================================================================

default_args={
    'owner': 'rafael',
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='kaggle_to_landing_zone',
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=['landing', "kaggle", "extract"]
)as dag:
    task_download = PythonOperator(
        task_id="extrair_do_kaggle",
        python_callable=extrair_do_kaggle
    )

    task_upload = PythonOperator(
        task_id="enviar_para_minio",
        python_callable=carregar_para_minio
    )

    task_download >> task_upload