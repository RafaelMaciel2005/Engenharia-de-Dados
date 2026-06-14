from datetime import datetime
import os
import glob
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from kaggle.api.kaggle_api_extended import KaggleApi

# CONFIGURAÇÕES FIXAS CENTRALIZADAS (Sem chaves expostas!)
DATASET_NAME = "olistbr/brazilian-ecommerce"
CONN_ID_MINIO = "minio_default"
BUCKET_LANDING = "landing-zone"
TMP_DIR = "/opt/airflow/tmp_kaggle"

def extrair_do_kaggle():
    print("Iniciando a autenticação nativa com o Kaggle via arquivo kaggle.json...")
    
    # A biblioteca KaggleApi lê automaticamente o arquivo mapeado pelo Docker
    api = KaggleApi()
    api.authenticate()

    # Garante que a pasta temporária de download existe
    os.makedirs(TMP_DIR, exist_ok=True)
    
    print(f"Baixando pacote completo do dataset: {DATASET_NAME}")
    api.dataset_download_files(dataset=DATASET_NAME, path=TMP_DIR, unzip=True)
    print("Download e descompactação de todos os CSVs concluídos com sucesso!")

def carregar_para_minio():
    hook = S3Hook(aws_conn_id=CONN_ID_MINIO)

    # Garante a existência do Bucket da Landing Zone no MinIO
    if not hook.check_for_bucket(bucket_name=BUCKET_LANDING):
        print(f"Criando o bucket {BUCKET_LANDING} no MinIO...")
        hook.create_bucket(bucket_name=BUCKET_LANDING)

    data_atual = datetime.now().strftime("%Y-%m-%d")

    # Localiza todos os CSVs descompactados na pasta temporária
    arquivos_csv = glob.glob(os.path.join(TMP_DIR, "*.csv"))
    
    if not arquivos_csv:
        print("Nenhum arquivo CSV encontrado para upload.")
        return

    # Envia arquivo por arquivo dinamicamente para o MinIO
    for caminho_local in arquivos_csv:
        nome_arquivo = os.path.basename(caminho_local)
        
        # Constrói o particionamento temporal dinâmico por arquivo
        caminho_minio = f"vendas/ingestion_date={data_atual}/{nome_arquivo}"

        print(f"Enviando {nome_arquivo} para a Landing Zone...")      
        hook.load_file(
            filename=caminho_local,
            key=caminho_minio,
            bucket_name=BUCKET_LANDING,
            replace=True
        )
        
        # Apaga o arquivo local imediatamente para liberar espaço no container
        os.remove(caminho_local)
        
    print("Todos os datasets foram sincronizados na Landing Zone!")
    
    # Limpeza preventiva: remove a pasta temporária vazia
    try:
        os.rmdir(TMP_DIR)
    except OSError:
        pass