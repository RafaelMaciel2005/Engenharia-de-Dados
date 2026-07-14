# Extracao: baixa o dataset Olist do Kaggle e envia os CSVs crus para a
# Landing Zone no MinIO, particionados pela data de ingestao.
#
# Este e o UNICO script do pipeline que fala com o mundo externo (API do Kaggle).
# Tudo dali para frente (Bronze/Silver/Gold) le apenas do MinIO local — por isso
# qualquer reprocessamento das outras camadas nunca precisa passar por aqui.
from datetime import datetime
import os
import glob
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from kaggle.api.kaggle_api_extended import KaggleApi

# Configuracoes fixas centralizadas (sem chaves expostas — a credencial do Kaggle
# vem do kaggle.json montado pelo Docker, e a do MinIO da conexao do Airflow)
DATASET_NAME = "olistbr/brazilian-ecommerce"
CONN_ID_MINIO = "minio_default"
BUCKET_LANDING = "landing-zone"
TMP_DIR = "/opt/airflow/tmp_kaggle"

def extrair_do_kaggle():
    print("Iniciando a autenticação nativa com o Kaggle via arquivo kaggle.json...")

    # A biblioteca KaggleApi le automaticamente o kaggle.json que o docker-compose
    # monta em /home/airflow/.config/kaggle/ — nenhuma chave passa pelo codigo
    api = KaggleApi()
    api.authenticate()

    # Garante que a pasta temporária de download existe
    os.makedirs(TMP_DIR, exist_ok=True)

    print(f"Baixando pacote completo do dataset: {DATASET_NAME}")
    api.dataset_download_files(dataset=DATASET_NAME, path=TMP_DIR, unzip=True)
    print("Download e descompactação de todos os CSVs concluídos com sucesso!")

def carregar_para_minio():
    hook = S3Hook(aws_conn_id=CONN_ID_MINIO)

    # Cria o bucket na primeira execucao em ambiente novo (setup zero-manual)
    if not hook.check_for_bucket(bucket_name=BUCKET_LANDING):
        print(f"Criando o bucket {BUCKET_LANDING} no MinIO...")
        hook.create_bucket(bucket_name=BUCKET_LANDING)

    data_atual = datetime.now().strftime("%Y-%m-%d")

    # Localiza todos os CSVs descompactados na pasta temporária
    arquivos_csv = glob.glob(os.path.join(TMP_DIR, "*.csv"))

    if not arquivos_csv:
        print("Nenhum arquivo CSV encontrado para upload.")
        return

    for caminho_local in arquivos_csv:
        nome_arquivo = os.path.basename(caminho_local)

        # Particiona por data de ingestao (ingestion_date=YYYY-MM-DD). Cada rodada
        # de extracao cria sua propria particao — e o wildcard dos scripts da
        # Bronze le todas elas. Assim a Landing guarda o historico das extracoes.
        caminho_minio = f"vendas/ingestion_date={data_atual}/{nome_arquivo}"

        print(f"Enviando {nome_arquivo} para a Landing Zone...")
        hook.load_file(
            filename=caminho_local,
            key=caminho_minio,
            bucket_name=BUCKET_LANDING,
            replace=True  # re-rodar no mesmo dia sobrescreve a particao do dia (idempotente)
        )

        # Apaga o arquivo local imediatamente para liberar espaço no container
        os.remove(caminho_local)

    print("Todos os datasets foram sincronizados na Landing Zone!")

    # Limpeza final: remove a pasta temporaria. O rmdir so funciona em pasta
    # vazia — se sobrou algum arquivo nao-CSV do unzip, deixamos a pasta la
    # (nao vale a pena falhar a task por causa de lixo temporario).
    try:
        os.rmdir(TMP_DIR)
    except OSError:
        pass
