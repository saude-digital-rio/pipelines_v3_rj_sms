# -*- coding: utf-8 -*-
import os
import datetime
import pandas as pd 
from typing import Optional

from pipelines.utils.prefect import flow
from pipelines.utils.logger import log

# 2. Importando a task que BAIXA do bucket
from pipelines.utils.google import download_file_from_bucket_task

# 3. Importando a task GENÉRICA OFICIAL que FAZ O UPLOAD para o BigQuery
from pipelines.utils.datalake import upload_df_to_datalake_task

# 4. Importando a task que limpa os nomes das colunas para o BigQuery
from pipelines.utils.cleanup import cleanup_columns_for_bigquery

@flow(name="Extract GCS to BigQuery CSV")
def extract_gcs_csv(
    gcs_uri: str, 
    dataset_id: str, 
    table_id: str,
    periodo_referencia: str,
    environment: str = "dev"
):
    """
    Flow que baixa um CSV do bucket GCS, aplica transformações de limpeza nas colunase cria/alimenta uma tabela no BigQuery.
    """
    
    log(f"Iniciando processo para o ambiente '{environment}'.")
    log(f"Origem: {gcs_uri} | Destino: {dataset_id}.{table_id}")

    local_csv_path = None

    # Baixar o arquivo do Bucket para a máquina
    # Retorna algo como '/tmp/data/seu_arquivo.csv'
    local_csv_path = download_file_from_bucket_task(gcs_uri=gcs_uri)

    log(f"Arquivo baixado temporariamente em: {local_csv_path}")

    df = pd.read_csv(local_csv_path, sep=",") # Carrega os dados na memória para tratamento
    df = cleanup_columns_for_bigquery(df) # Padroniza os nomes das colunas (remove espaços, acentos, etc.)

    # Linhagem de dados
    df ["arquivo_origem"] = gcs_uri # Salva o nome/ caminho do arquivo de origem
    df ["data_carga"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Salva o momento exato
    df ["periodo_referencia"] = periodo_referencia # Salva o período de referência do arquivo

    log(f"Colunas tratadas e metadados de rastreio adicionados: {list(df.columns)}")

    # Envia o dataframe com os dados limpos para a camada de staging/bruta
    upload_df_to_datalake_task(
        df=df,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="replace",   
        source_format="csv",
        csv_delimiter=","   
    )
    
    log("Processo finalizado com sucesso!")

    # Garante que o arquivo seja deletado da máquina mesmo se o flow falhar, evitando acúmulo de "lixo" no disco do servidor ou nuvem.
    if local_csv_path and os.path.exists(local_csv_path):
        try:
            os.remove(local_csv_path) 
            log(f"Limpeza concluida. Arquivo temporário removido.")
        except Exception as error:
                log(f"Aviso ao tentar remover arquivo temporário: {error}", level="Warning")