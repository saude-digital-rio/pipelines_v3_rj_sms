# -*- coding: utf-8 -*-
import datetime 
import os
from typing import Optional

import pandas as pd 

from pipelines.utils.cleanup import cleanup_columns_for_bigquery
from pipelines.utils.datalake import upload_df_to_datalake_task
from pipelines.utils.google import download_file_from_bucket_task
from pipelines.utils.logger import log
from pipelines.utils.prefect import flow

@flow(name="Extract GCS to BigQuery CSV")
def extract_gcs_csv(
    gcs_uri: str, 
    dataset_id: str, 
    table_id: str,
    periodo_referencia: str,
    environment: str = "dev"
) -> None: 
    
    """
    Flow que baixa um CSV do bucket GCS, aplica transformações de limpeza nas colunase cria/alimenta uma tabela no BigQuery.
    """
    
    log(f"Iniciando processo para o ambiente '{environment}'.")
    log(f"Origem: {gcs_uri} | Destino: {dataset_id}.{table_id}")

    local_csv_path: Optional[str] = None
    
    try:
        # Baixar o arquivo do Bucket para a máquina
        local_csv_path = download_file_from_bucket_task(gcs_uri=gcs_uri)
        log(f"Arquivo baixado temporariamente em: {local_csv_path}")

        df = pd.read_csv(local_csv_path, sep=",") # Carrega os dados na memória para tratamento
        df = cleanup_columns_for_bigquery(df) # Padroniza os nomes das colunas (remove espaços, acentos, etc.)

        # Linhagem de dados(sem os espaços extras para não falhar no Linter)
        df ["arquivo_origem"] = gcs_uri 
        df ["data_carga"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
        df ["periodo_referencia"] = periodo_referencia

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
        
    finally:    

        # Garante que o arquivo seja deletado da máquina mesmo se o flow falhar, evitando acúmulo de "lixo" no disco do servidor ou nuvem.
        if local_csv_path and os.path.exists(local_csv_path):
            try:
                os.remove(local_csv_path) 
                log("Limpeza concluida. Arquivo temporário removido.")
            except Exception as error:
                    log(f"Aviso ao tentar remover arquivo temporário: {error}", level="Warning")