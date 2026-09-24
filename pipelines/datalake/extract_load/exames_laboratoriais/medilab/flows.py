# -*- coding: utf-8 -*-

from typing import Optional

import pandas as pd

from pipelines.datalake.extract_load.exames_laboratoriais.medilab.tasks import (
  get_latest_csv_from_gcs,
)
from pipelines.utils.cleanup import cleanup_columns_for_bigquery
from pipelines.utils.datalake import upload_df_to_datalake_task
from pipelines.utils.datetime import now, now_str
from pipelines.utils.google import download_file_from_bucket_task
from pipelines.utils.logger import log
from pipelines.utils.prefect import flow, flow_config

from .schedules import schedules


@flow(name="Extração: Relatórios Medilab")
def medilab_extraction(
  gcs_uri: str,
  dataset_id: str = "brutos_relatorios_medilab",
  table_id: str = "relatorios",
  environment: str = "dev",
) -> None:
  """
  Flow que baixa um CSV do bucket GCS, aplica transformações de limpeza nas colunase cria/alimenta uma tabela no BigQuery.
  """

  log(f"Iniciando processo para o ambiente '{environment}'.")

  # Automação Híbrida: Usa a Task local que consulta o utilitário do projeto
  if not gcs_uri.endswith(".csv"):
    log(f"Busca de forma automática o arquivo mais recente na pasta do bucket: {gcs_uri}")
    gcs_uri = get_latest_csv_from_gcs(gcs_folder_uri=gcs_uri)

  extracted_at = now_str()  # Data/hora de extração do arquivo do bucket
  local_csv_path: Optional[str] = None

  # Baixar o arquivo do Bucket para a máquina
  local_csv_path = download_file_from_bucket_task(gcs_uri=gcs_uri)
  log(f"Arquivo baixado temporariamente em: {local_csv_path}")

  df = pd.read_csv(local_csv_path, sep=",")  # Carrega os dados na memória para tratamento
  df = cleanup_columns_for_bigquery(
    df
  )  # Padroniza os nomes das colunas (remove espaços, acentos, etc.)

  extracted_at = now().strftime("%Y-%m-%d %H:%M:%S")

  # Linhagem de dados(sem os espaços extras para não falhar no Linter)
  df["arquivo_origem"] = gcs_uri
  df["data_carga"] = extracted_at
  df["periodo_referencia"] = now().strftime("%Y-%m-01")

  log(f"Colunas tratadas e metadados de rastreio adicionados: {list(df.columns)}")

  # Envia o dataframe com os dados limpos para a camada de staging/bruta
  upload_df_to_datalake_task(
    df=df,
    dataset_id=dataset_id,
    table_id=table_id,
    dump_mode="replace",
    source_format="csv",
    csv_delimiter=",",
  )

  log("Processo finalizado com sucesso!")


_flows = [flow_config(flow=medilab_extraction, schedules=schedules)]
