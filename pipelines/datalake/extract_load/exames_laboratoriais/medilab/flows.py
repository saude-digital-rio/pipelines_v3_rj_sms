# -*- coding: utf-8 -*-

from typing import Optional

import pandas as pd

from pipelines.utils.cleanup import cleanup_columns_for_bigquery
from pipelines.utils.datalake import upload_df_to_datalake_task
from pipelines.utils.datetime import now_str
from pipelines.utils.google import download_file_from_bucket_task
from pipelines.utils.logger import log
from pipelines.utils.prefect import flow, flow_config
from google.cloud import storage


def get_latest_csv_from_gcs(gcs_folder_uri: str) -> str:
  """
  Busca o CSV mais recente em uma pasta do GCS.
  (Função mantida localmente no flow para evitar alterações no utils base do projeto).
  """
  # Parseamento seguro da URI (sem depender de utils externos que exigem nome de arquivo)
  path_without_gs = gcs_folder_uri.replace("gs://", "")
  parts = path_without_gs.split("/", 1)
  bucket_name = parts[0]
  prefix = parts[1] if len(parts) > 1 else ""

  client = storage.Client()
  bucket = client.get_bucket(bucket_name)

  # Garante que a busca seja feita dentro da pasta correta
  if prefix and not prefix.endswith("/"):
    prefix += "/"

  blobs = list(bucket.list_blobs(prefix=prefix))
  csv_blobs = [b for b in blobs if b.name.endswith(".csv")]

  if not csv_blobs:
    raise FileNotFoundError(f"Nenhum arquivo .csv encontrado em '{gcs_folder_uri}'")

  # Ordena pelo mais recente
  latest_blob = sorted(csv_blobs, key=lambda b: b.time_created, reverse=True)[0]
  latest_uri = f"gs://{bucket_name}/{latest_blob.name}"

  log(f"Arquivo mais recente encontrado: {latest_uri}")
  return latest_uri


@flow(name="Extração Relatórios Medilab")
def medilab_extraction(
  gcs_uri: str, dataset_id: str, table_id: str, environment: str = "dev"
) -> None:
  """
  Flow que baixa um CSV do bucket GCS, aplica transformações de limpeza nas colunase cria/alimenta uma tabela no BigQuery.
  """

  log(f"Iniciando processo para o ambiente '{environment}'.")

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

  # Linhagem de dados(sem os espaços extras para não falhar no Linter)
  df["arquivo_origem"] = gcs_uri
  df["data_carga"] = extracted_at

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


_flows = [flow_config(flow=medilab_extraction, schedules=[])]
