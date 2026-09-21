# -*- coding: utf-8 -*-
from google.cloud import storage
from prefect import task

from pipelines.utils.logger import log

@task(name="Extração: Buscar CSV mais recente no bucket(GCS)")

def get_latest_csv_from_gcs(gcs_folder_uri: str) -> str:
  """
  Busca o CSV mais recente em uma pasta do bucket(GCS).
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
