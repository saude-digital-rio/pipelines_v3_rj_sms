# -*- coding: utf-8 -*-
from prefect import task

from pipelines.utils.google import get_latest_file_from_gcs


@task(name="Extração: Buscar CSV mais recente no bucket(GCS)")
def get_latest_csv_from_gcs(gcs_folder_uri: str) -> str:
  """
  A partir de um URI de pasta em um bucket do GCS, encontra o arquivo CSV
  mais recentemente adicionado nela, e retorna seu URI.
  """
  return get_latest_file_from_gcs(gcs_folder_uri=gcs_folder_uri, extension=".csv")
