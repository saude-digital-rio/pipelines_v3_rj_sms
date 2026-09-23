# -*- coding: utf-8 -*-
from typing import Literal, Optional

from pipelines.constants import CIT
from pipelines.utils.google import build_bucket_name
from pipelines.utils.logger import log
from pipelines.utils.prefect import flow, flow_config, rename_flow_run

from .schedules import schedules
from .tasks import (
  cleanup_downloaded_file,
  download_file,
  list_informes_files,
  prepare_files_for_upload,
  upload_file,
  write_log,
)

LOG_DATASET_ID = "controle_pipelines"


@flow(
  name="Migração: Infomes Mensais Vitacare (Google Drive)",
  description="Lista arquivos do Google Drive e faz upload para o GCS",
  owners=[CIT.HERIAN_ID.value],
  tags=["CIT"],
)
def informes_vitacare(
  root_folder_id: str,
  bucket_name: str,
  table_id: str = "log_gdrive_to_gcs",
  reference_month: Optional[str] = None,
  environment: Literal["dev", "prod"] = "dev",
):
  """
  Args:
    root_folder_id(str?):
      ID da pasta a ser extraída/migrada.
    bucket_name(str?):
      Nome do bucket no Google Cloud Storage onde serão inseridos os arquivos.
    table_id(str?):
      Nome da tabela de logging onde serão inseridos as informações do flow.
    reference_month(str?):
      Mês de referência dos informes a serem extraídos (Ex: "2026-09").
      Se não for definido, o mês de referência é o anterior.
    environment(str?):
      Ambiente de execução, "dev" (padrão) ou "prod".


  """
  log_items = []
  rename_flow_run(new_name=f"{environment} - {bucket_name}")

  resolved_bucket_name = build_bucket_name(
    bucket_name=bucket_name, environment=environment
  )

  try:
    files = list_informes_files(folder_id=root_folder_id, reference_month=reference_month)

    # Processamento sequencial para evitar muitos downloads/uploads simultâneos.
    for file in files:
      downloaded_file = download_file(file=file)
      prepared_files = prepare_files_for_upload(downloaded_file=downloaded_file)

      for prepared_file in prepared_files:
        result = upload_file(
          prepared_file=prepared_file, bucket_name=resolved_bucket_name
        )
        log_items.append(result)

      cleanup_downloaded_file(downloaded_file=downloaded_file)

  finally:
    if log_items:
      write_log(
        log_items=log_items,
        dataset_id=LOG_DATASET_ID,
        table_id=table_id,
        environment=environment,
      )

  total_success = sum(1 for log_item in log_items if log_item["status"] == "success")
  total_failed = sum(1 for log_item in log_items if log_item["status"] == "failed")

  log(
    f"(gdrive_to_gcs) processamento finalizado: "
    f"{total_success} sucesso(s), {total_failed} falha(s)"
  )


_flows = [flow_config(flow=informes_vitacare, schedules=schedules)]
