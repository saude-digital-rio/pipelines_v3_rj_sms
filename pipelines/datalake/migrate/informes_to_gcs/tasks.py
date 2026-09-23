# -*- coding: utf-8 -*-
import os
import posixpath
import shutil
import zipfile
from datetime import timedelta

from prefect.context import FlowRunContext

from pipelines.utils.datalake import update_logs_to_datalake
from pipelines.utils.datetime import now, parse_date_or_today
from pipelines.utils.env import get_prefect_url
from pipelines.utils.google import (
  download_google_drive_file,
  list_google_drive_files,
  list_google_drive_folder,
  upload_to_cloud_storage,
)
from pipelines.utils.io import create_tmp_data_folder
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


@task
def list_informes_files(folder_id: str, reference_month: str = None) -> list[dict]:
  # Dentro da pasta raiz (INFORMES-MENSAIS-ETSN) há pastas para cada AP
  # Dentro de cada pasta de uma AP há pastas referentes a meses (Ex: 2026-09)
  # Dentro de cada pasta de um mês há subpastas para diferentes temas/arquivos
  # Root Folder -> APs -> mes_referencia -> Temas -> Files

  if not reference_month:
    log(
      "Mês de referência não fornecido. Utilizando mês anterior como referência",
      level="warning",
    )
    current_date = now()
    reference_date = current_date - timedelta(weeks=4)
    reference_month = reference_date.strftime("%Y-%m")

  log(f"Mês de referência: {reference_month}")

  aps = list_google_drive_folder(folder_id=folder_id)
  all_files = []

  # Pastas de cada AP
  for ap in aps:
    current_folder_ap = ap.get("id", None)
    ap_name = ap.get("name", None)
    if current_folder_ap:
      log(f"Listando pastas e arquivos em {ap_name}")

      # Lista as pastas de mes-referência dentro da pasta da AP
      month_folders = list_google_drive_folder(folder_id=current_folder_ap)
      month_folder = [
        folder for folder in month_folders if folder.get("name") == reference_month
      ]

      if month_folder[0].get("id", None):
        # Usa list_google_drive_files para utilizar lógica recursiva
        files = list_google_drive_files(folder_id=month_folder[0].get("id", None))
        all_files.extend(files)
  return all_files


@task
def download_file(file: dict) -> dict:
  tmp_path = None
  try:
    source_path = file.get("relative_path") or file.get("name")
    tmp_path = create_tmp_data_folder(prefix="gdrive")
    destination_path = os.path.join(tmp_path, source_path)
    local_path = download_google_drive_file(
      file_id=file["id"], destination_path=destination_path
    )
    return {
      "file": file,
      "local_path": local_path,
      "tmp_path": tmp_path,
      "status": "success",
      "error_message": None,
    }
  except Exception as exc:
    source_path = file.get("relative_path") or file.get("name")
    log(f"Erro baixando arquivo '{source_path}': {repr(exc)}", level="error")
    return {
      "file": file,
      "local_path": None,
      "tmp_path": tmp_path,
      "status": "failed",
      "error_message": str(exc),
    }


@task
def prepare_files_for_upload(downloaded_file: dict) -> list[dict]:
  file = downloaded_file["file"]
  source_path = file.get("relative_path") or file.get("name")
  source_file_name = file.get("name")

  if downloaded_file["status"] == "failed":
    return [
      {
        "source_path": source_path,
        "source_file_name": source_file_name,
        "local_path": None,
        "gcs_blob_path": None,
        "gcs_uri": None,
        "status": "failed",
        "error_message": downloaded_file["error_message"],
      }
    ]

  local_path = downloaded_file["local_path"]

  if not source_file_name.lower().endswith(".zip"):
    return [
      {
        "source_path": source_path,
        "source_file_name": source_file_name,
        "local_path": local_path,
        "gcs_blob_path": source_path,
        "gcs_uri": None,
        "status": "ready",
        "error_message": None,
      }
    ]

  try:
    extracted_dir = f"{local_path}_extracted"

    with zipfile.ZipFile(local_path, "r") as zip_file:
      inner_files = [info for info in zip_file.infolist() if not info.is_dir()]
      zip_file.extractall(extracted_dir)

    prepared_files = []
    zip_parent_path = posixpath.dirname(source_path)

    for inner_file in inner_files:
      inner_path = inner_file.filename.replace("\\", "/").strip("/")
      local_inner_path = os.path.join(extracted_dir, *inner_path.split("/"))
      # Arquivos internos sobem sem incluir o nome do ZIP no caminho do GCS.
      gcs_blob_path = posixpath.join(zip_parent_path, inner_path)

      prepared_files.append(
        {
          "source_path": posixpath.join(source_path, inner_path),
          "source_file_name": posixpath.basename(inner_path),
          "local_path": local_inner_path,
          "gcs_blob_path": gcs_blob_path,
          "gcs_uri": None,
          "status": "ready",
          "error_message": None,
        }
      )

    return prepared_files

  except zipfile.BadZipFile as exc:
    log(f"ZIP corrompido '{source_path}': {repr(exc)}", level="error")
    return [
      {
        "source_path": source_path,
        "source_file_name": source_file_name,
        "local_path": local_path,
        "gcs_blob_path": None,
        "gcs_uri": None,
        "status": "failed",
        "error_message": f"ZIP corrompido: {exc}",
      }
    ]

  except Exception as exc:
    log(f"Erro extraindo ZIP '{source_path}': {repr(exc)}", level="error")
    return [
      {
        "source_path": source_path,
        "source_file_name": source_file_name,
        "local_path": local_path,
        "gcs_blob_path": None,
        "gcs_uri": None,
        "status": "failed",
        "error_message": str(exc),
      }
    ]


@task
def upload_file(prepared_file: dict, bucket_name: str) -> dict:
  if prepared_file["status"] == "failed":
    timestamp = now().isoformat()
    prepared_file["started_at"] = timestamp
    prepared_file["finished_at"] = timestamp
    return prepared_file

  prepared_file["started_at"] = now().isoformat()

  try:
    blob_prefix = posixpath.dirname(prepared_file["gcs_blob_path"]) or None
    upload_to_cloud_storage(
      path=prepared_file["local_path"], bucket_name=bucket_name, blob_prefix=blob_prefix
    )

    prepared_file["status"] = "success"
    prepared_file["gcs_uri"] = f"gs://{bucket_name}/{prepared_file['gcs_blob_path']}"
    prepared_file["finished_at"] = now().isoformat()
    return prepared_file

  except Exception as exc:
    log(
      f"Erro enviando arquivo '{prepared_file['source_path']}': {repr(exc)}",
      level="error",
    )
    prepared_file["status"] = "failed"
    prepared_file["error_message"] = str(exc)
    prepared_file["finished_at"] = now().isoformat()
    return prepared_file


@task
def cleanup_downloaded_file(downloaded_file: dict) -> None:
  tmp_path = downloaded_file.get("tmp_path")

  if tmp_path and os.path.exists(tmp_path):
    log(f"Apagando arquivos temporários em '{tmp_path}'")
    shutil.rmtree(tmp_path, ignore_errors=True)


@task
def write_log(
  log_items: list[dict], dataset_id: str, table_id: str, environment: str
) -> dict:
  if not log_items:
    return {"inserted_rows": 0}

  flow_run_id = None
  flow_run_url = None
  flow_run_context = FlowRunContext.get()

  if flow_run_context:
    flow_run_id = str(flow_run_context.flow_run.id)
    flow_run_url = f"{get_prefect_url()}/runs/flow-run/{flow_run_id}"

  fallback_timestamp = now().isoformat()
  rows = []
  for log_item in log_items:
    timestamp = log_item.get("finished_at") or fallback_timestamp
    rows.append(
      {
        "flow_run_id": flow_run_id,
        "flow_run_url": flow_run_url,
        "source_path": log_item["source_path"],
        "source_file_name": log_item["source_file_name"],
        "gcs_uri": log_item["gcs_uri"],
        "status": log_item["status"],
        "error_message": log_item["error_message"],
        "timestamp": timestamp,
        "data_particao": parse_date_or_today(timestamp).date().isoformat(),
      }
    )

  return update_logs_to_datalake(
    logs=rows, dataset_id=dataset_id, table_id=table_id, environment=environment
  )
