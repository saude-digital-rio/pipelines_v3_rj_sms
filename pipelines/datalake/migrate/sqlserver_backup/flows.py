# -*- coding: utf-8 -*-
from pipelines.constants import CIT

# from pipelines.utils.google import build_bucket_name
from pipelines.utils.logger import log
from pipelines.utils.prefect import flow, flow_config, rename_flow_run

from .constants import LOG_DATASET_ID, LOG_TABLE_ID
from .schedules import schedules
from .tasks import (
  list_backup_files,
  prepare_restore_plan,
  restore_backup,
  start_instance,
  stop_instance,
  write_log,
)


@flow(
  name="Migração: Backup SQL Server → Cloud SQL",
  description=(
    "Restaura backups de SQL Server (.BAK), guardados em um bucket do GCS, "
    "para uma instância Cloud SQL"
  ),
  owners=[CIT.HERIAN_ID.value],
  tags=["CIT"],
)
def sqlserver_backup(
  backup_type: str,
  bucket_name: str,
  file_pattern: str,
  instance_name: str = "vitacare",
  table_id: str = LOG_TABLE_ID,
  environment: str = "dev",
):
  rename_flow_run(new_name=f"{environment} - {backup_type}")

  # Houve uma mudança no bucket utilizado, com isso, essa dinstinção entre dev e prod n se faz necessário
  # Deixando aqui caso algum dia mude novamente (05/08/26)
  # resolved_bucket_name =  build_bucket_name(bucket_name=bucket_name, environment=environment)
  resolved_bucket_name = bucket_name
  results = []
  instance_started = False

  try:
    files = list_backup_files(bucket_name=resolved_bucket_name, file_pattern=file_pattern)
    restore_plan = prepare_restore_plan(
      files=files, bucket_name=resolved_bucket_name, backup_type=backup_type
    )

    if not restore_plan:
      log("(sqlserver_backup) nenhum backup para restaurar")
      return

    instance_started = True
    start_instance(instance_name=instance_name)

    for restore_item in restore_plan:
      result = restore_backup(restore_item=restore_item, instance_name=instance_name)
      results.append(result)

  finally:
    if instance_started:
      try:
        stop_instance(instance_name=instance_name)
      except Exception as exc:
        log(
          f"(sqlserver_backup) erro ao desligar instância '{instance_name}': {repr(exc)}",
          level="error",
        )

    if results:
      write_log(
        log_items=results,
        dataset_id=LOG_DATASET_ID,
        table_id=table_id,
        environment=environment,
      )

  total_success = sum(1 for result in results if result["status"] == "success")
  total_failed = sum(1 for result in results if result["status"] == "failed")

  log(
    f"(sqlserver_backup) processamento finalizado: "
    f"{total_success} sucesso(s), {total_failed} falha(s)"
  )

  if total_failed > 0:
    raise RuntimeError(f"{total_failed} restauração(ões) falharam")


_flows = [flow_config(flow=sqlserver_backup, schedules=schedules)]
