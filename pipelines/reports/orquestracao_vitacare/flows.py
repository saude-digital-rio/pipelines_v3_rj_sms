# -*- coding: utf-8 -*-
from datetime import datetime, time, timedelta
from typing import Literal

from prefect import task

from pipelines.constants import CIT
from pipelines.datalake.extract_load.vitacare_historico.flows import vitacare_historico
from pipelines.datalake.migrate.gdrive_to_gcs.flows import gdrive_to_gcs
from pipelines.datalake.migrate.sqlserver_backup.flows import sqlserver_backup
from pipelines.datalake.transform.dbt.flows import sms_execute_dbt
from pipelines.utils.datetime import SAO_PAULO_TZ, now
from pipelines.utils.prefect import (
  create_flow_run,
  flow,
  flow_config,
  wait_for_flow_run_task,
)

from .constants import constants
from .tasks import schedule_next_runs
from .schedules import schedules


@flow(
  name="Orquestração: Vitacare Histórico",
  description="Executa sequencialmente: gdrive_to_gcs → sqlserver_backup → vitacare_historico",
  owners=[CIT.DANIEL_ID.value],
  tags=["CIT"],
)
def orquestracao_vitacare(
  environment: Literal["prod", "dev"] = "dev", should_repeat: Literal[True, False] = False
):
  """
  Args:
    environment(str):
      Ambiente de execução, "dev" (padrão) ou "prod".
    should_repeat(bool):
      Flag que indica se o flow deve se repetir para os próximos dias (5 dia).
  """

  if should_repeat:
    schedule_next_runs(environment=environment)

  # TODO: Descomentar antes de subir para prod 
  # # 1. gdrive_to_gcs
  # fr_gdrive = create_flow_run(
  #   flow=gdrive_to_gcs,
  #   parameters=constants.GDRIVE_TO_GCS_PARAMS.value,
  #   environment=environment,
  # )
  # wait_for_flow_run_task(flow_run_id=fr_gdrive.id)

  # # 2. sqlserver_backup
  # fr_sqlserver = create_flow_run(
  #   flow=sqlserver_backup,
  #   parameters=constants.SQLSERVER_BACKUP_PARAMS.value,
  #   environment=environment,
  # )
  # wait_for_flow_run_task(flow_run_id=fr_sqlserver.id)

  # # 3. vitacare_historico
  # fr_vitacare = create_flow_run(
  #   flow=vitacare_historico,
  #   parameters=constants.VITACARE_HISTORICO_PARAMS.value,
  #   environment=environment,
  # )
  # wait_for_flow_run_task(flow_run_id=fr_vitacare.id)

  # 4. Executa o dbt run com -s tag:vitacare_historico
  fr_dbt = create_flow_run(
    flow=sms_execute_dbt,
    parameters=constants.DBT_PARAMS.value

  )

_flows = [flow_config(flow=orquestracao_vitacare, schedules=schedules)]
