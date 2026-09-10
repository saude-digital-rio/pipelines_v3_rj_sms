# -*- coding: utf-8 -*-
from prefect import task
from datetime import datetime, timedelta, time
from typing import Literal, Optional
from pipelines.constants import CIT
from pipelines.utils.datetime import now, SAO_PAULO_TZ
from pipelines.datalake.extract_load.vitacare_historico.flows import vitacare_historico
from pipelines.datalake.migrate.gdrive_to_gcs.flows import gdrive_to_gcs
from pipelines.datalake.migrate.sqlserver_backup.flows import sqlserver_backup
from pipelines.utils.prefect import (
  create_flow_run,
  flow,
  flow_config,
  wait_for_flow_run_task,
)

from .constants import constants
from .schedules import schedules

@task
def schedule_next_runs(environment):
  today = now().date()

  for i in range(1,6):
    next_date = today + timedelta(days=i)
    next_schedule = datetime.combine(next_date, time(hour=1, tzinfo=SAO_PAULO_TZ))
    
    create_flow_run(
      flow=orquestracao_vitacare,
      parameters={"environment": "prod", "should_repeat": False},
      environment=environment,
      scheduled_time=next_schedule
    )

@flow(
  name="Orquestração: Vitacare Histórico",
  description="Executa sequencialmente: gdrive_to_gcs → sqlserver_backup → vitacare_historico",
  owners=[CIT.DANIEL_ID.value],
  tags=["CIT"],
)
def orquestracao_vitacare(
  environment: Literal['prod', 'dev'] = 'dev',
  should_repeat: Literal[True, False] = False
):
  """
  Args:
    environment(str):
      Ambiente de execução, "dev" (padrão) ou "prod".
    should_repeat(bool):
      Flag que indica se o flow deve se repetir para os próximos dias (5 dia).
"""

  if should_repeat:
    today = now().date()
    for i in range(1,6):
      next_date = today + timedelta(days=i)
      next_schedule = datetime.combine(next_date, time(hour=1, tzinfo=SAO_PAULO_TZ))
      
      create_flow_run.run(
        flow=orquestracao_vitacare,
        parameters={"environment": "prod", "should_repeat": False},
        environment=environment,
        scheduled_time=next_schedule
      )

  # 1. gdrive_to_gcs
  fr_gdrive = create_flow_run(
    flow=gdrive_to_gcs,
    parameters=constants.GDRIVE_TO_GCS_PARAMS.value,
    environment=environment,
  )
  wait_for_flow_run_task(flow_run_id=fr_gdrive.id)

  # 2. sqlserver_backup
  fr_sqlserver = create_flow_run(
    flow=sqlserver_backup,
    parameters=constants.SQLSERVER_BACKUP_PARAMS.value,
    environment=environment,
  )
  wait_for_flow_run_task(flow_run_id=fr_sqlserver.id)

  # 3. vitacare_historico
  fr_vitacare = create_flow_run(
    flow=vitacare_historico,
    parameters=constants.VITACARE_HISTORICO_PARAMS.value,
    environment=environment,
  )
  wait_for_flow_run_task(flow_run_id=fr_vitacare.id)


_flows = [flow_config(flow=orquestracao_vitacare, schedules=schedules)]
