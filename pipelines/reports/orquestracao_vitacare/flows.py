# -*- coding: utf-8 -*-
from typing import Literal

from pipelines.constants import CIT
from pipelines.datalake.extract_load.vitacare_historico.flows import vitacare_historico
from pipelines.datalake.migrate.sqlserver_backup.flows import sqlserver_backup
from pipelines.datalake.transform.dbt.flows import sms_execute_dbt
from pipelines.utils.prefect import (
  create_flow_run,
  flow,
  flow_config,
  wait_for_flow_run_task,
)

from .constants import constants
from .schedules import schedules
from .tasks import schedule_next_runs


@flow(
  name="Orquestração: Vitacare Histórico",
  description="Executa sequencialmente: gdrive_to_gcs → sqlserver_backup → vitacare_historico",
  owners=[CIT.HERIAN_ID.value],
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

  environment_params = {"environment": environment}
  if should_repeat:
    schedule_next_runs(environment=environment)

  # 1. sqlserver_backup
  sqlserver_backup_params = constants.SQLSERVER_BACKUP_PARAMS.value
  sqlserver_backup_params.update(environment_params)
  fr_sqlserver = create_flow_run(
    flow=sqlserver_backup, parameters=sqlserver_backup_params, environment=environment
  )
  wait_for_flow_run_task(flow_run_id=fr_sqlserver.id)

  # 2. vitacare_historico
  fr_vitacare = create_flow_run(
    flow=vitacare_historico, parameters=environment_params, environment=environment
  )
  wait_for_flow_run_task(flow_run_id=fr_vitacare.id)

  # 3. Executa o dbt run pra tag:vitacare_historico
  dbt_params = constants.DBT_PARAMS.value
  dbt_params.update({"environment": environment})
  create_flow_run(flow=sms_execute_dbt, parameters=dbt_params)


_flows = [flow_config(flow=orquestracao_vitacare, schedules=schedules)]
