# -*- coding: utf-8 -*-
from time import sleep

from prefect import get_client

from pipelines.constants import CIT
from pipelines.utils.logger import log
from pipelines.utils.prefect import create_flow_run, flow, flow_config, rename_flow_run
from pipelines.utils.state_handlers import handle_flow_state_change

from .constants import vitacare_constants
from .tasks import (
  extract_table_to_bigquery,
  get_cnes_from_bigquery,
  get_database_tables,
  start_cloudsql_instance,
  start_cloudsql_proxy,
  stop_cloudsql_instance,
  stop_cloudsql_proxy,
  validate_environment,
  write_log,
)


@flow(
  name="Extração: Vitacare Histórico - CNES",
  state_handlers=[handle_flow_state_change],
  owners=[CIT.DANIEL_ID.value],
)
def vitacare_historico_cnes(
  environment: str = "dev",
  cnes: str = None,
  table_names: list[str] = None,
  log_dataset_id: str = vitacare_constants.LOG_DATASET.value,
  log_table_id: str = vitacare_constants.LOG_TABLE.value,
):
  environment = validate_environment(environment=environment)
  rename_flow_run(new_name=f"{environment} - vitacare_historico - {cnes}")
  database_environment = "dev"

  if not table_names:
    table_names = get_database_tables(environment=environment)
  proxy_process_id = None
  results = []

  try:
    proxy_process_id = start_cloudsql_proxy(environment=database_environment)
    database_name = f"vitacare_historic_{cnes}"

    for table_name in table_names:
      result = extract_table_to_bigquery(
        database_name=database_name,
        environment=database_environment,
        cnes=cnes,
        table_name=table_name,
      )
      results.append(result)

  finally:
    if proxy_process_id:
      stop_cloudsql_proxy(process_id=proxy_process_id)
    if results:
      write_log(
        log_items=results,
        dataset_id=log_dataset_id,
        table_id=log_table_id,
        environment=environment,
      )

  total_failed = sum(1 for result in results if result["status"] == "failed")

  if total_failed > 0:
    log(f"{total_failed} extração(ões) falharam no CNES '{cnes}'")


@flow(
  name="Extração: Vitacare Histórico",
  state_handlers=[handle_flow_state_change],
  owners=[CIT.HERIAN_ID.value],
  tags=["CIT"],
)
def vitacare_historico(
  environment: str = "dev", cnes: str = None, table_name: str = None
):
  environment = validate_environment(environment=environment)
  rename_flow_run(new_name=f"{environment} - vitacare_historico")

  cnes_list = [cnes] if cnes else get_cnes_from_bigquery()
  table_names = (
    [table_name] if table_name else get_database_tables(environment=environment)
  )
  log_dataset_id = vitacare_constants.LOG_DATASET.value
  log_table_id = vitacare_constants.LOG_TABLE.value

  instance_started = False
  active_flow_runs = {}
  failed_flow_runs = []

  try:
    log(
      f"(vitacare_historico) preparando extração de {len(cnes_list)} CNES "
      f"e {len(table_names)} tabela(s)"
    )
    start_cloudsql_instance()
    instance_started = True

    pending_cnes = list(reversed(cnes_list))

    def submit_next_cnes() -> None:
      if not pending_cnes:
        return

      cnes_item = pending_cnes.pop()
      flow_run = create_flow_run(
        flow=vitacare_historico_cnes,
        flow_run_name=f"{environment} - vitacare_historico - {cnes_item}",
        parameters={
          "environment": environment,
          "cnes": cnes_item,
          "table_names": table_names,
          "log_dataset_id": log_dataset_id,
          "log_table_id": log_table_id,
        },
        environment=environment,
      )
      active_flow_runs[flow_run.id] = cnes_item

    while (
      pending_cnes
      and len(active_flow_runs) < vitacare_constants.CNES_CONCURRENCY_LIMIT.value
    ):
      submit_next_cnes()

    with get_client(sync_client=True) as client:
      while active_flow_runs:
        finished_flow_runs = []
        for flow_run_id, cnes_item in active_flow_runs.items():
          flow_run = client.read_flow_run(flow_run_id=flow_run_id)
          state = flow_run.state
          if not state or not state.is_final():
            continue

          finished_flow_runs.append((flow_run_id, cnes_item, state))

        for flow_run_id, cnes_item, state in finished_flow_runs:
          del active_flow_runs[flow_run_id]
          log(
            f"(vitacare_historico) flow run do CNES '{cnes_item}' finalizada "
            f"com estado '{state.name}'"
          )
          if not state.is_completed():
            failed_flow_runs.append((cnes_item, state.name))
          submit_next_cnes()

        if active_flow_runs:
          active_cnes = ", ".join(active_flow_runs.values())
          log(
            f"(vitacare_historico) aguardando {len(active_flow_runs)} CNES "
            f"ativo(s): {active_cnes}; {len(pending_cnes)} CNES restante(s)"
          )
          sleep(60)

  finally:
    if instance_started:
      stop_cloudsql_instance()

  if failed_flow_runs:
    raise RuntimeError(f"{len(failed_flow_runs)} CNES falharam: {failed_flow_runs}")


_flows = [
  flow_config(
    flow=vitacare_historico_cnes,
    schedules=[],
    dockerfile="./pipelines/datalake/extract_load/vitacare_historico/Dockerfile",
    memory="small",
  ),
  flow_config(
    flow=vitacare_historico,
    dockerfile="./pipelines/datalake/extract_load/vitacare_historico/Dockerfile",
    memory="small",
  ),
]
