# -*- coding: utf-8 -*-
import asyncio
import re
import time
import unicodedata
from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Type, Union
from uuid import UUID

from prefect import State, Task, get_client
from prefect.client.schemas import FlowRun, StateType
from prefect.client.schemas.actions import GlobalConcurrencyLimitUpdate
from prefect.client.schemas.filters import FlowRunFilter
from prefect.context import FlowRunContext
from prefect.deployments.flow_runs import run_deployment
from prefect.exceptions import ObjectNotFound
from prefect.flows import Flow as OriginalFlow
from prefect.flows import FlowDecorator as OriginalFlowDecorator
from prefect.futures import PrefectFuture
from prefect.schedules import Schedule
from prefect.task_runners import TaskRunner

from pipelines.utils.env import get_current_environment, get_prefect_url, is_dev_run
from pipelines.utils.infisical import inject_bd_credentials
from pipelines.utils.logger import log

#################
## FLOWS
#################


class Flow(OriginalFlow):
  def __init__(
    self,
    *args,
    owners: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    **kwargs,
  ):
    # Guarda listas de owners, tags como atributos
    self.owners = owners or []
    self.tags = tags or []
    # Continua chamando o __init__ do Flow original
    super().__init__(*args, **kwargs)

  def get_owners(self):
    return self.owners


class FlowDecorator(OriginalFlowDecorator):
  name: str = None
  description: str = ""

  state_handlers: List[Callable] = None
  on_crashed: List[Callable] = None
  on_cancellation: List[Callable] = None
  task_runner: Union[
    Type[TaskRunner[PrefectFuture[Any]]], TaskRunner[PrefectFuture[Any]], None
  ] = None
  # ...

  owners: List[str] = None
  tags: List[str] = None
  log_prints: bool = False

  def __init__(
    self,
    *args,
    name: Optional[str] = None,
    description: str = "",
    state_handlers: Optional[List[Callable]] = None,
    on_crashed: Optional[List[Callable]] = None,
    on_cancellation: Optional[List[Callable]] = None,
    task_runner: Union[
      Type[TaskRunner[PrefectFuture[Any]]], TaskRunner[PrefectFuture[Any]], None
    ] = None,
    owners: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    log_prints: bool = False,
    **kwargs,
  ):
    """
    Args:
      name(str?): Nome do flow, aparece na UI
      description(str?): Descrição do flow, aparece (escondido) na UI
      state_handlers(List[Callable]?):
        Funções que executam em toda transição de estado do flow
      on_crashed(List[Callable]?):
        Funções que executam quando o flow entra em estado 'Crashed'
      on_cancellation(List[Callable]?):
        Funções que executam quando o flow é cancelado
      owners(List[str]?):
        Lista de IDs do Discord de 'donos' do flow
      tags(List[str]?):
        Tags para melhor categorizar os flows
      log_prints(bool?):
        Parâmetro do @flow original, supostamente redireciona print()s
        comuns aos logs da execução; acho que não funciona :3
    """
    self.name = name
    self.description = description or ""

    # Importação no meio do código porque senão dá erro de importação circular :(
    from pipelines.utils.state_handlers import handle_flow_state_change

    self.state_handlers = list(set([handle_flow_state_change, *(state_handlers or [])]))
    self.on_crashed = list(set([*(on_crashed or []), *self.state_handlers]))
    self.on_cancellation = list(set([*(on_cancellation or []), *self.state_handlers]))

    self.task_runner = task_runner

    self.owners = owners or []
    self.tags = tags or []
    self.log_prints = log_prints

  def __call__(self, *args, **kwargs):
    return Flow(
      *args,
      name=self.name,
      description=self.description,
      owners=self.owners,
      tags=self.tags,
      log_prints=self.log_prints,
      on_completion=[*self.state_handlers],
      on_failure=[*self.state_handlers],
      on_cancellation=[*self.on_cancellation],
      on_crashed=[*self.on_crashed],
      on_running=[*self.state_handlers],
      task_runner=self.task_runner,
      validate_parameters=False,
      **kwargs,
    )


flow = FlowDecorator


#################
## TASKS
#################


def authenticated_task(
  fn: Callable = None, **task_init_kwargs: Any
) -> Union[Task, Callable[[Callable], Task]]:
  """
  Função que injeta credenciais no ambiente de uma Task.
  Se `fn` estiver definido, retorna uma instância `Task(fn=fn, **task_init_kwargs)`.
  Se `fn` for None, retorna um decorator que pode ser usado para tornar uma função
  em uma Task autenticada. Ex.:

  ```python
  from pipelines.utils.prefect import authenticated_task as task

  @task(...)
  def xxxxx():
    # ...
  ```
  """

  def inject_credential_setting_in_function(function):

    def new_function(**kwargs):
      env = get_current_environment()
      log(f"[Injected] Configurando credenciais BD para ambiente {env}", level="debug")
      inject_bd_credentials(environment=env)
      log("[Injected] Executando task normalmente...", level="debug")
      return function(**kwargs)

    new_function.__name__ = function.__name__
    return new_function

  # Instância de Task
  if fn is not None:
    return Task(fn=inject_credential_setting_in_function(fn), **task_init_kwargs)
  # Decorator
  return lambda any_function: Task(
    fn=inject_credential_setting_in_function(any_function), **task_init_kwargs
  )


def create_flow_run(
  flow: Flow,
  parameters: dict = None,
  wait: bool = False,
  environment: str | None = None,
  flow_run_name: str = None,
  scheduled_time: datetime | None = None,
):
  """
  Cria uma nova flow run de um determinado flow.
  Args:
    flow(Flow):
      O flow a ser executada.
    parameters(dict?):
      Parâmetros do flow.
    wait(bool?):
      Se deve esperar o flow terminar, ou retornar imediatamente.
      Por padrão, não espera.
    environment(str?):
      Ambiente de execução; se "prod", executa o deployment de
      produção; se "dev", executa o deployment de staging.
  """
  environment = environment or get_current_environment()

  deployment_name = f"{flow.name}/{flow.name}" + (
    "" if environment == "prod" else " (stg)"
  )
  log(f"[create_flow_run] Requisitando execução de flow '{deployment_name}'...")
  flow_run = run_deployment(
    name=deployment_name,
    flow_run_name=flow_run_name,
    parameters=parameters,
    timeout=(0 if not wait else None),
    as_subflow=False,  # tenho recebido erro 422 sem isso aqui --Avellar
    scheduled_time=scheduled_time,
  )
  base_url = get_prefect_url()
  log(
    f"[create_flow_run] Flow run criada; confira em: {base_url}/runs/flow-run/{flow_run.id}"
  )
  return flow_run


def wait_for_flow_run(
  flow_run_id: UUID, timeout_seconds: int | None = None, raise_if_timeout: bool = True
):
  """
  Aguarda uma execução de flow terminar, seja com sucesso ou erro.

  Args:
    flow_run_id(UUID): O ID da FlowRun a ser esperada.
    timeout_seconds(int?):
      Tempo máximo a esperar o fim da FlowRun, em segundos.
    raise_if_timeout(bool?):
      Flag indicando se deve disparar erro caso o tempo
      máximo expire; True por padrão.
  Returns:
    out(bool):
      Retorna True quando a FlowRun termina. Em caso de
      timeout_seconds definido e raise_if_timeout=False,
      retorna False caso o tempo máximo expire.
  """
  log(f"Esperando execução de Flow Run com ID [{str(flow_run_id)[:8]}...] terminar...")
  with get_client(sync_client=True) as client:
    start_time = time.monotonic()
    # Desculpa eu sei que é feio mas é literalmente a implementação
    # do próprio Prefect, quase copiada e colada
    while True:
      # Confere o status atual da flow run
      updated_flow_run = client.read_flow_run(flow_run_id)
      flow_state = updated_flow_run.state
      # Se terminou, sucesso
      if flow_state and flow_state.is_final():
        return True
      # Se tivemos timeout
      if (
        timeout_seconds is not None and (time.monotonic() - start_time) >= timeout_seconds
      ):
        if raise_if_timeout:
          raise TimeoutError(
            "Tempo máximo de espera por flow run excedido! "
            f"flow_run.id='{flow_run_id}', "
            f"flow_run.state='{updated_flow_run.state}'"
          )
        return False
      time.sleep(15)  # Espera entre conferências de status
  # :)


@authenticated_task
def wait_for_flow_run_task(
  flow_run_id: UUID, timeout_seconds: int | None = None, raise_if_timeout: bool = True
):
  """
  Aguarda uma execução de flow terminar, seja com sucesso ou erro.

  Args:
    flow_run_id(UUID): O ID da FlowRun a ser esperada.
    timeout_seconds(int?):
      Tempo máximo a esperar o fim da FlowRun, em segundos.
    raise_if_timeout(bool?):
      Flag indicando se deve disparar erro caso o tempo
      máximo expire; True por padrão.
  Returns:
    out(bool):
      Retorna True quando a FlowRun termina. Em caso de
      timeout_seconds definido e raise_if_timeout=False,
      retorna False caso o tempo máximo expire.
  """
  return wait_for_flow_run(
    flow_run_id=flow_run_id,
    timeout_seconds=timeout_seconds,
    raise_if_timeout=raise_if_timeout,
  )


@authenticated_task
def rename_flow_run(new_name: str):
  ctx = FlowRunContext.get()
  if not ctx:
    log(
      "[rename_flow_run] Erro ao renomear flow_run; FlowRunContext inexistente",
      level="warning",
    )
    return
  if not ctx.flow_run:
    log(
      "[rename_flow_run] Erro ao renomear flow_run; FlowRunContext.flow_run inexistente",
      level="warning",
    )
    return

  async def update():
    async with get_client() as client:
      await client.update_flow_run(flow_run_id=ctx.flow_run.id, name=new_name)

  asyncio.run(update())


def flow_config(
  flow: Flow,
  schedules: list[Schedule] = None,
  dockerfile: str = None,
  memory: Literal["small", "medium", "large"] = "small",
  mount_gcs: bool = False,
  region: Optional[Literal["bra"]] = None,
) -> dict:
  """
  Retorna uma configuração de flow, a ser usada na variável
  `_flows`: `_flows = [ flow_config(...), ... ]`

  Args:
    flow(Flow):
      O flow a ser executado.
    schedules(list[Schedule]?):
      Lista de schedules para o flow; pode ser vazia/None.
    dockerfile(str?):
      Caminho do Dockerfile customizado que executa o flow.
      Pode ser vazio/None. Ex.: `"./pipelines/datalake/..."`
    memory(Literal["small", "medium", "large"]?):
      Quantidade de memória RAM disponibilizada para a VM
      executando o flow. Atenção: em Google Cloud Run Jobs,
      não existe disco rígido; o filesystem reside na
      própria memória RAM.
      * Para `memory="small"` (valor padrão), são alocados 4 GB
        de RAM, ideal para flows que não fazem escrita de
        muitos dados em "disco".
      * Para `memory="medium"`, são alocados 12 GB de RAM
      * Para `memory="large"`, são alocados 24 GB de RAM
    mount_gcs(bool?):
      Flag indicando se um bucket do GCS deve ser montado
      em `/mnt/gcs` ou não. Como não há disco, se for
      necessário escrever arquivos maiores que a RAM
      disponível, é necessário usar um bucket externo.
      Falso por padrão.
    region(Literal["bra"]?):
      Identificador interno de região onde o flow será executado.
      Se None, será a região padrão (prov. us-central1).
      Tenha em mente que regiões alternativas costumam ser mais
      caras do que a região padrão; só use se absolutamente
      necessário (p.ex. geoblocking de websites).
  """
  if not schedules:
    schedules = []

  memory = str(memory).strip().lower()
  if memory not in ("small", "medium", "large"):
    raise ValueError(f"'{memory}' não é um valor válido para `memory`!")

  if not region:
    region = None

  return {
    "flow": flow,
    "schedules": schedules,
    "dockerfile": dockerfile,
    "memory": memory,
    "gcs": bool(mount_gcs),
    "region": region,
  }


def get_run_parameters() -> dict[str, Any]:
  """
  Retorna um dicionário com os parâmetros passados para essa execução de flow
  """
  ctx = FlowRunContext.get()
  if ctx is None:
    raise RuntimeError("Não foi possível obter referência à Flow Run!")
  return ctx.parameters


def get_flow_name():
  """Retorna o nome da flow, especificado em @flow(name='...')"""
  fr_ctx = FlowRunContext.get()
  if not fr_ctx:
    raise RuntimeError("Não foi possível obter 'FlowRunContext'!")
  return fr_ctx.flow.name


def get_normalized_flow_name():
  """
  Retorna o nome do flow normalizado para uso em pastas etc.
  Ex.: "Report: Previsão do Tempo" → "report_previsao_do_tempo"
  """
  flow_name = get_flow_name().strip()
  # Normaliza o nome para deploy
  normalized_flow_name = re.sub(
    r"_{2,}",
    "_",
    re.sub(
      r"[^a-z_]", "", (unicodedata.normalize("NFD", flow_name).lower().replace(" ", "_"))
    ),
  )
  if len(normalized_flow_name) < 1:
    raise RuntimeError("Após normalização, nome do flow fica vazio!")
  if is_dev_run():
    return f"{normalized_flow_name}_staging"
  return normalized_flow_name


@authenticated_task
def as_task(function, args: list = None, kwargs: dict = None):
  """Executa uma função comum como task"""
  args = [] if not args else args
  kwargs = dict() if not kwargs else kwargs

  return function(*args, **kwargs)


def get_flow_runs_with_state(
  states: List[
    Literal[
      "SCHEDULED",
      "PENDING",
      "RUNNING",
      "COMPLETED",
      "FAILED",
      "CANCELLED",
      "CRASHED",
      "PAUSED",
      "CANCELLING",
    ]
  ],
) -> List[Dict[str, Union[FlowRun, Flow]]]:
  with get_client(sync_client=True) as client:
    flow_runs = client.read_flow_runs(
      flow_run_filter=FlowRunFilter(state={"type": {"any_": states}})
    )
    return [
      {"flow_run": flow_run, "flow": client.read_flow(flow_run.flow_id)}
      for flow_run in flow_runs
    ]


def set_flow_run_state(
  flow_run_id: str, state: StateType, message: Optional[str] = None
) -> dict:
  """
  Requisita a mudança de estado de uma flow run, a partir de seu ID.
  Retorna um dicionário com chaves "status" e "details". Uso:
  ```python
  set_flow_run_state(
    flow_run_id=str(flow_run_uuid),
    state=StateType.CANCELLING,
    message="Flow cancelado programaticamente",
  )
  # Deve retornar algo como:
  #  {'status': 'ACCEPT', 'details': 'accept_details'}
  # O flow run em si terá logs como:
  #  INFO | Running hook 'handle_flow_state_change' in response to entering state 'Cancelling'
  #  INFO | [handle_flow_state_change] '...' (...) -> Cancelling
  ```
  """
  with get_client(sync_client=True) as client:
    result = client.set_flow_run_state(
      flow_run_id=flow_run_id, state=State(type=state, message=message)
    )
    reason = result.details.reason if hasattr(result.details, "reson") else ""
    reason = "" if not reason else f": {reason}"
    return {"status": result.status.value, "details": f"{result.details.type}{reason}"}


def clear_concurrency_limit(limit: str):
  try:
    with get_client(sync_client=True) as client:
      client.update_global_concurrency_limit(
        name=limit, concurrency_limit=GlobalConcurrencyLimitUpdate(active_slots=0)
      )
    # Em estado Crashed, esses logs não aparecem :\
    log(f"[clear_concurrency_limit] Limite '{limit}' resetado")
  except ObjectNotFound:
    log(f"[clear_concurrency_limit] Limite '{limit}' não encontrado!", level="warning")
