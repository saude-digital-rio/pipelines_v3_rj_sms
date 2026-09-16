# -*- coding: utf-8 -*-
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Literal, Optional, Tuple

from elasticsearch import Elasticsearch, exceptions
from prefect import State, Task

from pipelines.utils.cleanup import cleanup_bigquery_name, prettify_byte_size
from pipelines.utils.datetime import parse_date_or_today
from pipelines.utils.logger import log
from pipelines.utils.monitor import get_ram_snapshot

from .constants import constants as flow_consts


def normalize_dates(
  data_inicio: Optional[str], data_fim: Optional[str], mode: Literal["extract", "update"]
) -> Tuple[date, date]:
  """
  Recebe dois objetos, ou strings de data ou None, e retorna `date()`s equivalentes.
  * Caso `data_inicio` seja None, será `data_fim` subtraída de N dias.
  * Caso `data_fim` seja None, será o dia de hoje.
  Importante: em mode='extract', a data início será SEMPRE o dia 1º do mês, e a data fim
  será sempre o último dia do mês. Essa decisão tem como objetivo facilitar
  a limpeza de partições repetidas posteriormente. Ex.:
  >>> normalize_dates(data_inicio='2025-06-20', data_fim='2026-01-04', mode="extract")
  ( date(2025, 6, 1), date(2026, 1, 31) )
  """
  dt_fim = parse_date_or_today(data_fim).date()
  if mode == "extract":
    # Calcula último dia do mês
    dt_fim = (
      date(dt_fim.year, 12, 31)
      if dt_fim.month == 12
      else (date(dt_fim.year, dt_fim.month + 1, 1) - timedelta(days=1))
    )

  if not data_inicio:
    dt_inicio = (
      # Quando consultando por data de criação, queremos a(s) partição(ões)
      # que inclui(em) os dias desejados
      (dt_fim.replace(year=dt_fim.year - 1, day=1))
      if mode == "extract"
      # Caso contrário, consultando por data de atualização, queremos
      # somente os N dias mesmo
      else (dt_fim - timedelta(days=flow_consts.DEFAULT_WINDOW_DAYS.value - 1))
    )
  else:
    dt_inicio = datetime.fromisoformat(data_inicio).date()
    if mode == "extract":
      dt_inicio = dt_inicio.replace(day=1)

  if dt_inicio > dt_fim:
    raise ValueError(
      f"Data inicial '{dt_inicio}' não pode ser posterior à data final '{dt_fim}'!"
    )

  return (dt_inicio, dt_fim)


def connect_ES(url: str, user: str, password: str) -> Elasticsearch:
  es = Elasticsearch(
    url,
    basic_auth=(user, password),
    request_timeout=600,
    max_retries=5,
    retry_on_timeout=True,
    http_compress=True,
  )
  try:
    es.info()
    log("Conexão com o Elasticsearch estabelecida.")
  except exceptions.ConnectionError as e:
    log(f"Falha na conexão com o Elasticsearch: {repr(e)}")
    raise e
  return es


def build_ES_query(
  page_size: int, data_inicial: str, data_final: str, mode: Literal["extract", "update"]
):
  rj_ibge = "330455"
  # ElasticSearch tem limite de 10k resultados por requisição
  # > "By default, you cannot use from and size to page through
  #    more than 10,000 hits"
  # [Ref] https://www.elastic.co/docs/reference/elasticsearch/rest-apis/paginate-search-results
  page_size = min(max(1, page_size), 10_000)

  filter_field = "data_solicitacao" if mode == "extract" else "data_atualizacao"

  return {
    "size": page_size,
    "sort": [{"data_solicitacao": {"order": "desc"}}],
    "query": {
      "bool": {
        "must": [
          {"match": {"codigo_central_reguladora": rj_ibge}},
          {
            "range": {
              filter_field: {
                "gte": data_inicial,
                "lte": data_final,
                "time_zone": "-03:00",
              }
            }
          },
        ]
      }
    },
  }


def table_name_from_resource(resource: str) -> str:
  """
  Retorna o nome de tabela designada para recurso ("index")
  sendo requisitado no ElasticSearch
  """
  if resource.startswith("solicitacao") or resource.startswith("marcacao"):
    return cleanup_bigquery_name(resource, lowercase=True)
  raise NotImplementedError(f"Não há tabela definida por padrão para '{resource}'")


def handle_task_state_change(task: Task, task_run, state: State):
  snapshot = get_ram_snapshot()
  log(
    "\n======== RAM ========\n"
    f"{snapshot['used_pretty']} / "
    f"{snapshot['total_pretty']} "
    f"({snapshot['used_pct']:.2f}%)"
    "\n====================="
  )

  files = []
  for path in Path("/tmp/pipelines").rglob("*"):
    if path.is_file():
      files.append(f"{path}: {prettify_byte_size(path.stat().st_size)}")

  log("Arquivos em /tmp:\n" + "\n".join(sorted(files)))
