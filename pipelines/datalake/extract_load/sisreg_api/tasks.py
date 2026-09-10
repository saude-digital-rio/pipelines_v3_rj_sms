# -*- coding: utf-8 -*-
import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Literal, Optional, Tuple
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
from google.cloud import bigquery, storage
from prefect import task as unauthenticated_task

from pipelines.utils.cleanup import cleanup_columns_for_bigquery, prettify_byte_size
from pipelines.utils.datalake import safe_df_to_parquet
from pipelines.utils.datetime import is_valid_YYYYMMDD, now, now_str, today
from pipelines.utils.env import get_google_project_for_environment
from pipelines.utils.io import create_tmp_data_folder
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task

from .constants import constants as flow_consts
from .utils import build_ES_query, connect_ES, normalize_dates


@unauthenticated_task
def gerar_faixas_de_data(
  data_inicio: Optional[str] = None,
  data_fim: Optional[str] = None,
  mode: Literal["extract", "update"] = "extract",
  dias_por_faixa: int | None = None,
) -> List[Tuple[str, str]]:
  """
  Gera uma lista de tuplas (inicio, fim) dividindo o intervalo
  entre data_inicial e data_final em blocos de tamanho 'dias_por_faixa'.
  """
  # NOTE: Tecnicamente, pra mode="update", aqui não precisaria
  #       arredondar pro mês mais próximo, mas devemos usar sempre
  #       com precisão de mês, então não faz mal
  dt_inicio, dt_fim = normalize_dates(data_inicio, data_fim, mode)

  if not dias_por_faixa:
    dias_por_faixa = flow_consts.DEFAULT_WINDOW_DAYS.value

  log("Gerando faixas de datas para processamento em lotes")
  faixas = []
  while dt_inicio <= dt_fim:
    # Calcula faixa
    dt_chunk_inicio = dt_inicio
    dt_chunk_fim = dt_chunk_inicio + timedelta(days=dias_por_faixa - 1)
    # Se a faixa termina depois do limite, trunca
    if dt_chunk_fim > dt_fim:
      dt_chunk_fim = dt_fim
    # Salva faixa calculada
    faixa_inicio_str = dt_chunk_inicio.isoformat()
    faixa_fim_str = dt_chunk_fim.isoformat()
    faixas.append((faixa_inicio_str, faixa_fim_str))
    # Nova data de início
    dt_inicio = dt_chunk_fim + timedelta(days=1)

  log(f"{len(faixas)} faixas de datas geradas com sucesso:\n{faixas}")
  return faixas


@unauthenticated_task(
  retries=5, retry_delay_seconds=30, tags=[flow_consts.CONCURRENCY_LIMIT_TAG.value]
)
def extract_from_api(
  user: str,
  password: str,
  index_name: str,
  page_size: int,
  data_inicio: str,
  data_fim: str,
  mode: Literal["extract", "update"],
):
  """
  Extrai dados do SISREG via API do ElasticSearch,
  considerando apenas o intervalo [data_inicial, data_final].

  Ao final, escreve em disco em formato Parquet e
  retorna apenas o caminho do arquivo.
  """
  # Valida as datas recebidas
  if not is_valid_YYYYMMDD(data_inicio):
    raise ValueError(f"Data inicial '{data_inicio}' é inválida!")

  if not is_valid_YYYYMMDD(data_fim):
    raise ValueError(f"Data final '{data_fim}' é inválida!")

  if data_inicio > data_fim:
    raise ValueError(
      f"Data inicial '{data_inicio}' não pode ser posterior à data final '{data_fim}'!"
    )

  ###

  log(f"[{data_inicio} : {data_fim}] Conectando ao ElasticSearch...")
  es = connect_ES(flow_consts.API_URL.value, user, password)
  query = build_ES_query(page_size, data_inicio, data_fim, mode)

  # Lista de IDs a serem limpados posteriormente
  latest_scroll_id = None
  scroll_ids = []
  dados: List[dict] = []

  # Loop externo de consumo
  i = -1
  while True:
    i += 1

    # Loop interno de conexão com a API;
    # retenta `max_retries` vezes até desistir
    retries = 0
    max_retries = 5
    while True:
      if i == 0:
        resposta: dict = es.search(
          index=index_name, body=query, scroll=flow_consts.SCROLL_TIMEOUT.value
        )
      else:
        resposta = es.scroll(
          scroll_id=latest_scroll_id, scroll=flow_consts.SCROLL_TIMEOUT.value
        )

      # Se conseguiu obter os dados, sai do loop interno
      if not resposta.get("timed_out", False):
        break
      # Caso contrário, retenta até `max_retries` vezes
      retries += 1
      if retries > max_retries:
        raise RuntimeError(
          f"[{data_inicio} : {data_fim}] Timeout repetido na consulta inicial."
        )

      log(
        f"[{data_inicio} : {data_fim}] Timeout na consulta; retentando em 10s ({retries}/{max_retries})"
      )
      time.sleep(10)

    # Atualiza o scroll_id caso ele seja novo; scroll_id é
    # identificador do estado dos dados quando a requisição foi feita
    # É necessário para que dados de páginas seguintes sejam consistentes
    # e não tenham sofrido alterações entre requisições
    new_scroll_id = resposta.get("_scroll_id")
    if new_scroll_id and latest_scroll_id != new_scroll_id:
      scroll_ids.append(new_scroll_id)
      latest_scroll_id = new_scroll_id

    # Confere metadados
    # '_shards': {'total': x, 'successful': x, 'skipped': x, 'failed': x}
    shards: dict = resposta.get("_shards", {})
    if shards.get("failed", 0) > 0:
      raise RuntimeError(
        f"[{data_inicio} : {data_fim}] Consulta com falhas em shards: {shards}"
      )
    if shards.get("skipped", 0) > 0:
      log(
        f"[{data_inicio} : {data_fim}] Consulta com shards pulados: {shards}",
        level="warning",
      )

    hits: List[dict] = resposta["hits"]["hits"]
    # O total de registros não muda, então só precisamos pegá-lo
    # na resposta da primeira página
    if i == 0:
      # 'hits': {
      #   'total': {'value': x, 'relation': 'eq'},
      #   'max_score': x,
      #   'hits': [ ... ]   # Dados de verdade estão aqui
      # }
      total_obj: dict = resposta["hits"]["total"]
      total_registros = total_obj["value"] if total_obj.get("relation") == "eq" else None

      if total_registros == 0 or not hits:
        log(f"[{data_inicio} : {data_fim}] Nenhum registro no intervalo.")
        return None
      log(
        f"[{data_inicio} : {data_fim}] Total de registros encontrados: {total_registros}"
      )
    # Em páginas seguintes, precisamos conferir se ainda recebemos resultados
    # Caso contrário, a extração terminou
    else:
      if not hits:
        break

    # 'hits': [
    #   {
    #     '_index': 'xxx',  # Nome do endpoint; ex. 'solicitacao-ambulatorial'
    #     '_type': '_doc',
    #     '_id': 'xxx',
    #     '_score': x,
    #     '_source': {
    #       ... # Dados de verdade (agora é sério)
    #     },
    #   },
    #   ...
    # ]
    # Dados de verdade ficam no '_source', é um dicionário enorme
    for registro in hits:
      dado: dict = registro.get("_source", None)
      if not dado:
        continue
      data_solicitacao = (
        dado.get("data_solicitacao") or today().replace(day=1).isoformat()
      )
      dado["data_particao"] = (
        datetime.fromisoformat(data_solicitacao)
        .astimezone(ZoneInfo("America/Sao_Paulo"))
        .date()
        .replace(day=1)
        .isoformat()
      )
      dados.append(dado)
    log(
      f"[{data_inicio} : {data_fim}] Processados {len(dados)}/{total_registros} registros"
    )

  # Scrolls em aberto consomem memória do servidor de API;
  # limpa os scrolls porque somos educados
  es.options(ignore_status=(404,)).clear_scroll(scroll_id=scroll_ids)

  # Valida dados recebidos vs. reportados
  total_obtido = len(dados)
  diff = abs(total_obtido - total_registros)
  # Permite até 5% de diferença
  if total_registros > 0 and abs(diff / total_registros) > 0.05:
    raise ValueError(
      f"[{data_inicio} : {data_fim}] "
      f"Divergência na contagem de registros processados. "
      f"Esperado: {total_registros}, Obtido: {total_obtido}"
    )

  df = pd.DataFrame(dados, dtype=str)
  df = cleanup_columns_for_bigquery(df, lowercase=True)
  df["_run_id"] = str(uuid4())
  df["_extracted_at"] = now_str()
  return df


@unauthenticated_task()
def write_partitions_to_disk(df: pd.DataFrame) -> list[tuple[str, str]]:
  """
  Agrupa dados de um DataFrame por data_particao, salva cada pedaço
  como um Parquet, e retorna uma lista de tuplas (data_particao, caminho do Parquet).
  """
  root_path = create_tmp_data_folder()
  all_paths = []
  for data_particao, partition_df in df.groupby("data_particao"):
    partition_path = os.path.join(root_path, f"{data_particao}.parquet")
    all_paths.append(
      (data_particao, safe_df_to_parquet(partition_df, output_path=partition_path))
    )

  output = []
  for path in Path(root_path).iterdir():
    if path.is_file():
      output.append(f"{path.name}: {prettify_byte_size(path.stat().st_size)}")
  log(
    f"Foram encontradas {len(all_paths)} partições nos dados:\n"
    + "\n".join(sorted(output))
  )
  return all_paths


@task()
def read_partition_from_bigquery(
  dataset_id: str,
  table_id: str,
  data_particao: str,
  environment: Literal["dev", "prod"] = "dev",
) -> str:
  """
  Lê todos os registros de uma partição específica da tabela BigLake (staging)
  e retorna como caminho para um Parquet com os dados. Se a partição não existir
  ou estiver vazia, retorna uma string vazia.

  Args:
    dataset_id(str): Nome do dataset no BigQuery
    table_id(str): Nome da tabela
    data_particao(str): Data da partição no formato "YYYY-MM-DD".
    environment(str): "dev" ou "prod".

  Returns:
    out(str): Caminho do Parquet com os dados
  """
  project = get_google_project_for_environment(environment)
  full_table = f"`{project}.{dataset_id}_staging.{table_id}`"
  sql = f"SELECT * FROM {full_table} WHERE data_particao = '{data_particao}'"

  log(f"[{data_particao}] Lendo partição existente: {full_table}")
  client = bigquery.Client()
  try:
    df = client.query(sql).to_dataframe()
    log(f"[{data_particao}] {len(df)} registros lidos da partição existente.")
    out_path = safe_df_to_parquet(df)
    del df
    return out_path
  except Exception as e:
    # A tabela pode ainda não existir na primeira execução
    log(f"[{data_particao}] Não foi possível ler a partição existente: {repr(e)}")
    return ""


@task()
def delete_partition_files(
  dataset_id: str,
  table_id: str,
  data_particao: str,
  environment: Literal["dev", "prod"] = "dev",
  sanity_check: Optional[str] = None,
):
  """
  Apaga todos os arquivos Parquet de uma partição específica no GCS (staging).

  Args:
    dataset_id(str): Nome do dataset
    table_id(str): Nome da tabela
    data_particao(str): Data da partição no formato "YYYY-MM-DD".
    environment(str): "dev" ou "prod".
    sanity_check(pd.DataFrame):
      DataFrame contendo os dados novos; caso esteja vazio, a partição não é deletada.
  """
  if (
    not sanity_check
    or not os.path.isfile(sanity_check)
    or os.stat(sanity_check).st_size <= 0
  ):
    raise RuntimeError(
      f"DataFrame `sanity_check` veio vazio! Partição {data_particao} não será apagada"
    )

  dt = datetime.fromisoformat(data_particao).date()

  prefix = (
    f"staging/{dataset_id}/{table_id}/"
    f"ano_particao={dt.year}/"
    f"mes_particao={dt.month:02}/"
    f"data_particao={data_particao}/"
  )

  project = get_google_project_for_environment(environment)
  client = storage.Client()
  bucket = client.bucket(project)

  blobs = list(bucket.list_blobs(prefix=prefix, match_glob="**.parquet"))
  if len(blobs) <= 0:
    log(f"[{data_particao}] Não há arquivos em 'gs://{project}/{prefix}'")
    return
  log(
    f"[{data_particao}] Apagando {len(blobs)} arquivo(s) da partição (gs://{project}/{prefix})"
  )
  bucket.delete_blobs(blobs)


@unauthenticated_task()
def merge_partition(old_df_path: str, new_df_path: str, data_particao: str) -> str:
  """
  Junta dois DataFrames, deduplicando por `codigo_solicitacao`.

  Args:
    old_df(DataFrame): DataFrame com dados já presentes no datalake.
    new_df(DataFrame): DataFrame com os registros recém-extraídos da API para esta partição.
    data_particao(str): Data da partição no formato "YYYY-MM-DD".

  Returns
    out(str): Caminho do Parquet combinado
  """

  if not old_df_path:
    log(f"[{data_particao}] Nenhum dado já no datalake; fazendo upload de tudo")
    return new_df_path

  old_df = pd.read_parquet(old_df_path)
  os.remove(old_df_path)
  new_df = pd.read_parquet(new_df_path)
  os.remove(new_df_path)

  log(
    f"[{data_particao}] {len(old_df)} registros no datalake; {len(new_df)} atualizados."
  )
  merged_df = pd.concat([old_df, new_df], ignore_index=True)
  del old_df
  del new_df
  merged_df = merged_df.drop_duplicates(
    subset=["codigo_solicitacao"], keep="last"
  ).reset_index(drop=True)

  log(f"[{data_particao}] Merge concluído; {len(merged_df)} registros no final")

  out_path = safe_df_to_parquet(merged_df)
  del merged_df
  return out_path


@task()
def delete_old_files(
  data_inicio: Optional[str], data_fim: Optional[str], dataset_id: str, table_id: str
):
  # Esse flow é executado com frequência, e dados antigos acabam acumulando
  # Por isso, forçamos a extração de meses inteiros (dia 1 a último dia)
  # e aqui apagamos arquivos antigos de um mesmo mês
  dt_inicio, dt_fim = normalize_dates(data_inicio, data_fim, "extract")
  dt_fim = dt_fim.replace(day=1)

  client = storage.Client()
  project_name = get_google_project_for_environment()
  bucket = client.bucket(project_name)

  current_year = dt_inicio.year
  current_month = dt_inicio.month

  # Itera por todos os meses entre data início e fim
  while (current_year, current_month) <= (dt_fim.year, dt_fim.month):
    # Calculamos o caminho da pasta de partição referente
    PATH = (
      f"staging/{dataset_id}/"
      f"{table_id}/"
      f"ano_particao={current_year}/"
      f"mes_particao={current_month:02}/"
      f"data_particao={current_year}-{current_month:02}-01/"
    )
    # Lista todos os arquivos na pasta, calcula data/hora limite de criação
    blobs = bucket.list_blobs(prefix=PATH, match_glob="**.parquet")
    threshold_date = now() - timedelta(hours=12)
    log(f"Conferindo arquivos antigos (>12h) no mês {current_year}-{current_month:02}...")
    # Lista de arquivos a serem apagados
    to_delete = []
    for blob in blobs:
      if blob.time_created < threshold_date:
        log(
          f"Encontrado {blob.name} "
          f"(criado em {blob.time_created.strftime('%Y-%m-%d %H:%M:%S')})"
        )
        to_delete.append(blob)
    log(f"Deletando {len(to_delete)} arquivo(s)")
    bucket.delete_blobs(to_delete)

    # Calcula mês seguinte a ser conferido
    if current_month == 12:
      current_month = 1
      current_year += 1
    else:
      current_month += 1

  return
