# -*- coding: utf-8 -*-
import os
import tracemalloc
from typing import Literal, Optional

import pandas as pd

from pipelines.constants import CIT
from pipelines.utils._trace import display_top
from pipelines.utils.datalake import upload_df_to_datalake_task
from pipelines.utils.infisical import get_secret_task
from pipelines.utils.prefect import clear_concurrency_limit, flow, flow_config

from .constants import constants as flow_consts
from .schedules import schedules
from .tasks import (
  delete_old_files,
  delete_partition_files,
  extract_from_api,
  gerar_faixas_de_data,
  merge_partition,
  read_partition_from_bigquery,
)
from .utils import table_name_from_resource


# A tag de limite de concorrência pra task de extração do Sisreg
# não tem "slot decay" configurado; em caso de crash/cancelamento de flow,
# os slots não são desocupados. O Prefect tem, em teoria, algum sistema
# de GC que deveria reabrir os slots, mas eu nunca vi acontecendo.
# Então colocamos hooks pra 'manualmente' reabrir os slots caso
# o flow seja interrompido
def clear_sisreg_limit(*args, **kwargs):
  limit = f"tag:{flow_consts.CONCURRENCY_LIMIT_TAG.value}"
  clear_concurrency_limit(limit)


@flow(
  name="Extração: Sisreg API",
  owners=[CIT.AVELLAR_ID.value],
  tags=["CIT", "SUBGERAL"],
  on_crashed=[clear_sisreg_limit],
  on_cancellation=[clear_sisreg_limit],
)
def extract_sisreg_api(
  es_index: Literal[
    "solicitacao-ambulatorial-rj", "marcacao-ambulatorial-rj", "solicitacao-hospitalar-rj"
  ],
  data_inicio: Optional[str] = None,
  data_fim: Optional[str] = None,
  page_size: int = 10_000,
  dias_por_faixa: int | None = flow_consts.DEFAULT_WINDOW_DAYS.value,
  dataset_id: str = "brutos_sisreg_api_v2",
  mode: Literal["extract", "update"] = "extract",
  table_id: Optional[str] = None,
  environment: Literal["dev", "prod"] = "dev",
):
  """
  Args:
    es_index(["solicitacao-ambulatorial-rj", "marcacao-ambulatorial-rj", "solicitacao-hospitalar-rj"]):
      Endpoint do ElasticSearch a ser contactado. No momento,
      a API só aceita 3 valores possíveis.
    data_inicio(str?):
      Data, no formato ISO (YYYY-MM-DD), a partir da qual
      registros são obtidos. Quando None, é `data_fim` - 6 meses.
    data_fim(str?):
      Data, no formato ISO (YYYY-MM-DD), até a qual
      registros são obtidos. Quando None, é o dia de hoje.
    page_size(int?):
      As respostas da API são paginadas; esse é o limite de
      registros por página. Por padrão, 10,000, o máximo que
      a API permite.
    dias_por_faixa(int?):
      Quantos dias cada task deve processar
    dataset_id(str?):
      Nome do dataset onde os dados devem ser inseridos.
      Por padrão, 'brutos_sisreg_api_v2'.
    table_id(str?):
      Nome da tabela onde os dados devem ser inseridos.
      Se None (padrão), é inferido de 'es_index': por exemplo,
      o endpoint "marcacao-ambulatorial-rj" vai para a tabela
      "marcacao_ambulatorial_rj".
    environment(str?):
      Ambiente de execução, "dev" (padrão) ou "prod".
  """
  # Guia:
  # https://servicos-datasus.saude.gov.br/detalhe/jDCFmnHyYQ
  # Manual:
  # https://mobileapps.saude.gov.br/portal-servicos/files/f3bd659c8c8ae3ee966e575fde27eb58/dfabfeedef07f675a142b63fa2553c6f_msv3sgc2e.pdf

  username = get_secret_task(
    secret_name="ES_USERNAME", environment=environment, path="/sisreg_api"
  )
  password = get_secret_task(
    secret_name="ES_PASSWORD", environment=environment, path="/sisreg_api"
  )

  faixas = gerar_faixas_de_data(
    data_inicio=data_inicio, data_fim=data_fim, mode=mode, dias_por_faixa=dias_por_faixa
  )

  dataset_id = dataset_id if dataset_id else "brutos_sisreg_api_v2"
  table_id = table_id if table_id else table_name_from_resource(es_index)

  tracemalloc.start()

  for inicio, fim in faixas:
    # 1) Extrai lote a lote, retorna dados em dataframe
    df: pd.DataFrame = extract_from_api(
      user=username,
      password=password,
      index_name=es_index,
      page_size=page_size,
      data_inicio=inicio,
      data_fim=fim,
      mode=mode,
    )
    if df is None or df.empty:
      continue

    if mode == "extract":
      # 2a) Se estamos só extraindo, faz upload direto do dataframe com 'append'
      # Como extraímos o mês inteiro, vamos ter um substituto completo dos
      # dados já presentes, então dados antigos são excluídos no fim do flow
      upload_df_to_datalake_task(
        df=df,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        source_format="parquet",
        date_partition_column="data_particao",
      )

    elif mode == "update":
      # Ao que parece, resultados de tasks são guardados de alguma forma em memória
      # para, caso uma task falhe, ela possa ser retentada com os mesmos valores
      # Uma forma de diminuir o impacto disso é, ao invés de retornar DataFrames,
      # salvá-los a Parquets e retornar o caminho deles; é o que fazemos abaixo

      # 2b) Para cada partição presente nos dados novos:
      for data_particao, partition_df in df.groupby("data_particao"):
        # 2b.1) Lê os dados dessa partição já no BigQuery
        existing_df_path = read_partition_from_bigquery(
          dataset_id=dataset_id,
          table_id=table_id,
          data_particao=data_particao,
          environment=environment,
        )
        # 2b.2) Junta os dados antigos com os dados novos
        merged_df_path = merge_partition(
          old_df_path=existing_df_path, new_df=partition_df, data_particao=data_particao
        )
        # 2b.3) Apaga os arquivos antigos da partição antes de reenviar
        # TODO: melhorar, porque se upload_... falha, ficamos sem dados
        delete_partition_files(
          dataset_id=dataset_id,
          table_id=table_id,
          data_particao=data_particao,
          environment=environment,
          sanity_check=merged_df_path,
        )
        # 2b.4) Reupload dos dados agora atualizados
        upload_df_to_datalake_task(
          df=pd.read_parquet(merged_df_path).astype(str),
          dataset_id=dataset_id,
          table_id=table_id,
          dump_mode="append",
          source_format="parquet",
          date_partition_column="data_particao",
        )

        os.remove(merged_df_path)
        snapshot = tracemalloc.take_snapshot()
        display_top(snapshot)

  if mode == "extract":
    # 3) Por fim, apaga arquivos antigos
    delete_old_files(
      data_inicio=data_inicio, data_fim=data_fim, dataset_id=dataset_id, table_id=table_id
    )


# TODO: é possível fazer esse flow funcionar com memory=medium (sem levar 5h)?
_flows = [flow_config(flow=extract_sisreg_api, schedules=schedules, memory="large")]
