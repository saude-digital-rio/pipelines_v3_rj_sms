# -*- coding: utf-8 -*-

import glob
import os
import shutil
import uuid
from typing import Dict, List, Literal, Optional

import basedosdados as bd
import pandas as pd
from google.cloud import bigquery

from pipelines.utils.env import get_google_project_for_environment
from pipelines.utils.io import create_tmp_data_folder
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


def safe_df_to_parquet(df: pd.DataFrame, output_path: Optional[str] = None) -> str:
  """
  Cria um arquivo Parquet que é preenchido com os dados de um DataFrame,
  convertidos para string.

  Args:
    df(pd.DataFrame): O DataFrame a ser exportado.
    output_path(str?):
      O caminho do arquivo a ser criado. Algo como `/tmp/file.parquet`.
      Se não for passado, uma pasta temporária local será criada, e o
      nome do arquivo será um UUID v4.
  Returns:
    O caminho do arquivo criado.
  """
  if not output_path:
    root_folder = create_tmp_data_folder()
    output_path = os.path.join(root_folder, f"{uuid.uuid4()}.parquet")

  df = df.fillna("").astype(str)
  df.to_parquet(output_path, index=False, compression="zstd")
  return output_path


def upload_to_datalake(
  input_path: str,
  dataset_id: str,
  table_id: str,
  dump_mode: Literal["append", "replace"] = "append",
  delete_mode: Literal["staging", "all"] = "staging",
  source_format: Literal["csv", "parquet"] = "csv",
  csv_delimiter: str = ";",
  if_storage_data_exists: Literal["replace", "raise", "pass"] = "replace",
  biglake_table: bool = True,
  dataset_is_public: bool = False,
  exception_on_missing_input_file: bool = False,
):
  """
  Faz upload de arquivo para um bucket do Google Cloud Storage;
  cria ou adiciona dados a uma tabela do BigQuery.

  Args:
    input_path (str):
      Caminho para o arquivo com dados; pode ser pasta ou arquivo
    dataset_id (str):
      Nome do dataset no BigQuery
    table_id (str):
      Nome da tabela no BigQuery
    dump_mode (str?):
      Como criar os dados: `"append"` não apaga dados existentes, `"replace"` substitui
      tudo que existia. Por padrão, é `"append"`.
    delete_mode (str?):
      `"all"` apaga tanto a tabela externa (`_staging`) quanto a materializada;
      `"staging"` apaga somente a tabela externa. Por padrão, é `"staging"`.
    source_format (str?):
      Formato dos dados (`"csv"` ou `"parquet"`). Por padrão, é `"csv"`.
    csv_delimiter (str?):
      Separador usado no arquivo CSV. Por padrão, é `";"`.
    if_storage_data_exists (str?):
      O que fazer se o dado já existir no GCS: `"raise"` dispara erro de conflito;
      `"replace"` substitui o dado; `"pass"` não faz nada. Por padrão, é `"replace"`.
    biglake_table (bool?):
      Se a tabela é BigLake – i.e. permite consultas mesmo sem ser materializada.
      Por padrão, é `True`.
    dataset_is_public (bool?):
      Se o dataset é público. Por padrão, é `False`.
    exception_on_missing_input_file (bool?):
      Se deve disparar um `FileNotFoundError` caso `input_path` seja string vazia ou
      uma pasta sem arquivos. Por padrão, é `False`.
  """
  if input_path == "":
    log("`input_path` vazio; nada para fazer upload", level="warning")
    if exception_on_missing_input_file:
      raise FileNotFoundError(f"Nenhum arquivo em '{input_path}'")
    return

  if os.path.isdir(input_path):
    log(f"`input_path` é diretório: '{input_path}'")

    reference_path = os.path.join(input_path, f"**/*.{source_format}")
    log(f"Procurando por arquivos em '{reference_path}'...")

    if len(glob.glob(reference_path, recursive=True)) == 0:
      log(
        f"Nenhum arquivo '{source_format}' encontrado em '{input_path}'", level="warning"
      )
      if exception_on_missing_input_file:
        raise FileNotFoundError(f"Nenhum arquivo em '{input_path}'")
      return

  log("Montando referências à tabela e ao GCS...")
  tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
  table_staging = f"{tb.table_full_name['staging']}"

  st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
  storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
  storage_path_link = (
    f"https://console.cloud.google.com/storage/browser/{st.bucket_name}"
    f"/staging/{dataset_id}/{table_id}"
  )
  log(
    f"Fazendo upload de arquivo '{input_path}' para '{storage_path}' "
    f"com formato '{source_format}'"
  )

  try:
    # Se tabela não existe, cria
    table_exists = tb.table_exists(mode="staging")
    if not table_exists:
      log(f"CRIANDO TABELA: '{dataset_id}.{table_id}'")
      tb.create(
        path=input_path,
        source_format=source_format,
        csv_delimiter=csv_delimiter,
        if_storage_data_exists=if_storage_data_exists,
        biglake_table=biglake_table,
        dataset_is_public=dataset_is_public,
      )
      log("Upload para o BigQuery bem sucedido")
      return

    # Se a tabela já existe, e queremos dar `append`
    if dump_mode == "append":
      log(f"TABELA EXISTE; FAZENDO APPEND: '{dataset_id}.{table_id}'")
      tb.append(filepath=input_path, if_exists="raise")
      log("Upload para o BigQuery bem sucedido")
      return

    # Se a tabela já existe, e queremos substituí-la
    if dump_mode == "replace":
      # Apaga GCS
      log(
        "OVERWRITE: TABELA EXISTE; APAGANDO DADOS ANTIGOS DO GCS\n"
        f"'{storage_path}'\n"
        f"'{storage_path_link}'"
      )
      st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
      log(
        f"OVERWRITE: DADOS ANTIGOS APAGADOS DO GCS\n'{storage_path}'\n'{storage_path_link}'"
      )

      # Apaga tabela do BigQuery
      tb.delete(mode=("staging" if delete_mode == "staging" else "all"))
      if delete_mode == "staging":
        log(f"OVERWRITE: TABELA APAGADA DO BIGQUERY:\n{table_staging}\n")
      else:
        deleted_tables = [
          tb.table_full_name[key] for key in tb.table_full_name.keys() if key != "all"
        ]
        log(f"OVERWRITE: TABELAS APAGADAS DO BIGQUERY:\n{deleted_tables}\n")

      # Cria tabela agora que a anterior foi apagada
      tb.create(
        path=input_path,
        source_format=source_format,
        csv_delimiter=csv_delimiter,
        if_storage_data_exists=if_storage_data_exists,
        biglake_table=biglake_table,
        dataset_is_public=dataset_is_public,
      )
      log("Upload para o BigQuery bem sucedido")
      return

    raise ValueError(f"`dump_mode` '{dump_mode}' desconhecido!")

  except Exception as e:
    log(f"An error occurred: {e}", level="error")
    raise e


@task(retries=3, retry_delay_seconds=60)
def upload_to_datalake_task(
  input_path: str,
  dataset_id: str,
  table_id: str,
  dump_mode: Literal["append", "replace"] = "append",
  delete_mode: Literal["staging", "all"] = "staging",
  source_format: Literal["csv", "parquet"] = "csv",
  csv_delimiter: str = ";",
  if_storage_data_exists: Literal["replace", "raise", "pass"] = "replace",
  biglake_table: bool = True,
  dataset_is_public: bool = False,
  exception_on_missing_input_file: bool = False,
):
  """
  Faz upload de arquivo para um bucket do Google Cloud Storage;
  cria ou adiciona dados a uma tabela do BigQuery.

  Args:
    input_path (str):
      Caminho para o arquivo com dados; pode ser pasta ou arquivo
    dataset_id (str):
      Nome do dataset no BigQuery
    table_id (str):
      Nome da tabela no BigQuery
    dump_mode (str?):
      Como criar os dados: `"append"` não apaga dados existentes, `"replace"` substitui
      tudo que existia. Por padrão, é `"append"`.
    delete_mode (str?):
      `"all"` apaga tanto a tabela externa (`_staging`) quanto a materializada;
      `"staging"` apaga somente a tabela externa. Por padrão, é `"staging"`.
    source_format (str?):
      Formato dos dados (`"csv"` ou `"parquet"`). Por padrão, é `"csv"`.
    csv_delimiter (str?):
      Separador usado no arquivo CSV. Por padrão, é `";"`.
    if_storage_data_exists (str?):
      O que fazer se o dado já existir no GCS: `"raise"` dispara erro de conflito;
      `"replace"` substitui o dado; `"pass"` não faz nada. Por padrão, é `"replace"`.
    biglake_table (bool?):
      Se a tabela é BigLake – i.e. permite consultas mesmo sem ser materializada.
      Por padrão, é `True`.
    dataset_is_public (bool?):
      Se o dataset é público. Por padrão, é `False`.
    exception_on_missing_input_file (bool?):
      Se deve disparar um `FileNotFoundError` caso `input_path` seja string vazia ou
      uma pasta sem arquivos. Por padrão, é `False`.
  """
  return upload_to_datalake(
    input_path=input_path,
    dataset_id=dataset_id,
    table_id=table_id,
    dump_mode=dump_mode,
    delete_mode=delete_mode,
    source_format=source_format,
    csv_delimiter=csv_delimiter,
    if_storage_data_exists=if_storage_data_exists,
    biglake_table=biglake_table,
    dataset_is_public=dataset_is_public,
    exception_on_missing_input_file=exception_on_missing_input_file,
  )


def create_date_partitions(
  dataframe: pd.DataFrame,
  partition_column: str,
  file_format: Literal["csv", "parquet"] = "csv",
  root_folder: str = "/tmp/data/",
  csv_delimiter: str = ";",
):
  """"""
  dataframe[partition_column] = pd.to_datetime(dataframe[partition_column])
  dataframe["data_particao"] = dataframe[partition_column].dt.strftime("%Y-%m-%d")
  dates = dataframe["data_particao"].unique()
  dataframes = [
    (date, dataframe[dataframe["data_particao"] == date].drop(columns=["data_particao"]))
    for date in dates
  ]

  for _date, dataframe in dataframes:
    partition_folder = os.path.join(
      root_folder,
      f"ano_particao={_date[:4]}/mes_particao={_date[5:7]}/data_particao={_date}",
    )
    os.makedirs(partition_folder, exist_ok=True)
    file_folder = os.path.join(partition_folder, f"{uuid.uuid4()}.{file_format}")

    if file_format == "csv":
      dataframe.to_csv(file_folder, index=False, sep=csv_delimiter)
    elif file_format == "parquet":
      safe_df_to_parquet(df=dataframe, output_path=file_folder)

  return root_folder


def upload_df_to_datalake(
  df: pd.DataFrame | None,
  dataset_id: str,
  table_id: str,
  dump_mode: Literal["replace", "append"] = "replace",
  source_format: Literal["csv", "parquet"] = "csv",
  biglake_table: bool = True,
  csv_delimiter: str = ";",
  date_partition_column: Optional[str] = None,
  dataset_is_public: bool = False,
):
  """
  Faz upload de um DataFrame como tabela no BigQuery, com opção de
  coluna de partição por data.
  Args:
    df(pd.DataFrame):
      O DataFrame com dados que virará uma tabela
    dataset_id(str):
      Nome do dataset que conterá a tabela ("project.{dataset}.table")
    table_id(str):
      Nome da tabela a ser criada ("project.dataset.{table}")
    dump_mode(str?):
      Modo de criação da tabela em caso de existência prévia: `"replace"`
      (padrão) substitui os dados anteriores; `"append"` adiciona
      os dados ao final dos já existentes
    source_format(str?):
      Formato dos dados que serão salvos no GCS, na camada _staging;
      por padrão, é `"csv"`, mas também pode ser `"parquet"`. CSVs são
      melhores caso o arquivo original queira ser baixado e compartilhado;
      Parquets são melhores caso a intenção seja fazer consultas na tabela
      externa, 'BigLake'
    biglake_table(bool?):
      Se deve ou não criar uma tabela externa no BigQuery; por padrão,
      é `True`
    csv_delimiter(str?):
      Delimitador de campos no caso de `source_format="csv"`; por padrão,
      é `;`. Só importa se você pretende compartilhar os CSVs originais.
      Importante não misturar delimitadores! Se uma tabela já existe separada
      por `,`, fazer upload de mais dados para ela com `;` vai quebrar consultas.
    date_partition_column(str?):
      Nome da coluna de particionamento por data; normalmente algo
      como `"data_particao"`. Por padrão, nenhuma (`None`)
    dataset_is_public(bool?):
      Flag de dataset público; por padrão, `False`
  """
  if df is None or df.empty:
    log(
      f"Dataframe vazio para '{dataset_id}.{table_id}'; upload ignorado", level="warning"
    )
    return None

  root_folder = create_tmp_data_folder()
  log(f"Usando diretório '{root_folder}'")

  # Converte todas as colunas para string
  df = df.astype(str)

  if date_partition_column:
    log(f"Criando particionamento por data para dataframe com {df.shape[0]} linhas")
    create_date_partitions(
      dataframe=df,
      partition_column=date_partition_column,
      file_format=source_format,
      root_folder=root_folder,
      csv_delimiter=csv_delimiter,
    )
  else:
    log(f"Criando partição única para dataframe com {df.shape[0]} linhas")
    file_path = os.path.join(root_folder, f"{uuid.uuid4()}.{source_format}")
    if source_format == "csv":
      df.to_csv(file_path, index=False, sep=csv_delimiter)
    elif source_format == "parquet":
      safe_df_to_parquet(df=df, output_path=file_path)

  log(f"Fazendo upload de dados em '{root_folder}'")
  upload_to_datalake(
    input_path=root_folder,
    dataset_id=dataset_id,
    table_id=table_id,
    dump_mode=dump_mode,
    source_format=source_format,
    csv_delimiter=csv_delimiter,
    # `if_storage_data_exists` aqui só dispara se o UUID4 escolhido para
    # um arquivo JÁ EXISTIA no GCS; efetivamente impossível, então erro
    # pra que a task seja retentada
    if_storage_data_exists="raise",
    biglake_table=biglake_table,
    dataset_is_public=dataset_is_public,
    exception_on_missing_input_file=True,
  )

  log(f"Apagando dados locais: '{root_folder}'")
  shutil.rmtree(root_folder)
  return


def update_logs_to_datalake(
  logs: List[dict],
  dataset_id: str = None,
  table_id: str = None,
  environment: str = None,
  table_full_id: str = None,
) -> dict:
  if not logs:
    return {"inserted_rows": 0}

  if table_full_id:
    full_table_name = table_full_id.split(".")
    if len(full_table_name) != 3:
      raise ValueError(
        f"`table_full_id` deve ter formato project.dataset.table: {table_full_id}"
      )
    project_id, dataset_id, table_id = full_table_name
  elif dataset_id and table_id:
    project_id = get_google_project_for_environment(environment=environment)
  else:
    raise ValueError(
      "Informe `table_full_id` ou os parâmetros `dataset_id` e `table_id`."
    )

  log(
    f"(update_logs_to_datalake) enviando {len(logs)} log(s) para "
    f"{project_id}.{dataset_id}.{table_id}"
  )
  upload_df_to_datalake(
    df=pd.DataFrame(logs),
    dataset_id=dataset_id,
    table_id=table_id,
    dump_mode="append",
    source_format="parquet",
    date_partition_column="data_particao",
  )
  return {"inserted_rows": len(logs)}


@task(retries=3, retry_delay_seconds=60)
def upload_df_to_datalake_task(
  df: pd.DataFrame,
  dataset_id: str,
  table_id: str,
  dump_mode: Literal["replace", "append"] = "replace",
  source_format: Literal["csv", "parquet"] = "csv",
  biglake_table: bool = True,
  csv_delimiter: str = ";",
  date_partition_column: Optional[str] = None,
  dataset_is_public: bool = False,
):
  """
  Faz upload de um DataFrame como tabela no BigQuery, com opção de coluna de partição
  por data
  Args:
    df(pd.DataFrame):
      O DataFrame com dados que virará uma tabela
    dataset_id(str):
      Nome do dataset que conterá a tabela ("project.{dataset}.table")
    table_id(str):
      Nome da tabela a ser criada ("project.dataset.{table}")
    dump_mode(str?):
      Modo de criação da tabela em caso de existência prévia: `"replace"`
      (padrão) substitui os dados anteriores; `"append"` adiciona
      os dados ao final dos já existentes
    source_format(str?):
      Formato dos dados que serão salvos no GCS, na camada _staging;
      por padrão, é `"csv"`, mas também pode ser `"parquet"`. CSVs são
      melhores caso o arquivo original queira ser baixado e compartilhado;
      Parquets são melhores caso a intenção seja fazer consultas na tabela
      externa, 'BigLake'
    biglake_table(bool?):
      Se deve ou não criar uma tabela externa no BigQuery; por padrão,
      é `True`
    csv_delimiter(str?):
      Delimitador de campos no caso de `source_format="csv"`; por padrão,
      é `;`. Só importa se você pretende compartilhar os CSVs originais
    date_partition_column(str?):
      Nome da coluna de particionamento por data; normalmente algo
      como `"data_particao"`. Por padrão, nenhuma (`None`)
    dataset_is_public(bool?):
      Flag de dataset público; por padrão, `False`
  """
  return upload_df_to_datalake(
    df=df,
    dataset_id=dataset_id,
    table_id=table_id,
    dump_mode=dump_mode,
    source_format=source_format,
    biglake_table=biglake_table,
    csv_delimiter=csv_delimiter,
    date_partition_column=date_partition_column,
    dataset_is_public=dataset_is_public,
  )


@task(retries=1, retry_delay_seconds=3 * 60)
def get_email_recipients_task(
  environment: str | None = None,
  dataset: str | None = None,
  table: str | None = None,
  recipients: List[str] | str | None = None,
  error: bool | None = False,
) -> Dict[str, List[str]]:
  """
  Retorna lista de recipientes de email no formato esperado pela API da Iplan,
  a partir de tabela com formato esperado de colunas `email` (`str`),
  `tipo` (`Literal['TO', 'CC', 'BCC', 'SKIP']`) e `recebe_erro` (`bool`).
  Opcionalmente recebe uma lista de recipientes diretamente, sem leitura de
  banco de dados, pensada para uso em testes.

  Args:
    environment(str):
      Ambiente de execução (ex. "dev", "prod", etc), usado para obter nome do projeto.
      Pode ser omitido se `recipients` não for nulo.
    dataset(str?):
      Dataset para procurar pelos recipientes. Por padrão, `"brutos_sheets"`.
      Pode ser omitido se `recipients` não for nulo..
    table(str):
      Tabela para procurar pelos recipientes. Busca será em `rj-sms(-dev).{dataset}.{table}`.
      Pode ser omitido se `recipients` não for nulo.
    recipients(str | str[]?):
      Um ou vários endereços de email de recipientes. Não lê o banco de dados se definido.
    error(bool?):
      Flag opcional indicando se a mensagem contém erro. Somente endereços marcados
      como recipientes de erro recebem a mensagem.

  Returns:
      recipients(Dict[str, List[str]]):
        Recipientes do email, no formato usado pela API da Iplan.
        Possui 3 chaves, contendo listas de endereços (`"to_addresses"`,
        `"cc_addresses"`, `"bcc_addresses"`) se referindo aos cabeçalhos
        TO, CC e BCC a serem enviados.
  """
  # Se queremos sobrescrever os recipientes do email
  # (ex. enviar somente para uma pessoa, para teste)
  if recipients is not None:
    if type(recipients) is str:
      recipients = [recipients]
    if type(recipients) is list or type(recipients) is tuple:
      recipients = list(recipients)
      log(f"Sobrescrevendo recipientes ({len(recipients)}): {recipients}")
      return {"to_addresses": recipients, "cc_addresses": [], "bcc_addresses": []}
    log(f"Tipo de `recipients` não reconhecido: '{type(recipients)}'; ignorando")

  client = bigquery.Client()

  PROJECT = get_google_project_for_environment(environment=environment)
  DATASET = dataset or "brutos_sheets"
  if not table or not isinstance(table, str) or len(table) == 0:
    raise ValueError(f"Parâmetro `table` ({repr(table)}) deve ser nome válido de tabela")
  TABLE = table

  QUERY = f"""
SELECT email, tipo, recebe_erro
FROM `{PROJECT}.{DATASET}.{TABLE}`
  """
  log(f"Buscando recipientes em `{PROJECT}.{DATASET}.{TABLE}`...")
  rows = [row.values() for row in client.query(QUERY).result()]
  log(f"Encontrado(s) {len(rows)} registro(s)")

  to_addresses = []
  cc_addresses = []
  bcc_addresses = []
  for email, kind, gets_errors in rows:
    email = str(email).strip()
    kind = str(kind).lower().strip()
    gets_errors = str(gets_errors).lower().strip() == "true"
    if not email:
      continue
    if "@" not in email:
      log(f"Recipiente '{email}' não contém '@'; ignorando", level="warning")
      continue

    should_get = (error and gets_errors) or not error
    if should_get:
      if kind == "to":
        to_addresses.append(email)
      elif kind == "cc":
        cc_addresses.append(email)
      elif kind == "bcc":
        bcc_addresses.append(email)
      elif kind == "skip":
        continue
      else:
        log(
          f"Tipo de recipiente '{kind}' (para '{email}') não reconhecido; ignorando",
          level="warning",
        )
    else:
      log(f"Ignorando '{email}' porque não foi marcado como recipiente de erros")

  log(
    f"Recipientes: {len(to_addresses)} (TO); {len(cc_addresses)} (CC); {len(bcc_addresses)} (BCC)"
  )
  # Erro caso por algum motivo não tenha nenhum destinatário
  if len(to_addresses) + len(cc_addresses) + len(bcc_addresses) <= 0:
    raise ValueError("Email não possui recipientes!")

  return {
    "to_addresses": to_addresses,
    "cc_addresses": cc_addresses,
    "bcc_addresses": bcc_addresses,
  }
