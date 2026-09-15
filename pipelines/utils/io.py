# -*- coding: utf-8 -*-
import os
import re
import shutil
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import List

from pipelines.utils.cleanup import prettify_byte_size
from pipelines.utils.datetime import today_str
from pipelines.utils.env import is_local_run
from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task
from pipelines.utils.prefect import get_normalized_flow_name


def get_gcs_mount_dir():
  """
  Retorna a pasta local onde o bucket do GCS está montado.
  Em execuções locais, retorna uma pasta qualquer em `/tmp`.
  """
  flow_folder = get_normalized_flow_name()
  # Se estamos rodando localmente, então não queremos dar
  # erro pra isso; usa pasta local no lugar
  if is_local_run():
    path = f"/tmp/pipelines/__mnt_gcs/{flow_folder}"
    os.makedirs(path, exist_ok=True)
    return path

  # Caso contrário, então /mnt/gcs já deve existir
  # Se não existe, então algo não está configurado corretamente
  if not os.path.exists("/mnt/gcs"):
    raise RuntimeError("GCS não foi montado :(")

  path = f"/mnt/gcs/{flow_folder}"
  os.makedirs(path, exist_ok=True)
  return path


def create_tmp_data_folder(
  prefix: str = None, suffix: str = None, in_gcs: bool = False
) -> str:
  """
  Cria uma pasta temporária (ou em `/tmp/pipelines`, ou no GCS) com
  nome aleatório, para uso geral. Opcionalmente recebe um prefixo
  ou sufixo, adicionados em volta dos caracteres aleatórios.

  Retorna o caminho da pasta.
  """
  prefix = "" if not prefix else f"{prefix}_"
  suffix = "" if not suffix else f"_{suffix}"

  # UUID aqui seria mais "confiável" (e mais lento), mas
  # esses arquivos não vão persistir por muito tempo
  # (cada flow run é um container novo), e `urandom(S)`
  # gera S bytes = S*2 caracteres = 16^(S*2) opções...
  folder_name = f"{prefix}{os.urandom(8).hex()}{suffix}"
  today = today_str()

  root = get_gcs_mount_dir() if in_gcs else f"/tmp/pipelines/{get_normalized_flow_name()}"
  path = f"{root}/tmp/{today}/{folder_name}"

  if os.path.exists(path):
    log(f"Uh oh! '{path}' já existia; deletando...")
    shutil.rmtree(path)
  os.makedirs(path)
  return path


@task
def create_data_folders_task() -> dict[str, str]:
  """
  Cria duas pastas, './data/raw' e './data/partition_directory',
  e retorna seus caminhos completos. Caso uma pasta já exista,
  apaga seu conteúdo.

  Returns:
    dict: O dicionário:
    {
      "data": "{cwd}/data/raw"
      "partition_directory": "{cwd}/data/partition_directory"
    }
  """
  try:
    path_raw = os.path.join(os.getcwd(), "data", "raw")
    path_partition = os.path.join(os.getcwd(), "data", "partition_directory")

    if os.path.exists(path_raw):
      shutil.rmtree(path_raw, ignore_errors=False)
    os.makedirs(path_raw)

    if os.path.exists(path_partition):
      shutil.rmtree(path_partition, ignore_errors=False)
    os.makedirs(path_partition)

    folders = {"raw": path_raw, "partition_directory": path_partition}

    log(f"Pastas criadas: {folders}")
    return folders

  except Exception as e:  # pylint: disable=W0703
    log(f"Erro ao criar pastas {e}", level="error")
    sys.exit(f"Erro ao criar pastas: {e}")


def create_partitions(
  data_path: str | list[str],
  partition_directory: str,
  level: str = "day",
  partition_date: str = None,
  file_type: str = "csv",
) -> None:
  """
  Copia arquivos para diretórios particionados por dia ou mês.
  """
  log(f"Data path: {data_path}")
  log(f"Partition directory: {partition_directory}")
  log(f"Partition level: {level}")
  log(f"Partition date: {partition_date}")
  log(f"File type: {file_type}")

  if isinstance(data_path, str):
    if os.path.isdir(data_path):
      files = Path(data_path).glob(f"*.{file_type}")
    else:
      files = [data_path]
  elif isinstance(data_path, list):
    files = data_path
  else:
    raise ValueError("data_path deve ser uma string ou uma lista")

  for file_name in files:
    if level == "day":
      if partition_date is None:
        date_match = re.search(r"\d{4}-\d{2}-\d{2}", str(file_name))
        if not date_match:
          raise ValueError(
            "Nome do arquivo deve conter uma data YYYY-MM-DD para partição diária"
          )
        parsed_date = datetime.strptime(date_match.group(), "%Y-%m-%d")
      else:
        parsed_date = datetime.strptime(partition_date, "%Y-%m-%d")

      output_directory = (
        f"{partition_directory}/ano_particao={int(parsed_date.strftime('%Y'))}"
        f"/mes_particao={int(parsed_date.strftime('%m'))}"
        f"/data_particao={parsed_date.strftime('%Y-%m-%d')}"
      )

    elif level == "month":
      if partition_date is None:
        date_match = re.search(r"\d{4}-\d{2}", str(file_name))
        if not date_match:
          raise ValueError(
            "Nome do arquivo deve conter uma data YYYY-MM para partição mensal"
          )
        parsed_date = datetime.strptime(date_match.group(), "%Y-%m")
      else:
        parsed_date = datetime.strptime(partition_date, "%Y-%m")

      output_directory = (
        f"{partition_directory}/ano_particao={int(parsed_date.strftime('%Y'))}"
        f"/mes_particao={parsed_date.strftime('%m')}"
      )

    else:
      raise ValueError("level deve ser 'day' ou 'month'")

    os.makedirs(output_directory, exist_ok=True)
    shutil.copy(file_name, output_directory)
    log(f"Arquivo {file_name} copiado para partição: {output_directory}")

  log("Partições criadas com sucesso")


@task
def create_partitions_task(
  data_path: str | list[str],
  partition_directory: str,
  level: str = "day",
  partition_date: str = None,
  file_type: str = "csv",
) -> None:
  """
  Task Prefect v3 para copiar arquivos para diretórios particionados.
  """
  return create_partitions(
    data_path=data_path,
    partition_directory=partition_directory,
    level=level,
    partition_date=partition_date,
    file_type=file_type,
  )


def list_files_in_folder(folder: str, endswith: str = None, recursive: bool = False):
  """
  Retorna uma lista com o caminho de arquivos em uma pasta.
  Args:
          folder(str):
                  Caminho da pasta
          endswith(str?):
                  Filtro pelo final de arquivos; ex. `endswith=".csv"`
          recursive(bool?):
                  Quando `recursive=True` é passado, também procura por
                  arquivos em subpastas
  """
  files = []
  for dirpath, _, filenames in os.walk(folder):
    files.extend(
      [
        os.path.join(dirpath, file)
        for file in filenames
        if (not endswith or file.endswith(endswith))
      ]
    )
    if not recursive:
      break
  return files


@task
def list_files_in_folder_task(folder: str, endswith: str = None, recursive: bool = False):
  """
  Retorna uma lista com o caminho de arquivos em uma pasta.
  Args:
    folder(str):
      Caminho da pasta
    endswith(str?):
      Filtro pelo final de arquivos; ex. `endswith=".csv"`
    recursive(bool?):
      Quando `recursive=True` é passado, também procura por
      arquivos em subpastas
  """
  return list_files_in_folder(folder, endswith=endswith, recursive=recursive)


def zip_files_from_list(
  filelist: List[str], output_path: str = None, output_filename: str = None
) -> str:
  """
  Recebe uma lista de caminhos absolutos de arquivos e retorna o caminho
  absoluto de um arquivo ZIP contendo os arquivos passados.
  Opcionalmente recebe o nome do arquivo a ser criado em `output_filename`.
  """
  if not output_path:
    output_path = create_tmp_data_folder()
  output_path.rstrip("/")

  if not output_filename:
    output_filename = f"{os.urandom(8).hex()}.zip"
  elif not output_filename.endswith(".zip"):
    output_filename = f"{output_filename}.zip"

  zip_filepath = f"{output_path}/{output_filename}"

  log(f"Criando ZIP de {len(filelist)} arquivo(s)...")
  with zipfile.ZipFile(zip_filepath, "w", zipfile.ZIP_DEFLATED) as zipf:
    for file_path in filelist:
      if not os.path.exists(file_path):
        log(f"Arquivo '{file_path}' não existe!", level="error")
        continue
      zipf.write(file_path, arcname=os.path.basename(file_path))

  filesize = os.path.getsize(zip_filepath)
  log(f"Arquivo '{zip_filepath}' tem tamanho {prettify_byte_size(filesize)}")
  return zip_filepath


@task
def zip_files_from_list_task(
  filelist: List[str], output_path: str = None, output_filename: str = None
):
  """
  Recebe uma lista de caminhos absolutos de arquivos e retorna o caminho
  absoluto de um arquivo ZIP contendo os arquivos passados.
  """
  return zip_files_from_list(
    filelist, output_path=output_path, output_filename=output_filename
  )


def unzip_file(filepath: str, output_path: str = None) -> str:
  """
  Recebe o caminho absoluto de um ZIP, extrai todo o conteúdo dele,
  e retorna o caminho absoluto da pasta contendo os arquivos
  """
  if not output_path:
    output_path = create_tmp_data_folder()
  output_path.rstrip("/")

  try:
    with zipfile.ZipFile(filepath, "r") as zip_ref:
      log(f"Extraindo conteúdo do ZIP para '{output_path}'")
      zip_ref.extractall(output_path)
  except Exception as e:
    log("Erro extraindo arquivo!", level="error")
    raise e
  return output_path


@task
def unzip_file_task(filepath: str, output_path: str = None):
  return unzip_file(filepath=filepath, output_path=output_path)


def get_file_size(
  path: str,
  raise_if_missing: bool = False,
  raise_if_not_file: bool = False,
  pretty: bool = False,
) -> str | int:
  """
  Retorna tamanho do arquivo especificado. Por padrão, caso o arquivo
  não exista ou o caminho não seja um arquivo, retorna 0; opcionalmente
  dispara `RuntimeError`s nesses casos.

  Se `pretty=True`, retorna string já formatada (ex.: '4.8 MiB').
  """
  if not os.path.exists(path):
    if raise_if_missing:
      raise RuntimeError(f"Arquivo '{path}' não existe!")
    return 0
  if not os.path.isfile(path):
    if raise_if_not_file:
      raise RuntimeError(f"'{path}' não é um arquivo!")
    return 0
  size = os.path.getsize(path)
  if pretty:
    return prettify_byte_size(size)
  return size
