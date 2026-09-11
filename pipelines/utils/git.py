# -*- coding: utf-8 -*-
import os
import re
import shutil
import tarfile
from typing import Literal

import requests

from pipelines.utils.logger import log
from pipelines.utils.prefect import authenticated_task as task


def download_gh_repo(
  repo: str,
  branch: str = "main",
  destination_path: str = None,
  if_destination_exists: Literal["delete", "raise"] = "raise",
  repository_folder: str = None,
  token: str = None,
) -> str:
  """
  Baixa o conteúdo de um repositório no GitHub para uma pasta local.

  Args:
    repo (str):
      Nome do repositório no GitHub, no formato "organizacao/repositorio".
      Ex.: "prefeitura-rio/queries-rj-sms"
    branch (str?):
      Branch a ser baixada. Por padrão, possui o valor "main".
    destination_path (str?):
      Pasta para onde o código baixado deve ser baixado.
      Por padrão, "/tmp/runtime_download_repository".
    if_destination_exists (str?):
      Ação a ser feita se a pasta de destino local já existir.
      Recebe valores "delete" (pasta existente deve ser apagada)
      ou "raise" (FileExistsError). Valor padrão é "raise".
    repository_folder (str?):
      Pasta no repositório de onde buscar o conteúdo.
      Se não for informado, o repositório inteiro é copiado.
    token (str?):
      Personal Access Token (PAT) do GitHub para repositórios privados.
      Se não informado, o repositório é acessado publicamente.

  Returns:
    path (str): Caminho onde o repositório baixado está.
  """
  repo_format_matches = re.fullmatch(r"[a-z0-9_\-]+/[a-z0-9_\-]+", repo, re.IGNORECASE)
  if not repo_format_matches:
    raise ValueError(
      f"Nome do repositório '{repo}' deve ser '<user|org>/<repo>', "
      "como 'prefeitura-rio/queries-rj-sms'"
    )
  if not branch:
    branch = "main"

  # Se não recebemos uma pasta de destino, pasta padrão em /tmp
  if not destination_path:
    destination_path = "/tmp/runtime_download_repository"

  # Se a pasta de destino já existe
  if os.path.exists(destination_path):
    # Se não podemos apagá-la, erro aqui
    if if_destination_exists == "raise":
      raise FileExistsError(f"Diretório destino '{destination_path}' já existe!")
    # Senão, apaga a pasta e conteúdos
    elif if_destination_exists == "delete":
      if not destination_path.startswith("/tmp"):
        log(f"Apagando pasta '{destination_path}'...", level="warning")
      shutil.rmtree(destination_path)
    else:
      raise ValueError(
        f"Valor inesperado para `if_destination_exists`: '{if_destination_exists}'"
      )
  # Se chegamos aqui, não existe mais a pasta; cria
  os.makedirs(destination_path)

  tar_filename = os.path.join(destination_path, f"repo-{branch}.tar.gz")
  headers = {"Authorization": f"token {token}"} if token else {}
  req = requests.get(
    f"https://github.com/{repo}/archive/refs/heads/{branch}.tar.gz",
    headers=headers,
    stream=True,
  )
  with open(tar_filename, "wb") as fd:
    for chunk in req.iter_content(chunk_size=None):
      fd.write(chunk)

  if not repository_folder or not repository_folder.strip(" /"):
    repository_folder = ""
  else:
    repository_folder = repository_folder.strip(" /") + "/"

  # Remove o nome do repositório do caminho
  def members(tf: tarfile.TarFile):
    repo_name = repo.split("/")[1]
    subfolder = f"{repo_name}-{branch}/{repository_folder}"
    L = len(subfolder)
    for member in tf.getmembers():
      if member.path.startswith(subfolder):
        member.path = member.path[L:]
        yield member

  with tarfile.open(tar_filename, "r:gz") as tar:
    tar.extractall(path=destination_path, filter="data", members=members(tar))

  os.remove(tar_filename)

  return destination_path


@task
def download_gh_repo_task(
  repo: str,
  branch: str = "main",
  destination_path: str = None,
  if_destination_exists: Literal["delete", "raise"] = "raise",
  repository_folder: str = None,
  token: str = None,
) -> str:
  """
  Baixa o conteúdo de um repositório no GitHub para uma pasta local.

  Args:
    repo (str):
      Nome do repositório no GitHub, no formato "organizacao/repositorio".
      Ex.: "prefeitura-rio/queries-rj-sms"
    branch (str?):
      Branch a ser baixada. Por padrão, possui o valor "main".
    destination_path (str?):
      Pasta para onde o código baixado deve ser baixado.
      Por padrão, "/tmp/runtime_download_repository".
    if_destination_exists (str?):
      Ação a ser feita se a pasta de destino local já existir.
      Recebe valores "delete" (pasta existente deve ser apagada)
      ou "raise" (FileExistsError). Valor padrão é "raise".
    repository_folder (str?):
      Pasta no repositório de onde buscar o conteúdo.
      Se não for informado, o repositório inteiro é copiado.
    token (str?):
      Personal Access Token (PAT) do GitHub para repositórios privados.
      Se não informado, o repositório é acessado publicamente.

  Returns:
    path (str): Caminho onde o repositório baixado está.
  """
  return download_gh_repo(
    repo,
    branch=branch,
    destination_path=destination_path,
    if_destination_exists=if_destination_exists,
    repository_folder=repository_folder,
    token=token,
  )
