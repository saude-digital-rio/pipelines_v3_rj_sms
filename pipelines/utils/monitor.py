# -*- coding: utf-8 -*-
import asyncio
import os
from typing import List, Literal, Optional

import aiohttp
from discord import AllowedMentions, Embed, File, Webhook
from prefect.context import FlowRunContext, TaskRunContext

from pipelines.utils.cleanup import prettify_byte_size
from pipelines.utils.env import get_current_environment, get_prefect_url
from pipelines.utils.infisical import get_secret
from pipelines.utils.logger import log

type DiscordWebhookSlug = Literal[
  "custo_jobs",
  "data-ingestion",
  "dbt-runs",
  "dbt-runs-sms",
  "error",
  "hci_status",
  "warning",
]


async def send_discord_webhook(
  slug: DiscordWebhookSlug,
  text_content: Optional[str] = None,
  embed_content: Optional[List[Embed]] = None,
  file_path: Optional[str] = None,
  username: Optional[str] = None,
):
  """
  Envia mensagem para um webhook do Discord. Ainda que
  `text_content` e `embed_content` sejam parâmetros opcionais,
  é necessário definir pelo menos um.

  Args:
    slug(str):
      Referência ao canal destino, usada para obter o URL do webhook
    text_content(str?):
      Conteúdo textual da mensagem
    embed_content(list[Embed]?):
      Conteúdo já formatado como `Embed`
    file_path(str?):
      Caminho de arquivo a anexar à mensagem; ex.: arquivo de logs em .txt
    username(str?):
      Nome de usuário da conta que envia a mensagem
  """
  if not slug:
    raise ValueError("É preciso informar um `slug`!")

  environment = get_current_environment()
  secret_name = f"DISCORD_WEBHOOK_URL_{slug.upper()}"
  webhook_url = get_secret(
    secret_name=secret_name, environment=environment, path="/discord"
  )

  if not text_content and not embed_content:
    raise ValueError("É preciso definir pelo menos um entre conteúdo textual e Embed")
  if text_content and len(text_content) > 2000:
    raise ValueError(
      f"Conteúdo textual é longo demais: possui tamanho {len(text_content)}, máximo é 2000"
    )
  if embed_content and len(embed_content) > 5:
    raise ValueError(f"Embeds demais: possui {len(embed_content)} entradas, máximo é 5")

  params = {
    "content": text_content or "",
    "embeds": embed_content or [],
    "allowed_mentions": AllowedMentions(users=True),
    "username": username,
  }

  if file_path:
    if not os.path.exists(file_path):
      log(f"Arquivo '{file_path}' não existe! Ignorado", level="error")
    else:
      file = File(file_path, filename=os.path.basename(file_path))
      params["file"] = file
      # Código legado, migrei por abundância de caução; acho que
      # a gente nunca nem usa PNG, mas deixo abaixo como referência
      # if file_path.endswith(".png"):
      #   embed = Embed()
      #   embed.set_image(url=f"attachment://{file_path}")
      #   params["embeds"].append(embed)

  log(f"Enviando mensagem para Discord (slug={slug})", level="info")
  log(f"webhook_url: {webhook_url}", level="info")

  async with aiohttp.ClientSession() as session:
    webhook = Webhook.from_url(webhook_url, session=session)
    try:
      await webhook.send(**params)
    except RuntimeError as e:
      log("Erro ao enviar mensagem para webhook do Discord!", level="error")
      raise e


def send_discord_embed(
  slug: DiscordWebhookSlug, contents: List[Embed], username: Optional[str] = None
):
  """
  Envia um ou mais Embeds pré-formatados para o Discord

  Args:
    contents(list[Embed]): Conteúdo a ser enviado
    slug(str): Referência ao canal de destino
    username(str?): Nome de usuário da conta que envia a mensagem
  """
  return send_discord_webhook(slug=slug, embed_content=contents, username=username)


def send_discord_message(
  title: str,
  message: str,
  slug: DiscordWebhookSlug,
  file_path: Optional[str] = None,
  multiple_messages_ok: bool = False,
):
  """
  Envia mensagem textual a um canal do Discord, prefixada de informações
  sobre o flow e task que fizeram a requisição

  Args:
    title(str): Título da mensagem, formatado como H2
    message(str): Conteúdo textual da mensagem
    slug(str): Referência ao canal de destino
    multiple_messages_ok(bool):
      Flag indicando se, caso a mensagem seja longa demais, ela deve ser
      quebrada em várias mensagens distintas. Por padrão, é `False`, e
      mensagens longas são truncadas com "...". Somente passe `True` em
      flows cujas mensagens são muito relevantes e não muito longas –
      p.ex. relatórios do dbt. Caso contrário, envie uma mensagem resumida
      no Discord e coloque maiores detalhes nos logs do flow em si.
  """
  environment = get_current_environment()
  prefect_url = get_prefect_url()
  header_lines = [f"## {title}", f"> Environment: {environment}"]

  fr_ctx = FlowRunContext.get()
  if fr_ctx:
    flow_name = fr_ctx.flow.name
    flow_run_id = str(fr_ctx.flow_run.id)
    if prefect_url == "localhost":
      header_lines.append(f"> Flow (v3): {flow_name}")
    else:
      header_lines.append(
        f"> Flow (v3): [{flow_name}]({prefect_url}/runs/flow-run/{flow_run_id})"
      )

  tr_ctx = TaskRunContext.get()
  if tr_ctx:
    task_name = tr_ctx.task.name
    task_run_id = str(tr_ctx.task_run.id)
    if prefect_url == "localhost":
      header_lines.append(f"> Task: {task_name}")
    else:
      header_lines.append(
        f"> Task: [{task_name}]({prefect_url}/runs/task-run/{task_run_id})"
      )

  header_content = "\n".join([*header_lines, ""])

  DISCORD_MAX_CHARS = 2000  # Limite, por mensagem, do próprio Discord
  message = str(message).strip()
  message_max_char_count = DISCORD_MAX_CHARS - len(header_content)
  if not multiple_messages_ok and len(message) > message_max_char_count:
    log("Mensagem excede limite de caracteres; texto será truncado", level="warning")
    message = f"{message[: message_max_char_count - 3]}..."

  message_lines = message.split("\n")

  pages = []
  current_buffer = header_content  # Primeiro buffer já começa com o cabeçalho
  for line in message_lines:
    line = line.strip()
    if len(line) < 1:
      continue

    # Se juntar a próxima linha ao buffer ainda não atinge o limite de caracteres
    if len(current_buffer) + 1 + len(line) <= DISCORD_MAX_CHARS:
      # Adiciona ela ao buffer
      current_buffer += "\n" + line
      continue

    # Caso contrário, esse buffer está pronto; adiciona às mensagens já prontas
    current_buffer = current_buffer.strip()
    if len(current_buffer) > 0:
      pages.append(current_buffer)

    # Se a próxima linha por si só não passa do limite de caracteres, ela é o novo buffer
    if len(line) <= DISCORD_MAX_CHARS:
      current_buffer = line
      continue

    # Aqui, a linha passa do limite de caracteres; temos que quebrá-la
    # Se a linha possui caracteres DEMAIS, morre aqui
    if len(line) > DISCORD_MAX_CHARS * 3:
      raise ValueError(
        f"Tamanho da linha excede máximo permitido: {len(line)} > {DISCORD_MAX_CHARS * 3}"
      )
    # Quebra a linha em pedaços de `DISCORD_MAX_CHARS` e adiciona ao buffer
    for i in range(0, len(line), DISCORD_MAX_CHARS):
      chunk = line[i : i + DISCORD_MAX_CHARS].strip()
      if i == 0:
        current_buffer = chunk
      else:
        pages.append(current_buffer)
        current_buffer = chunk
  # Aqui, ainda devemos ter o final da mensagem no buffer; adiciona
  pages.append(current_buffer)

  async def send_multiple_discord_messages(contents):
    for idx, content in enumerate(contents):
      # Só envia o arquivo anexado na última mensagem
      if idx == len(contents) - 1:
        await send_discord_webhook(slug=slug, text_content=content, file_path=file_path)
      else:
        await send_discord_webhook(slug=slug, text_content=content)

  asyncio.run(send_multiple_discord_messages(pages))


def get_ram_snapshot():
  meminfo = {}
  with open("/proc/meminfo", "r") as f:
    for line in f:
      # Só temos interesse em ler RAM total e disponível
      if "MemTotal" in meminfo and "MemAvailable" in meminfo:
        break

      line: str
      parts = line.split()
      key = parts[0].replace(":", "")
      if key not in ("MemTotal", "MemAvailable"):
        continue
      # Valores em kB então x1024 aqui
      meminfo[key] = int(parts[1]) * 1024

  total = meminfo["MemTotal"]
  available = meminfo["MemAvailable"]
  used = total - available
  return {
    "total": total,
    "available": available,
    "used": used,
    "used_pct": (used / total) * 100,
    "total_pretty": prettify_byte_size(total),
    "available_pretty": prettify_byte_size(available),
    "used_pretty": prettify_byte_size(used),
  }
