import requests
from pandas import DataFrame
from prefect import task

from pipelines.utils.monitor import send_discord_message

from .constants import constants as flow_consts

URL = flow_consts.API_URL.value


@task(retries=3)
def get_brt_gps(environment):
  response = requests.get(URL)
  payload = response.json()
  df = DataFrame(payload["veiculos"])
  return df


@task
def df_to_markdown(df, trajeto, sentido):
  df_selected = df.loc[
    (df["sentido"] == sentido)
    & (df["trajeto"].str.contains(trajeto))
    & (df["ignicao"] == 1)
  ]

  value_counts = df_selected["trajeto"].value_counts()
  md_table = DataFrame(value_counts).to_string()
  return md_table


@task
def compose_message(table, trajeto, sentido):
  title = f"Relatório BRT GPS Trajeto `{trajeto}` - Sentido `{sentido}`"
  body_message = f"""	
	``` 
	{table}
	``` 
	"""
  return title, body_message


@task
def send_message(message, title):
  send_discord_message(title=title, message=message, slug="freshness")
