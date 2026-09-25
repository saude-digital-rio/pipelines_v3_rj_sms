from typing import Literal
from pipelines.constants import CIT
from pipelines.utils.prefect import flow, flow_config

from .tasks import (
	get_brt_gps,
	df_to_markdown,
	compose_message,
	send_message
)

@flow(
	name="Report: BRT GPS",
	owners=[CIT.HERIAN_ID.value],
	tags=['teste']
)
def brt_gps(
	trajeto: str,
	sentido: Literal['ida', 'volta'],
	environment: Literal['dev', 'prod'] = 'dev'
):
	"""
	Args:
		trajeto(str?):
		Trajeto do BRT a ser transmitido
		sentido(str?):
		Sentido do veículo no trajeto.
		environment(str?):
		Ambiente de execução.
 """

	df = get_brt_gps(environment=environment)
	md_table = df_to_markdown(
					df=df, 
					trajeto=trajeto, 
					sentido=sentido
				)
	title, message = compose_message(
						table=md_table, 
						trajeto=trajeto, 
						sentido=sentido
						)
	send_message(title=title, message=message)


_flows = [flow_config(flow=brt_gps, schedules=[])]
