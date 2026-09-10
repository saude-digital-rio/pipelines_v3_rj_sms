from datetime import datetime, time, timedelta

from prefect import task

from pipelines.utils.datetime import SAO_PAULO_TZ, now
from pipelines.utils.prefect import create_flow_run

from .flows import orquestracao_vitacare


@task
def schedule_next_runs(environment: str):
  """
  Agenda o orquestrador para os próximos 5 dias.
  """
  today = now().date()

  for i in range(1, 6):
    next_date = today + timedelta(days=i)
    next_schedule = datetime.combine(next_date, time(hour=1, tzinfo=SAO_PAULO_TZ))

    create_flow_run(
      flow=orquestracao_vitacare,
      parameters={"environment": "prod", "should_repeat": False},
      environment=environment,
      scheduled_time=next_schedule,
    )
