# -*- coding: utf-8 -*-
from prefect.schedules import Cron

schedules = [
  Cron(
    "0 16 * * 0#1",  # 16:00, primeiro domingo do mês
    timezone="America/Sao_Paulo",
    parameters={"environment": "prod", "should_repeat": True},
  )
]

# schedules = [
#   create_schedule(
#     parameters={"environment": "prod"},
#     interval="monthly",
#     config={"day": 7, "hour": 16, "minute": 0},
#   ),
# ]
