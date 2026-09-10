# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule_list

flow_parameters = [
  {
    "backup_type": "rnds_vaccine",
    # "bucket_name": "vitacare_backups_gdrive", -- bucket utilizado anteriormente
    "bucket_name": "rj_subpav_vitacare_backups",
    "instance_name": "vitacare",
    "file_pattern": "RNDS_Vaccine_Historic_*.bak",
    "environment": "prod",
  },
]

schedules = [
  *create_schedule_list(
    parameters_list=flow_parameters,
    interval="monthly",
    config={"day": 7, "hour": 18, "minute": 0},
  )
]
