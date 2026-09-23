# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule_list

vitacare_historico = [
  {
    "root_folder_id": "1VUdm8fixnUs_dJrcflsNvzXIGPX6e-2r",
    "bucket_name": "vitacare_backups_gdrive",
    "table_id": "log_gdrive_to_gcs",
    "start_date": "M-0",
    "end_date": "D-0",
    "environment": "prod",
  }
]

informes = [
  {
    "root_folder_id": "1H_49fLhbT0bWYk8gBKLOdYHgT_xODpg7",
    "bucket_name": "vitacare_informes_mensais_gdrive",
    "table_id": "log_gdrive_to_gcs",
    "start_date": "M-0",
    "end_date": "D-0",
    "environment": "prod",
  }
]


schedules = [
  # Desativado pois os backups da vitacare estão sendo enviados para um bucket
  # e não mais para pasta no Google Drive
  # *create_schedule_list(
  #   parameters_list=vitacare_historico,
  #   interval="monthly",
  #   config={"day": 7, "hour": 16, "minute": 0},
  # ),
  *create_schedule_list(
    parameters_list=informes,
    interval="monthly",
    config={"day": 7, "hour": 17, "minute": 0},
  )
]
