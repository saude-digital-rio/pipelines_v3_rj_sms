# -*- coding: utf-8 -*-
from enum import Enum


class constants(Enum):
  # Parâmetros para gdrive_to_gcs (vitacare_historico)
  GDRIVE_TO_GCS_PARAMS = {
    "root_folder_id": "1VUdm8fixnUs_dJrcflsNvzXIGPX6e-2r",
    "bucket_name": "vitacare_backups_gdrive",
    "table_id": "log_gdrive_to_gcs",
    "start_date": "M-0",
    "end_date": "D-0",
    "environment": "prod",
  }

  # Parâmetros para sqlserver_backup (vitacare_historic)
  SQLSERVER_BACKUP_PARAMS = {
    "backup_type": "vitacare_historic",
    "bucket_name": "vitacare_backups_gdrive",
    "instance_name": "vitacare",
    "file_pattern": "HISTÓRICO_PEPVITA_RJ/AP*/vitacare_historic_*_*_*.bak",
    "environment": "prod",
  }

  # Parâmetros para vitacare_historico
  VITACARE_HISTORICO_PARAMS = {"environment": "prod"}

  # Parâmetros do dbt run
  DBT_PARAMS = (
    {
      "command": "run",
      "environment": "prod",
      "rename_flow": True,
      "select": "tag:vitacare_historico",
      "send_discord_report": True,
    },
  )
