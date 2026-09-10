# -*- coding: utf-8 -*-
from enum import Enum


class constants(Enum):
  # Parâmetros para sqlserver_backup (vitacare_historic)
  SQLSERVER_BACKUP_PARAMS = {
    "backup_type": "vitacare_historic",
    "bucket_name": "rj_subpav_vitacare_backups",
    "instance_name": "vitacare",
    "file_pattern": "HISTÓRICO_PEPVITA_RJ/AP*/vitacare_historic_*_*_*.bak",
  }

  # Parâmetros do dbt run
  DBT_PARAMS = {
    "command": "run",
    "rename_flow": True,
    "select": "tag:vitacare_historico",
    "send_discord_report": True,
  }
