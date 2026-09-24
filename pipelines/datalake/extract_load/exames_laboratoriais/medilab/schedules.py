# -*- coding: utf-8 -*-
from pipelines.datalake.extract_load.exames_laboratoriais.medilab.constants import (
  MEDILAB_CONFIG,
)
from pipelines.utils.schedules import create_schedule_list

# Parâmetros
flow_parameters = [
  {
    "environment": "prod",
    "dataset_id": MEDILAB_CONFIG["DATASET_ID"],
    "table_id": MEDILAB_CONFIG["TABLE_ID"],
    "gcs_uri": MEDILAB_CONFIG["GCS_URI"],
  }
]

schedules = [
  *create_schedule_list(
    parameters_list=flow_parameters, interval="daily", config={"hour": 12, "minute": 0}
  )
]
