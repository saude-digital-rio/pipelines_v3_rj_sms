# -*- coding: utf-8 -*-
from prefect.schedules import Cron

from pipelines.utils.schedules import create_schedule_list

daily_flow_parameters = [
  {
    "source_project_name": "basedosdados",
    "source_dataset_name": "br_ms_cnes",
    "source_table_list": [
      "estabelecimento",
      "equipamento",
      "habilitacao",
      "leito",
      "profissional",
    ],
    "destination_dataset_name": "brutos_cnes_ftp_staging",
    "environment": "prod",
  },
  {
    "source_project_name": "rj-cvl",
    "source_dataset_name": "adm_contrato_gestao",
    "source_table_list": [
      "administracao_perfil",
      "administracao_unidade",
      "agencia",
      "banco",
      "conta_bancaria",
      "conta_bancaria_tipo",
      "contrato",
      "contrato_terceiros",
      "despesas",
      "estado_entrega",
      "fechamento",
      "fornecedor",
      "historico_alteracoes",
      "itens_nota_fiscal",
      "plano_contas",
      "receita_dados",
      "receita_item",
      "rubrica",
      "saldo_dados",
      "saldo_item",
      "secretaria",
      "tipo_arquivo",
      "tipo_documento",
      "usuario",
      "usuario_sistema",
    ],
    "destination_dataset_name": "brutos_osinfo_staging",
    "environment": "prod",
  },
  {
    "source_project_name": "rj-sma",
    "source_dataset_name": "recursos_humanos_ergon_saude",
    "source_table_list": ["funcionarios_sms"],
    "destination_dataset_name": "brutos_ergon_staging",
    "environment": "prod",
    "dbt_select_exp": "tag:ergon",
  },
  {
    "source_project_name": "rj-smfp",
    "source_dataset_name": "compras_materiais_servicos_sigma_staging",
    "source_table_list": ["classe", "grupo", "material", "subclasse"],
    "destination_dataset_name": "brutos_sigma_staging",
    "environment": "prod",
  },
  {
    "source_project_name": "iplan-rio-sms",
    "source_dataset_name": "billing",
    "source_table_list": ["gcp_billing_export_resource_v1_01BAED_6E64B4_606041"],
    "destination_dataset_name": "brutos_faturamento_nuvem_staging",
    "environment": "prod",
  },
]

weekly_flow_parameters = [
  {
    "source_project_name": "blp-medirec",
    "source_dataset_name": "medirec_ext",
    "source_table_list": ["cielab_exams_sms", "cielab_sectionitems_sms"],
    "destination_dataset_name": "brutos_medirec",
    "environment": "prod",
    "horribly_inefficient_method": True,  # !! não copie !!
    "horribly_inefficient_chunk_size": 250_000,
  }
]

cegonha_flow_parameters = [
  {
    "source_project_name": "rj-crm-registry",
    "source_dataset_name": "rmi_conversas",
    "source_table_list": ["chatbot"],
    "destination_dataset_name": "brutos_iplanrio",
    "environment": "prod",
  }
]


daily_schedules = create_schedule_list(
  parameters_list=daily_flow_parameters, interval="daily", config={"hour": 5}
)
weekly_schedules = create_schedule_list(
  parameters_list=weekly_flow_parameters, interval="weekly", config={"hour": 4}
)

cegonha_schedules = [
  # de 15 em 15min entre 7:00-21:00
  Cron("*/15 7-21 * * *", timezone="America/Sao_Paulo", parameters=params)
  for params in cegonha_flow_parameters
]

schedules = daily_schedules + weekly_schedules + cegonha_schedules
