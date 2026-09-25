# -*- coding: utf-8 -*-
from pipelines.utils.schedules import create_schedule_list

daily_parameters = [
  {
    "url": "https://docs.google.com/spreadsheets/d/1XySagSMiJs22XaYqq6IY372gqLOr4zA3koCpUf0kjOg",
    "gsheets_sheet_name": "Estabelecimentos",
    "table_id": "estabelecimento_auxiliar",
    "dataset_id": "brutos_sheets",
    "environment": "prod",
  },
  {
    "url": "https://docs.google.com/spreadsheets/d/1C8Fn-c6vwjx3X4nmc7X6V_tFw91rOmojXs9VgUWbQTg",
    "gsheets_sheet_name": "cdi-vps",
    "table_id": "cdi_destinatarios",
    "dataset_id": "brutos_sheets",
    "environment": "prod",
  },
  {
    "url": "https://docs.google.com/spreadsheets/d/1C8Fn-c6vwjx3X4nmc7X6V_tFw91rOmojXs9VgUWbQTg",
    "gsheets_sheet_name": "segurança",
    "table_id": "seguranca_destinatarios",
    "dataset_id": "brutos_sheets",
    "environment": "prod",
  },
  {
    "url": "https://docs.google.com/spreadsheets/d/1-3sJmuLPfmBjS8vPV1ct8qx5-5YdqnDH3_vD4EYkg9Q",
    "gsheets_sheet_name": "Mapeamento",
    "table_id": "usuarios_bigquery",
    "dataset_id": "brutos_sheets",
    "environment": "prod",
  },
  {
    "dataset_id": "brutos_sheets",
    "environment": "prod",
    "gsheets_sheet_name": "CID de Risco Gestacional",
    "table_id": "cids_risco_gestacional",
    "url": "https://docs.google.com/spreadsheets/d/1hpTi-pwJlYqOcdor__v4ONg6yH-2H73qClesdTmGyeo",
  },
  {
    "dataset_id": "brutos_sheets",
    "environment": "prod",
    "gsheets_sheet_name": "Encaminhamentos SER",
    "table_id": "encaminhamentos_ser",
    "url": "https://docs.google.com/spreadsheets/d/1hpTi-pwJlYqOcdor__v4ONg6yH-2H73qClesdTmGyeo",
  },
  {
    "url": "https://docs.google.com/spreadsheets/d/176u8I3xlAW7mFN3M0QADZ2b6jU0iUAfWs4CvKd5rhJU",
    "gsheets_sheet_name": "Municipios",
    "table_id": "municipios_rio",
    "dataset_id": "brutos_sheets",
    "environment": "prod",
  },
  {
    "url": "https://docs.google.com/spreadsheets/d/1WdUZ0vB5Sr2wVhA6bKKwOaQiauLB3r4A4iL2O1E2XwU",
    "gsheets_sheet_name": "Lista",
    "table_id": "procedimentos_ser",
    "dataset_id": "brutos_sheets",
    "environment": "prod",
  },
  {
    "url": "https://docs.google.com/spreadsheets/d/176u8I3xlAW7mFN3M0QADZ2b6jU0iUAfWs4CvKd5rhJU",
    "gsheets_sheet_name": "Bairros",
    "table_id": "bairros_rio",
    "dataset_id": "brutos_sheets",
    "environment": "prod",
  },
  {
    "url": "https://docs.google.com/spreadsheets/d/1b4HBj85ZqAXhS36hI0TZG5y4Obsav0utzRkcOiHMBC8",
    "gsheets_sheet_name": "Classificações CID",
    "table_id": "projeto_c34_cids",
    "dataset_id": "brutos_sheets",
    "environment": "prod",
  },
  {
    "url": "https://docs.google.com/spreadsheets/d/1b4HBj85ZqAXhS36hI0TZG5y4Obsav0utzRkcOiHMBC8",
    "gsheets_sheet_name": "Classificações Procedimentos",
    "table_id": "projeto_c34_procedimentos",
    "dataset_id": "brutos_sheets",
    "environment": "prod",
  },
  {
    "url": "https://docs.google.com/spreadsheets/d/1Y7ji6HL5a2fJGIX-olRCVdv98oQ5nWHaBYUNAF7jTKg",
    "gsheets_sheet_name": "relacao_materiais",
    "dataset_id": "brutos_sheets",
    "table_id": "material_mestre",
    "environment": "prod",
  },
  {
    "url": "https://docs.google.com/spreadsheets/d/1Y7ji6HL5a2fJGIX-olRCVdv98oQ5nWHaBYUNAF7jTKg",
    "gsheets_sheet_name": "UNIDADES POR PROGRAMA",
    "dataset_id": "brutos_sheets",
    "table_id": "material_cobertura_programas",
    "environment": "prod",
  },
  {
    "dataset_id": "brutos_sheets",
    "environment": "prod",
    "gsheets_sheet_name": "Farmácia Digital",
    "table_id": "gerenciamento_acesso_looker_farmacia",
    "url": "https://docs.google.com/spreadsheets/d/1VCtUiRFxMy1KatBfw9chUppPEIPSGDup9wiwfm9-Djo",
  },
  {
    "dataset_id": "brutos_sheets",
    "environment": "prod",
    "gsheets_sheet_name": "ATAS E PROCESSOS VIGENTES",
    "table_id": "compras_atas_processos_vigentes",
    "url": "https://docs.google.com/spreadsheets/d/1fi7MzF0S4OfTym-fjpLR51wIvTLq-WCE706N6eEEWys",
  },
  {
    "dataset_id": "brutos_sheets",
    "environment": "prod",
    "gsheets_sheet_name": "Farmácias",
    "table_id": "aps_farmacias",
    "url": "https://docs.google.com/spreadsheets/d/17b4LRwQ5F5K5jCdeO0_K1NzqoQV9JqSOAuA0HZhG0uI",
  },
  {
    "dataset_id": "brutos_sheets",
    "environment": "prod",
    "gsheets_sheet_name": "Dado",
    "table_id": "sigtap_procedimentos",
    "url": "https://docs.google.com/spreadsheets/d/14kBPPc9VdeMHlNbUVc_C6PLUrwL73yrQfyl14rjrGuA",
  },
  {
    "dataset_id": "brutos_sheets",
    "environment": "prod",
    "gsheets_sheet_name": "sheet1",
    "table_id": "profissionais_cns_cpf_aux",
    "url": "https://docs.google.com/spreadsheets/d/15OhN69JH6GdRK1Ixvr9P-eTSG03lM6qD7hobcT4Ilow",
  },
  {
    "url": "https://docs.google.com/spreadsheets/d/1P4JbgfSpaTyE7Qh3fzSeHIxlxcHclbJWOTFcRE-DwgE",
    "gsheets_sheet_name": "Procedimentos",
    "table_id": "assistencial_procedimento",
    "dataset_id": "brutos_sheets",
    "environment": "prod",
  },
  {
    "dataset_id": "brutos_sheets",
    "environment": "prod",
    "gsheets_sheet_name": "Contas",
    "table_id": "usuarios_permitidos_hci",
    "url": "https://docs.google.com/spreadsheets/d/1jwp5rV3Rwr2NGQy60YQgF47PSFuTcVpE8uKhcrODnRs",
  },
  {
    "dataset_id": "brutos_sheets",
    "environment": "prod",
    "gsheets_sheet_name": "Historico",
    "table_id": "projeto_odontologia_historico",
    "url": "https://docs.google.com/spreadsheets/d/1Af1SvIhQgvRr_da22Qpveb9VNvwLZai_KR69v8eJ1a8",
  },
  {
    "dataset_id": "brutos_sheets",
    "environment": "prod",
    "gsheets_sheet_name": "historico_ap",
    "table_id": "projeto_odontologia_historico_ap",
    "url": "https://docs.google.com/spreadsheets/d/1Af1SvIhQgvRr_da22Qpveb9VNvwLZai_KR69v8eJ1a8",
  },
  {
    "dataset_id": "brutos_sheets",
    "environment": "prod",
    "gsheets_sheet_name": "sigtap_odo",
    "table_id": "projeto_odontologia_sigtap_odo",
    "url": "https://docs.google.com/spreadsheets/d/1Af1SvIhQgvRr_da22Qpveb9VNvwLZai_KR69v8eJ1a8",
  },
  {
    "url": "https://docs.google.com/spreadsheets/d/1gCVtBz0udlcgFKtKJHvjsGwI0wA8kQyNU_bUSGHN8Hw",
    "gsheets_sheet_name": "cids",
    "table_id": "seguranca_cids",
    "dataset_id": "brutos_sheets",
    "environment": "prod",
  },
  {
    "dataset_id": "brutos_cdi",
    "environment": "prod",
    "gsheets_sheet_name": "Controle PGM 2025",
    "table_id": "pgm_2025",
    "url": "https://docs.google.com/spreadsheets/d/1JirkDMgtYUIiJ7z5Zcxnn3sCUAneWwVfgT6u-M3QHE8",
  },
  {
    "dataset_id": "brutos_cdi",
    "environment": "prod",
    "gsheets_sheet_name": "Controle PGM 2026",
    "table_id": "pgm_2026",
    "url": "https://docs.google.com/spreadsheets/d/1iZ8z5HSy7OXRRBk5MhlzYpk1mP1y6A-4r2vzgPi-0k4",
  },
  {
    "dataset_id": "brutos_cdi",
    "environment": "prod",
    "gsheets_sheet_name": "Equipe JR 2025",
    "table_id": "judicial_residual_2025",
    "url": "https://docs.google.com/spreadsheets/d/1JirkDMgtYUIiJ7z5Zcxnn3sCUAneWwVfgT6u-M3QHE8",
  },
  {
    "dataset_id": "brutos_cdi",
    "environment": "prod",
    "gsheets_sheet_name": "Equipe JR",
    "table_id": "judicial_residual_2026",
    "url": "https://docs.google.com/spreadsheets/d/1iZ8z5HSy7OXRRBk5MhlzYpk1mP1y6A-4r2vzgPi-0k4",
  },
  {
    "url": "https://docs.google.com/spreadsheets/d/1JirkDMgtYUIiJ7z5Zcxnn3sCUAneWwVfgT6u-M3QHE8",
    "gsheets_sheet_name": "Controle de Demandas - Equipe Individual",
    "table_id": "equipe_tutela_individual_2025",  # 2025
    "dataset_id": "brutos_cdi",
    "environment": "prod",
  },
  {
    "dataset_id": "brutos_cdi",
    "environment": "prod",
    "gsheets_sheet_name": "Controle de Demandas - Equipe Individual",
    "table_id": "equipe_tutela_individual_2026",
    "url": "https://docs.google.com/spreadsheets/d/1iZ8z5HSy7OXRRBk5MhlzYpk1mP1y6A-4r2vzgPi-0k4",
  },
  {
    "dataset_id": "brutos_cdi",
    "environment": "prod",
    "gsheets_sheet_name": "Controle de Demandas - Equipe Coletiva",
    "table_id": "equipe_tutela_coletiva_2025",
    "url": "https://docs.google.com/spreadsheets/d/1JirkDMgtYUIiJ7z5Zcxnn3sCUAneWwVfgT6u-M3QHE8",
  },
  {
    "dataset_id": "brutos_cdi",
    "environment": "prod",
    "gsheets_sheet_name": "Controle de Demandas - Equipe Coletiva",
    "table_id": "equipe_tutela_coletiva_2026",
    "url": "https://docs.google.com/spreadsheets/d/1iZ8z5HSy7OXRRBk5MhlzYpk1mP1y6A-4r2vzgPi-0k4",
  },
]
# /daily_parameters

weekly_parameters = [
  {
    "dataset_id": "brutos_sheets",
    "environment": "prod",
    "gsheets_sheet_name": "caps",
    "table_id": "contatos_caps",
    "url": "https://docs.google.com/spreadsheets/d/18yQ7o8CRnt-i4nPym0WyzMtVXdd96GvWbQwMQxAHlLM",
  }
]
# /weekly_parameters

monthly_parameters = [
  {
    "dataset_id": "brutos_sheets",
    "environment": "prod",
    "gsheets_sheet_name": "codigos",
    "table_id": "codigos_exames",
    "url": "https://docs.google.com/spreadsheets/d/1IwykWf0glAraHrVDZLJosxuhnpo8AO5EUdj1OSwDqNU",
  },
  # TODO: remover essa planilha quando migrar totalmente pra outra
  {
    "dataset_id": "brutos_sheets",
    "environment": "prod",
    "gsheets_sheet_name": "vacinas",
    "table_id": "vacinas_padronizadas",
    "url": "https://docs.google.com/spreadsheets/d/1TcN_4PHp2xnTzRkKLKO0eG1sjpwO7Nl5g9eAhEBDdOU",
  },
  {
    "dataset_id": "brutos_sheets",
    "environment": "prod",
    "gsheets_sheet_name": "Vacinas",
    "table_id": "depara_vacinas",
    "url": "https://docs.google.com/spreadsheets/d/1lrr5xm_CsMz4sgsEIluQj1j2ys2W61CxBq49bUNIfH8",
  },
]
# /monthly_parameters

# 1x a cada 6 meses
semiannual_parameters = [
  # Municípios do Brasil; IBGE passa anos sem atualizar
  # Vide https://www.ibge.gov.br/explica/codigos-dos-municipios.php
  {
    "url": "https://docs.google.com/spreadsheets/d/176u8I3xlAW7mFN3M0QADZ2b6jU0iUAfWs4CvKd5rhJU",
    "gsheets_sheet_name": "Brasil",
    "table_id": "municipios_brasil",
    "dataset_id": "brutos_sheets",
    "environment": "prod",
  }
]


schedules = [
  *create_schedule_list(
    parameters_list=daily_parameters, interval="daily", config={"minute": 1}
  ),
  *create_schedule_list(
    parameters_list=weekly_parameters,
    interval="weekly",
    config={"weekday": "domingo", "minute": 1},
  ),
  *create_schedule_list(
    parameters_list=monthly_parameters,
    interval="monthly",
    config={"day": 15, "minute": 1},
  ),
  *create_schedule_list(
    parameters_list=semiannual_parameters,
    interval="semiannual",
    config={"month": 1, "minute": 1},
  ),
]
