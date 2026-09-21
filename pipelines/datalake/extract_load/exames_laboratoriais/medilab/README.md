# Medilab - Extração e Carga de Relatórios Laboratoriais (GCS)

Esta pasta contém os pipelines (flows) e tarefas (tasks) responsáveis pela extração e carga de dados de relatórios laboratoriais Medilab armazenados no Google Cloud Storage (GCS) para o Datalake.

## Arquitetura dos Flows (`flows.py`)

A arquitetura utiliza um padrão de **Extração Híbrida**, permitindo tanto execuções automatizadas quanto cargas pontuais a partir de caminhos específicos no GCS.

### 1. Flow: `Extração: Relatórios Medilab`

- **Objetivo:** Extrair relatórios laboratoriais estruturados em formato CSV do GCS, realizar a padronização dos dados e carregar os registros processados na camada bruta do Datalake.

- **Funcionamento:**

  - Recebe o caminho de origem no GCS, podendo ser informado um arquivo específico ou uma pasta.

  - Quando é fornecido um diretório, identifica automaticamente o arquivo CSV mais recente disponível para processamento.

  - Realiza o download do arquivo de origem para o ambiente temporário do worker.

  - Carrega o arquivo CSV utilizando Pandas para processamento dos dados.

  - Realiza a higienização e padronização dos nomes das colunas, adequando os cabeçalhos ao padrão utilizado pelo BigQuery.

  - Adiciona os metadados necessários para rastreabilidade e linhagem dos dados processados.

  - Executa a carga do dataframe resultante para a tabela de destino no Datalake.

---

## Tarefas (`tasks.py`)

As principais *tasks* que compõem esse flow incluem:

- **Operações no Cloud Storage:**

  - `get_latest_csv_from_gcs(gcs_folder_uri)`: Acessa o bucket GCS e lista os arquivos disponíveis na pasta informada, filtrando apenas os arquivos com extensão `.csv`.

  - `get_latest_csv_from_gcs(gcs_folder_uri)`: Ordena os arquivos encontrados utilizando a data de criação registrada no GCS (`time_created`) e retorna a URI do arquivo mais recente, permitindo que o pipeline selecione automaticamente o relatório mais atual.

- **Download e Processamento de Dados:**

  - `download_file_from_bucket_task(...)`: Realiza o download do arquivo selecionado no GCS para o ambiente temporário do worker, disponibilizando o relatório para processamento local.

  - Executa a leitura do arquivo CSV utilizando Pandas e prepara os dados para as etapas de tratamento e carga.

- **Tratamento e Padronização dos Dados:**

  - `cleanup_columns_for_bigquery(...)`: Realiza a limpeza e padronização dos nomes das colunas, removendo acentos, espaços e caracteres especiais para adequação ao padrão de nomenclatura aceito pelo BigQuery.

  - Adiciona informações de rastreabilidade aos registros processados, permitindo identificar a origem dos dados carregados no Datalake.

- **Carga no DataLake:**

  - `upload_df_to_datalake_task(...)`: Realiza o upload do dataframe processado para a tabela de destino no Datalake, efetuando a carga dos dados tratados para o BigQuery.

---

## Utilitários e Configurações (`constants.py`, `schedules.py` e `utils`)

Os módulos auxiliares concentram as configurações de ambiente, agendamento e funções reutilizáveis utilizadas durante o processamento dos relatórios.

- **Configurações e Parametrização:**

  - `MEDILAB_CONFIG`: Dicionário localizado em `constants.py` responsável por centralizar as configurações utilizadas pelo pipeline, como `DATASET_ID`, `TABLE_ID` e a URI base de produção.

  - A utilização das constantes evita a definição de valores fixos (*hardcoded*) diretamente no código dos flows e tasks, facilitando a manutenção e configuração do pipeline.

- **Agendamento e Orquestração:**

  - `schedules.py`: Utiliza as configurações definidas em `constants.py` para estruturar os gatilhos e agendamentos responsáveis pela execução automatizada do flow no Prefect.

  - Permite que o pipeline seja executado de forma recorrente, mantendo o processamento dos relatórios integrado à rotina de ingestão do Datalake.

- **Utilitários do Repositório (`pipelines.utils`):**

  - `cleanup_columns_for_bigquery(...)`: Centraliza o tratamento dos cabeçalhos dos arquivos, garantindo que os nomes das colunas estejam adequados para utilização no BigQuery.

  - `download_file_from_bucket_task(...)`: Encapsula as operações de download dos arquivos armazenados no GCS para o ambiente temporário de processamento.

  - `upload_df_to_datalake_task(...)`: Encapsula a operação de carga dos dados processados para o Datalake, mantendo a integração com o BigQuery centralizada nos utilitários compartilhados.

---

