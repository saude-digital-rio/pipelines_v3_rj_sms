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
  - `get_latest_csv_from_gcs(gcs_folder_uri)`: Acessa o bucket GCS, lista os arquivos disponíveis na pasta informada e retorna a URI do arquivo CSV mais recente (baseado na data de criação `time_created`), permitindo que o pipeline selecione automaticamente o relatório mais atual.

- **Download e Processamento de Dados:**
  - `download_file_from_bucket_task(...)`: Realiza o download do arquivo selecionado no GCS para o ambiente temporário do worker, disponibilizando o relatório para processamento local utilizando Pandas.

- **Tratamento, Padronização e Linhagem dos Dados:**
  - `cleanup_columns_for_bigquery(...)`: Realiza a limpeza e padronização dos nomes das colunas, removendo acentos, espaços e caracteres especiais para adequação ao padrão do BigQuery.
  - **Adição de Metadados:** Injeta colunas essenciais para rastreabilidade e particionamento no dbt, incluindo `arquivo_origem` (URI do GCS), `data_carga` (momento exato da extração) e `periodo_referencia` (fixado no dia 01 do mês da extração para agregar a competência dos exames).

- **Carga no DataLake:**
  - `upload_df_to_datalake_task(...)`: Realiza o upload do dataframe processado para a tabela de destino na camada staging do Datalake no BigQuery.

---

## Utilitários e Configurações (`constants.py`, `schedules.py` e `utils`)

Os módulos auxiliares concentram as configurações de ambiente, agendamento e funções reutilizáveis utilizadas durante o processamento dos relatórios.

- **Configurações e Parametrização:**
  - `MEDILAB_CONFIG`: Dicionário localizado em `constants.py` responsável por centralizar as configurações utilizadas pelo pipeline, como `DATASET_ID`, `TABLE_ID` e `GCS_URI`. Evita a definição de valores fixos (*hardcoded*) no código.

- **Agendamento e Orquestração:**
  - `schedules.py`: Estrutura os gatilhos e agendamentos responsáveis pela execução automatizada do flow no Prefect, mantendo o processamento integrado à rotina do Datalake.

- **Utilitários do Repositório (`pipelines.utils`):**
  - **Padrão de Arquitetura:** As lógicas pesadas de extração e manipulação de arquivos no Cloud Storage estão isoladas como funções puras em `pipelines.utils.google`. As *tasks* locais funcionam apenas como *wrappers* (cascas) explicativas para o Prefect, mantendo o fluxo modular e limpo.

---

