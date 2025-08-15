# Documentação do Código: Pipeline de Dados Farmacêuticos

## Visão Geral

Este documento fornece uma descrição detalhada da arquitetura e do funcionamento do projeto **Pipeline de Dados Farmacêuticos**. O objetivo do projeto é automatizar o processo de extração, transformação e carga (ETL) de dados de medicamentos de duas fontes oficiais brasileiras: a Agência Nacional de Vigilância Sanitária (ANVISA) e a Câmara de Regulação do Mercado de Medicamentos (CMED).

O pipeline é construído em Python e foi projetado para ser executado em um ambiente containerizado com Docker, garantindo portabilidade e consistência.

## Arquitetura

O pipeline segue um padrão ETL clássico, dividido em três etapas principais, orquestradas por um script principal.

1.  **Extração (Extract)**: Coleta os dados brutos das fontes.
2.  **Transformação (Transform)**: Limpa, padroniza e unifica os dados.
3.  **Carga (Load)**: Armazena os dados processados em sistemas de destino (neste caso, PostgreSQL e Elasticsearch).

### Estrutura de Módulos

O código-fonte está organizado no diretório `src/` e dividido nos seguintes módulos:

-   `main.py`: O ponto de entrada do pipeline, responsável por orquestrar a execução das etapas.
-   `config.py`: Centraliza todas as configurações, como URLs, caminhos de arquivos e credenciais de banco de dados.
-   `extract.py`: Contém a lógica para baixar os arquivos de dados da ANVISA e da CMED.
-   `transform.py`: Responsável pela limpeza, padronização e unificação dos dados.
-   `load.py`: Contém a lógica para carregar os dados transformados no PostgreSQL e no Elasticsearch.

---

## Detalhamento dos Módulos

### `config.py`

Este módulo utiliza a biblioteca `python-dotenv` para carregar variáveis de ambiente de um arquivo `.env`, permitindo uma configuração segura e flexível. As principais configurações incluem:

-   **Caminhos de Diretórios**: Define os locais para `dados_brutos` e `dados_processados`.
-   **URLs das Fontes**: Armazena os links para os dados da ANVISA e da CMED.
-   **Nomes de Arquivos**: Especifica os nomes dos arquivos de entrada e saída.
-   **Credenciais de Banco de Dados**: Configurações de conexão para o PostgreSQL.
-   **Configurações do Elasticsearch**: Endereço e nome do índice para o Elasticsearch.

### `extract.py`

A etapa de extração é responsável por obter os dados mais recentes das fontes.

-   **Extração ANVISA**:
    -   Baixa um arquivo CSV diretamente de uma URL estática fornecida pela ANVISA.
    -   Salva o arquivo no diretório `dados_brutos`.

-   **Extração CMED**:
    -   Realiza web scraping na página de preços da CMED para encontrar a URL do arquivo de preços mais recente (um arquivo XLS ou XLSX).
    -   Verifica se o arquivo já existe localmente para evitar downloads desnecessários.
    -   Caso uma nova versão seja encontrada, remove arquivos antigos e baixa o novo.
    -   Salva o arquivo no diretório `dados_brutos`.

O módulo inclui tratamento de erros para falhas de rede e interrupções no pipeline caso os arquivos essenciais não possam ser baixados.

### `transform.py`

Este é o coração do pipeline, onde os dados brutos são processados e unificados. A lógica é implementada com a biblioteca `pandas`.

1.  **Carregamento**: Os arquivos CSV (ANVISA) e Excel (CMED) são carregados em DataFrames do pandas.
2.  **Limpeza de Dados da ANVISA**:
    -   Seleciona as colunas relevantes.
    -   Padroniza o `NUMERO_REGISTRO_PRODUTO`, removendo caracteres não numéricos e ajustando o comprimento.
    -   Remove espaços em branco extras.
3.  **Limpeza de Dados da CMED**:
    -   Renomeia as colunas para nomes padronizados e mais claros.
    -   Converte as colunas de preço para o formato numérico, removendo caracteres especiais.
    -   Remove linhas que não contêm nenhuma informação de preço.
    -   Cria uma coluna `REGISTRO_BASE` (com os 9 primeiros dígitos do registro) para servir como chave de unificação.
4.  **Unificação (Merge)**:
    -   Os dois DataFrames (ANVISA e CMED) são unificados usando um `inner join`. A junção é feita entre o `NUMERO_REGISTRO_PRODUTO` da ANVISA e o `REGISTRO_BASE` da CMED.
    -   Isso garante que o dataset final contenha apenas os medicamentos presentes em ambas as fontes.
5.  **Salvamento**:
    -   O DataFrame unificado é salvo como um arquivo CSV (`ANVISA_CMED_UNIFICADO.csv`) no diretório `dados_processados`.

### `load.py`

Este módulo é responsável por carregar os dados processados nos sistemas de armazenamento.

-   **Carregamento no PostgreSQL**: A intenção é usar o SQLAlchemy para criar uma tabela e inserir os dados do DataFrame unificado, permitindo consultas SQL estruturadas.
-   **Indexação no Elasticsearch**: O objetivo é enviar os dados para um índice do Elasticsearch, o que possibilita buscas textuais rápidas e complexas.

**Observação**: Atualmente, o arquivo `load.py` está vazio. A funcionalidade de carga ainda precisa ser implementada.

### `main.py`

O orquestrador do pipeline. Suas responsabilidades são:

-   **Execução Sequencial**: Chama as funções `run()` dos módulos `extract`, `transform` e `load` na ordem correta.
-   **Logging**: Configura e utiliza o `logging` para registrar informações, avisos e erros de cada etapa do processo.
-   **Monitoramento**: Mede e informa o tempo total de execução do pipeline.

---

## Fluxo de Dados

O fluxo de dados pode ser resumido da seguinte forma:

1.  **Fontes Externas** (ANVISA, CMED)
    - Os dados são baixados via HTTP (`extract.py`).
2.  **Diretório `dados_brutos`**
    - Arquivos CSV e XLS brutos são armazenados temporariamente.
3.  **Processamento em Memória** (`transform.py`)
    - Os dados são carregados, limpos e unificados com o `pandas`.
4.  **Diretório `dados_processados`**
    - O dataset unificado é salvo como um arquivo CSV.
5.  **Sistemas de Destino** (`load.py`)
    - O CSV processado é carregado no PostgreSQL e no Elasticsearch.

Este design modular e bem definido torna o pipeline fácil de entender, manter e estender.