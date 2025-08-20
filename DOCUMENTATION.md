# Documentação do Código: Pipeline de Dados Farmacêuticos

## Visão Geral

Este documento fornece uma descrição detalhada da arquitetura e do funcionamento do projeto **Pipeline de Dados Farmacêuticos**. O objetivo do projeto é automatizar o processo de extração, transformação e carga (ETL) de dados de medicamentos de duas fontes oficiais brasileiras: a Agência Nacional de Vigilância Sanitária (ANVISA) e a Câmara de Regulação do Mercado de Medicamentos (CMED).

O pipeline é construído em Python e foi projetado para ser executado num ambiente contentorizado com Docker, garantindo portabilidade e consistência.

## Arquitetura

O pipeline segue um padrão ETL clássico, dividido em três etapas principais, orquestradas por um script principal.

1.  **Extração (Extract)**: Coleta os dados brutos das fontes.
2.  **Transformação (Transform)**: Limpa, padroniza e unifica os dados.
3.  **Carga (Load)**: Armazena os dados processados nos sistemas de destino (PostgreSQL e Elasticsearch) através de uma estratégia que evita a indisponibilidade.

### Estrutura de Módulos

O código-fonte está organizado no diretório `src/` e dividido nos seguintes módulos:

-   `main.py`: O ponto de entrada do pipeline, responsável por orquestrar a execução das etapas.
-   `config.py`: Centraliza todas as configurações, como URLs, caminhos de ficheiros e credenciais de banco de dados.
-   `extract.py`: Contém a lógica para baixar os ficheiros de dados da ANVISA e da CMED.
-   `transform.py`: Responsável pela limpeza, padronização e unificação dos dados.
-   `load.py`: Contém a lógica para carregar os dados transformados no PostgreSQL e no Elasticsearch.
-   `manual_loader.py`: Script autónomo para carregar dados manuais (ex: tabelas de referência) para os sistemas de destino.

---

## Detalhamento dos Módulos

### `config.py`

Este módulo utiliza a biblioteca `python-dotenv` para carregar variáveis de ambiente de um ficheiro `.env`, permitindo uma configuração segura e flexível. As principais configurações incluem:

-   **Caminhos de Diretórios**: Define os locais para `dados_brutos`, `dados_processados` e `dados_manuais`.
-   **URLs das Fontes**: Armazena os links para os dados da ANVISA e da CMED.
-   **Nomes de Ficheiros**: Especifica os nomes dos ficheiros de entrada e saída.
-   **Credenciais de Banco de Dados**: Configurações de conexão para o PostgreSQL.
-   **Configurações do Elasticsearch**: Endereço e nome do índice para o Elasticsearch.

### `extract.py`

A etapa de extração é responsável por obter os dados mais recentes das fontes.

-   **Extração ANVISA**:
    -   Baixa um ficheiro CSV diretamente de uma URL estática fornecida pela ANVISA.
    -   Salva o ficheiro no diretório `dados_brutos`.

-   **Extração CMED**:
    -   Realiza web scraping na página de preços da CMED para encontrar a URL do ficheiro de preços mais recente (um ficheiro XLS ou XLSX).
    -   Verifica se o ficheiro já existe localmente para evitar downloads desnecessários.
    -   Caso uma nova versão seja encontrada, remove ficheiros antigos e baixa o novo.
    -   Salva o ficheiro no diretório `dados_brutos`.

O módulo inclui tratamento de erros para falhas de rede e interrupções no pipeline caso os ficheiros essenciais não possam ser baixados.

### `transform.py`

Este é o coração do pipeline, onde os dados brutos são processados e unificados. A lógica é implementada com a biblioteca `pandas`.

1.  **Carregamento**: Os ficheiros CSV (ANVISA) e Excel (CMED) são carregados em DataFrames do pandas.
2.  **Limpeza de Dados da ANVISA**:
    -   Seleciona as colunas relevantes.
    -   Padroniza o `NUMERO_REGISTRO_PRODUTO`, removendo caracteres não numéricos e ajustando o comprimento.
    -   Remove espaços em branco extras.
3.  **Limpeza de Dados da CMED**:
    -   Renomeia as colunas para nomes padronizados e mais claros.
    -   Converte as colunas de preço para o formato numérico, removendo caracteres especiais.
    -   Remove linhas que não contêm nenhuma informação de preço.
    -   Cria uma coluna `REGISTRO_BASE` (com os 9 primeiros dígitos do registo) para servir como chave de unificação.
4.  **Unificação (Merge)**:
    -   Os dois DataFrames (ANVISA e CMED) são unificados usando um `inner join`. A junção é feita entre o `NUMERO_REGISTRO_PRODUTO` da ANVISA e o `REGISTRO_BASE` da CMED.
    -   Isso garante que o dataset final contenha apenas os medicamentos presentes em ambas as fontes.
5.  **Salvamento**:
    -   O DataFrame unificado é salvo como um ficheiro CSV (`ANVISA_CMED_UNIFICADO.csv`) no diretório `dados_processados`.

### `load.py`

Este módulo é responsável por carregar os dados processados nos sistemas de armazenamento, utilizando uma estratégia de **atualização sem indisponibilidade** (*zero-downtime*).

-   **Estratégia de Carga (Blue-Green Deployment)**: Para evitar que o banco de dados fique indisponível durante a atualização, o pipeline adota uma abordagem segura:
    1.  Os novos dados são carregados num local secundário (uma tabela temporária no PostgreSQL e um novo índice no Elasticsearch).
    2.  Após a carga ser concluída com sucesso, o sistema de produção é atomicamente redirecionado para os novos dados.
    3.  Os dados antigos são removidos.

-   **Carregamento no PostgreSQL**:
    1.  Os dados são carregados num *schema* temporário (ex: `medicamentos_temp_12345`).
    2.  Uma transação atómica é iniciada para:
        a. Remover a chave primária da tabela principal para evitar conflitos.
        b. Truncar a tabela principal (`medicamentos`).
        c. Inserir os dados da tabela temporária na principal.
        d. Recriar a chave primária.
    3.  A tabela temporária é removida.
    - Este processo garante que a tabela principal esteja sempre disponível e consistente.

-   **Indexação no Elasticsearch**:
    1.  Os dados são indexados num novo índice com um nome baseado em timestamp (ex: `medicamentos-12345`).
    2.  Um **alias** (ex: `medicamentos`), que é o ponteiro público usado pelas aplicações, é atualizado de forma atómica para apontar para o novo índice.
    3.  O índice antigo, que já não está a ser usado, é removido.
    - Esta abordagem permite que as pesquisas continuem a funcionar sem interrupção durante todo o processo.

### `main.py`

O orquestrador do pipeline. As suas responsabilidades são:

-   **Execução Sequencial**: Chama as funções `run()` dos módulos `extract`, `transform` e `load` na ordem correta.
-   **Logging**: Configura e utiliza o `logging` para registar informações, avisos e erros de cada etapa do processo.
-   **Monitorização**: Mede e informa o tempo total de execução do pipeline.

### `manual_loader.py`

Este script é uma ferramenta de linha de comando autónoma, projetada para carregar dados de ficheiros CSV do diretório `dados_manuais/` para o PostgreSQL e o Elasticsearch.

-   **Função**: Serve para popular o banco de dados com dados de referência ou informações que não fazem parte do fluxo principal de ETL (por exemplo, tabelas de alíquotas de impostos, mapeamentos personalizados, etc.).
-   **Argumentos da Linha de Comando**:
    -   `filename`: O nome do ficheiro CSV a ser carregado (obrigatório).
    -   `--table-name`: O nome da tabela/índice de destino (opcional; se omitido, é derivado do nome do ficheiro).
-   **Operação**:
    -   Lê o ficheiro CSV especificado.
    -   Utiliza a **mesma estratégia de carga segura (blue-green)** do módulo `load.py` para garantir que a inserção dos dados manuais também ocorra sem qualquer indisponibilidade dos sistemas de destino.

---

## Fluxo de Dados

O fluxo de dados pode ser resumido da seguinte forma:

1.  **Fontes Externas** (ANVISA, CMED)
    - Os dados são baixados via HTTP (`extract.py`).
2.  **Diretório `dados_brutos`**
    - Ficheiros CSV e XLS brutos são armazenados temporariamente.
3.  **Processamento em Memória** (`transform.py`)
    - Os dados são carregados, limpos e unificados com o `pandas`.
4.  **Diretório `dados_processados`**
    - O dataset unificado é salvo como um ficheiro CSV.
5.  **Sistemas de Destino** (`load.py`)
    - O CSV processado é carregado no PostgreSQL e no Elasticsearch com uma estratégia de zero downtime.

Este design modular e bem definido torna o pipeline fácil de entender, manter e estender.