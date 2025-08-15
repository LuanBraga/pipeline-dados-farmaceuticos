# Pipeline de Dados Farmacêuticos

## Visão Geral

O **Pipeline de Dados Farmacêuticos** é uma solução de ETL (Extração, Transformação e Carga) automatizada e containerizada, projetada para extrair, limpar, padronizar e combinar informações de fontes farmacêuticas oficiais do Brasil: a Agência Nacional de Vigilância Sanitária (ANVISA) e a Câmara de Regulação do Mercado de Medicamentos (CMED).

Após o processamento, os dados unificados são armazenados em um banco de dados PostgreSQL para consultas estruturadas e indexados em um motor de busca Elasticsearch para otimizar buscas textuais complexas e rápidas.

## Funcionalidades

-   **Extração de Dados**: Coleta automática de dados da ANVISA e CMED.
-   **Limpeza e Padronização**: Normalização de formatos, remoção de duplicatas e correção de inconsistências.
-   **Unificação de Dados**: Combinação dos dados das diferentes fontes em um dataset integrado.
-   **Armazenamento Otimizado**: Dados estruturados armazenados em PostgreSQL e indexados em Elasticsearch.
-   **Containerização**: Implementação com Docker para garantir consistência, portabilidade e facilidade de deploy.

## Arquitetura e Fluxo de Dados

O pipeline segue um padrão ETL clássico, dividido em três etapas principais:

1.  **Extração (Extract)**: O módulo `extract.py` baixa os dados brutos da ANVISA (CSV) e da CMED (XLS/XLSX) e os armazena no diretório `dados_brutos/`.
2.  **Transformação (Transform)**: O módulo `transform.py` utiliza a biblioteca `pandas` para carregar os dados brutos, limpá-los, padronizá-los e unificá-los. O resultado é um único arquivo CSV (`ANVISA_CMED_UNIFICADO.csv`) salvo no diretório `dados_processados/`.
3.  **Carga (Load)**: O módulo `load.py` (atualmente em desenvolvimento) será responsável por carregar os dados processados no PostgreSQL e no Elasticsearch.

O fluxo de dados pode ser resumido da seguinte forma:

```
Fontes Externas (ANVISA, CMED)
        |
        v
Extração (extract.py)
        |
        v
Diretório dados_brutos/
        |
        v
Transformação (transform.py)
        |
        v
Diretório dados_processados/
        |
        v
Carga (load.py)
        |
        v
Sistemas de Destino (PostgreSQL, Elasticsearch)
```

## Pré-requisitos

-   Python 3.9+
-   Git
-   Docker
-   Docker Compose

## Configuração e Instalação

### 1. Clone o Repositório

```bash
git clone https://github.com/seu-usuario/pipeline-dados-farmaceuticos.git
cd pipeline-dados-farmaceuticos
```

### 2. Configure as Variáveis de Ambiente

Copie o arquivo de exemplo `.env.example` para um novo arquivo chamado `.env` e preencha com suas credenciais e configurações específicas.

```bash
cp .env.example .env
```

### 3. Execução com Docker (Recomendado)

Para construir as imagens e iniciar os containers, execute:

```bash
docker-compose up --build
```

Para parar os containers, execute:

```bash
docker-compose down
```

### 4. Execução Local (Opcional)

#### a. Crie e Ative um Ambiente Virtual

-   **Criação**:
    ```bash
    python -m venv .venv
    ```
-   **Ativação**:
    -   **macOS/Linux**: `source .venv/bin/activate`
    -   **Windows (PowerShell)**: `.\.venv\Scripts\activate`
    -   **Windows (CMD)**: `.\.venv\Scripts\activate`

#### b. Instale as Dependências

```bash
pip install -r requirements.txt
```

#### c. Execute o Pipeline

```bash
python -m src.main
```

*Nota: Para a execução local, certifique-se de que o PostgreSQL e o Elasticsearch estejam instalados e acessíveis em sua máquina.*

## Estrutura do Projeto

```
.
├── dados_brutos/         # Armazena os dados brutos extraídos
├── dados_processados/    # Armazena os dados processados e unificados
├── src/                  # Código-fonte do pipeline
│   ├── config.py         # Configurações (URLs, paths, credenciais)
│   ├── extract.py        # Módulo de extração de dados
│   ├── transform.py      # Módulo de transformação e limpeza
│   ├── load.py           # Módulo de carga de dados (em desenvolvimento)
│   └── main.py           # Ponto de entrada do pipeline
├── .env                  # Variáveis de ambiente (não versionado)
├── .env.example          # Exemplo de variáveis de ambiente
├── .gitignore            # Arquivos e diretórios ignorados pelo Git
├── docker-compose.yml    # Orquestração dos containers Docker
├── Dockerfile            # Definição do container da aplicação Python
├── README.md             # Documentação principal do projeto
└── requirements.txt      # Dependências Python
```

## Como Contribuir

Contribuições são bem-vindas! Se você tiver sugestões de melhorias ou correções de bugs, siga os seguintes passos:

1.  Faça um fork do projeto.
2.  Crie uma nova branch (`git checkout -b feature/sua-feature`).
3.  Faça suas alterações e commite (`git commit -m 'feat: Adiciona sua feature'`).
4.  Faça o push para a sua branch (`git push origin feature/sua-feature`).
5.  Abra um Pull Request.

## Licença

Este projeto está licenciado sob a Licença MIT. Veja o arquivo `LICENSE` para mais detalhes.
