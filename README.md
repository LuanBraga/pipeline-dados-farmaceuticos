# Pipeline de Dados Farmacêuticos

## Visão Geral

O **Pipeline de Dados Farmacêuticos** é uma solução automatizada e containerizada que extrai, limpa, padroniza, e combina informações provenientes das fontes oficiais farmacêuticas brasileiras ANVISA e CMED. Após a unificação, os dados são armazenados em um banco de dados relacional e indexados em um motor de busca para otimizar consultas complexas e rápidas.

---

## Funcionalidades Principais

- **Extração de Dados**: Obtenção automática de informações da ANVISA e CMED.
- **Limpeza e Padronização**: Normalização de formatos, remoção de duplicatas e correção de inconsistências.
- **Unificação de Dados**: Combinação dos dados das diferentes fontes em um dataset integrado.
- **Armazenamento Otimizado**: Dados estruturados armazenados em PostgreSQL e indexados em Elasticsearch.
- **Containerização**: Implementação com Docker para garantir consistência e portabilidade.

---

## Pré-requisitos

Certifique-se de possuir instaladas:

- Python 3.9+
- Git
- Docker
- Docker Compose

---

## Configuração e Instalação

### 1. Clone o Repositório

```bash
git clone https://github.com/seu-usuario/pipeline-dados-farmaceuticos.git
cd pipeline-dados-farmaceuticos
```

### 2. Configure Variáveis de Ambiente

Copie e edite o arquivo de exemplo:

```bash
cp .env.example .env
```

Preencha o arquivo `.env` com suas credenciais e configurações específicas.

### 3. Execução com Docker (Recomendado)

Execute para construir imagens e subir os containers:

```bash
docker-compose up --build
```

Para parar os containers:

```bash
docker-compose down
```

### 4. Configuração Local (Opcional)

#### 4.1 Ambiente Virtual Python

Crie e ative um ambiente virtual:

- **Criação**:

```bash
python3 -m venv .venv
```

- **Ativação**:

**macOS/Linux:**

```bash
source .venv/bin/activate
```

**Windows (PowerShell):**

```powershell
.\.venv\Scripts\activate
```

**Windows (CMD):**

```cmd
.\.venv\Scripts\activate
```

#### 4.2 Instale Dependências

```bash
pip install -r requirements.txt
```

#### 4.3 Execute o Pipeline Localmente

```bash
python -m src.main
```


*Nota:* Certifique-se de que PostgreSQL e Elasticsearch estejam acessíveis localmente.


#### Extra: Gerando o Arquivo requirements.txt

```bash
pip freeze > requirements.txt
```

---

## Estrutura do Projeto

```
.
├── dados_brutos/         # Dados brutos e processados
├── src/                  # Código fonte
│   ├── config.py         # Configuração
│   ├── extract.py        # Extração
│   ├── transform.py      # Limpeza e transformação
│   ├── load.py           # Carga em banco de dados e busca
│   └── main.py           # Entrada do pipeline
├── .env                  # Variáveis de ambiente (não versionado)
├── .env.example          # Exemplo de variáveis
├── .gitignore            # Ignorados pelo Git
├── docker-compose.yml    # Orquestração Docker
├── Dockerfile            # Container Python
├── README.md             # Documentação principal
└── requirements.txt      # Dependências Python
```