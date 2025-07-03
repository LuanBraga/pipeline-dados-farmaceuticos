import os
from dotenv import load_dotenv

load_dotenv()

# REFATORAR os.path PARA pathlib
# from pathlib import Path
#
# # Define a raiz do projeto de forma orientada a objetos
# PROJECT_ROOT = Path(__file__).resolve().parent.parent
#
# # diretório para armazenar os dados brutos
# DATA_DIR = PROJECT_ROOT / "dados_brutos"
#
# # garante que o diretório de dados exista
# DATA_DIR.mkdir(parents=True, exist_ok=True)


# define a raiz do projeto como o diretório pai do diretório 'src'
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# diretorio para armazenar os dados brutos
DATA_DIR = os.path.join(PROJECT_ROOT, "dados_brutos")

# Diretório para armazenar os dados processados e unificados
PROCESSED_DATA_DIR = os.path.join(PROJECT_ROOT, "dados_processados")

# nome do arquivo da ANVISA
ANVISA_FILENAME = "DADOS_ABERTOS_MEDICAMENTOS.csv"

# Nome do arquivo unificado a ser gerado
UNIFIED_FILENAME = "ANVISA_CMED_UNIFICADO.csv"

# URL direta para o arquivo csv da ANVISA
ANVISA_CSV_URL = "https://dados.anvisa.gov.br/dados/DADOS_ABERTOS_MEDICAMENTOS.csv"

# URL base da CMED
CMED_BASE_URL = "https://www.gov.br"
CMED_PRICES_PAGE_URL = f"{CMED_BASE_URL}/anvisa/pt-br/assuntos/medicamentos/cmed/precos"

# configurações do Banco de Dados PostgreSQL
DB_USER = os.getenv("POSTGRES_USER", "admin")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "strongpassword")
DB_NAME = os.getenv("POSTGRES_DB", "medicamentos_db")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

# string de conexão para o SQLAlchemy
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# configurações do Elasticsearch
ES_HOST = os.getenv("ES_HOST", "localhost")
ES_PORT = int(os.getenv("ES_PORT", 9200))
ES_URL = f"http://{ES_HOST}:{ES_PORT}"
ES_INDEX_NAME = "medicamentos"

# garante que o diretório de dados exista
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)