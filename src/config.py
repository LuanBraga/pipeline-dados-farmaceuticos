import os
from dotenv import load_dotenv

load_dotenv()

# diretorio para armazenar os dados brutos
DATA_DIR = "dados_brutos"

# nome do arquivo da ANVISA
ANVISA_FILENAME = "DADOS_ABERTOS_MEDICAMENTOS.csv"

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