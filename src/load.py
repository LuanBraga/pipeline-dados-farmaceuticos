import logging
import pandas as pd
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError
from src import config

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_to_postgres(df: pd.DataFrame):
    """
    Carrega o DataFrame para uma tabela no PostgreSQL.

    A tabela 'medicamentos' será substituída se já existir.
    """
    if df.empty:
        logger.warning("O DataFrame está vazio. Nenhum dado será carregado no PostgreSQL.")
        return

    try:
        logger.info(f"Conectando ao banco de dados PostgreSQL em {config.DB_HOST}...")
        engine = create_engine(config.DATABASE_URL)

        logger.info("Carregando dados na tabela 'medicamentos'...")
        # Usar 'if_exists='replace'' é conveniente para desenvolvimento,
        # mas em produção, uma estratégia de 'append' ou 'upsert' seria mais comum.
        df.to_sql('medicamentos', engine, if_exists='replace', index=False, chunksize=1000)

        logger.info(f"{len(df)} registros carregados com sucesso no PostgreSQL.")

    except Exception as e:
        logger.critical(f"Falha ao carregar dados para o PostgreSQL: {e}", exc_info=True)
        raise

def load_to_elasticsearch(df: pd.DataFrame):
    """
    Carrega o DataFrame para um índice no Elasticsearch.

    O índice será recriado se já existir.
    """
    if df.empty:
        logger.warning("O DataFrame está vazio. Nenhum dado será carregado no Elasticsearch.")
        return

    try:
        logger.info(f"Conectando ao Elasticsearch em {config.ES_URL}...")
        es = Elasticsearch(config.ES_URL)

        if es.indices.exists(index=config.ES_INDEX_NAME):
            logger.warning(f"Índice '{config.ES_INDEX_NAME}' já existe. Deletando para recriação.")
            es.indices.delete(index=config.ES_INDEX_NAME)

        logger.info(f"Criando o índice '{config.ES_INDEX_NAME}' no Elasticsearch.")
        es.indices.create(index=config.ES_INDEX_NAME)

        actions = [
            {
                "_index": config.ES_INDEX_NAME,
                "_source": record
            }
            for record in df.to_dict(orient='records')
        ]

        logger.info(f"Indexando {len(actions)} documentos no Elasticsearch...")
        helpers.bulk(es, actions)
        logger.info("Dados indexados com sucesso no Elasticsearch.")

    # Bloco específico para capturar erros de indexação em massa
    except BulkIndexError as e:
        logger.critical(f"Falha ao indexar {len(e.errors)} documento(s).", exc_info=False)
        # Imprime os detalhes dos 5 primeiros erros para diagnóstico
        for i, error in enumerate(e.errors[:5]):
            logger.error(f"  Erro #{i+1}: {error}")
        raise

    except Exception as e:
        logger.critical(f"Falha ao carregar dados para o Elasticsearch: {e}", exc_info=True)
        raise

def run(df: pd.DataFrame):
    """
    Orquestra a carga de dados para todos os destinos (PostgreSQL e Elasticsearch).
    """
    logger.info("--- Iniciando Etapa de Carga de Dados ---")
    try:
        load_to_postgres(df)
        load_to_elasticsearch(df)
        logger.info("--- Etapa de Carga de Dados Concluída com Sucesso ---")
    except Exception as e:
        logger.error(f"Ocorreu um erro durante a etapa de carga: {e}")
        # Propaga a exceção para que o main.py possa capturá-la
        raise
