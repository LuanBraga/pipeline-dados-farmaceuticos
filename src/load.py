import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import (
    String, Text, BigInteger, Numeric, Boolean
)
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError
from src import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_to_postgres(df: pd.DataFrame):
    if df.empty:
        logger.warning("O DataFrame está vazio. Nenhum dado será carregado no PostgreSQL.")
        return


    dtype_mapping = {
        "NUMERO_REGISTRO_PRODUTO": String(13),
        "CLASSE_TERAPEUTICA": Text,
        "PRINCIPIO_ATIVO": Text,
        "LABORATORIO": Text,
        "CNPJ": String(14),
        "REGISTRO_CMED": String(20),
        "PRODUTO": Text,
        "APRESENTACAO": Text,
        "TIPO_PRODUTO": String(50),
        "TARJA": String(50),
        "CODIGO_GGREM": BigInteger,
        "REGIME_DE_PRECO": String(50),
        "RESTRICAO_HOSPITALAR": Boolean,
        "LISTA_DE_CONCESSAO_DE_CREDITO_TRIBUTARIO_PIS_COFINS": String(10),
    }

    # Mapeia todas as colunas de preço para Numeric para precisão monetária
    price_cols = [col for col in df.columns if col.startswith('PRECO_MAXIMO_AO_CONSUMIDOR')]
    for col in price_cols:
        dtype_mapping[col] = Numeric(10, 2)

    try:
        logger.info(f"Conectando ao banco de dados PostgreSQL em {config.DB_HOST}...")
        engine = create_engine(config.DATABASE_URL)

        # Converte colunas booleanas e de string para os formatos corretos
        if 'RESTRICAO_HOSPITALAR' in df.columns:
            df['RESTRICAO_HOSPITALAR'] = df['RESTRICAO_HOSPITALAR'].apply(lambda x: True if x == 'Sim' else False)

        df.to_sql(
            config.DB_TABLE_NAME,
            engine,
            if_exists='replace',
            index=False,
            dtype=dtype_mapping,
            chunksize=1000
        )

        with engine.connect() as connection:
            logger.info(f"Definindo 'REGISTRO_CMED' como chave primária.")
            connection.execute(text(f'ALTER TABLE "{config.DB_TABLE_NAME}" ADD PRIMARY KEY ("REGISTRO_CMED");'))
            connection.commit()

        logger.info(f"{len(df)} registros carregados com sucesso no PostgreSQL.")

    except SQLAlchemyError as e:
        logger.critical(f"Falha ao carregar dados para o PostgreSQL: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.critical(f"Ocorreu um erro inesperado no PostgreSQL: {e}", exc_info=True)
        raise


def load_to_elasticsearch(df: pd.DataFrame):
    if df.empty:
        logger.warning("O DataFrame está vazio. Nenhum dado será carregado no Elasticsearch.")
        return

    # Mapeamento explícito para o Elasticsearch
    es_mapping = {
        "properties": {
            "NUMERO_REGISTRO_PRODUTO": {"type": "keyword"},
            "CLASSE_TERAPEUTICA": {"type": "text", "analyzer": "brazilian"},
            "PRINCIPIO_ATIVO": {"type": "text", "analyzer": "brazilian"},
            "LABORATORIO": {"type": "text", "analyzer": "brazilian"},
            "CNPJ": {"type": "keyword"},
            "REGISTRO_CMED": {"type": "keyword"},
            "PRODUTO": {"type": "text", "analyzer": "brazilian"},
            "APRESENTACAO": {"type": "text", "analyzer": "brazilian"},
            "TIPO_PRODUTO": {"type": "keyword"},
            "TARJA": {"type": "keyword"},
            "CODIGO_GGREM": {"type": "long"},
            "REGIME_DE_PRECO": {"type": "keyword"},
            "RESTRICAO_HOSPITALAR": {"type": "boolean"},
            "LISTA_DE_CONCESSAO_DE_CREDITO_TRIBUTARIO_PIS_COFINS": {"type": "keyword"},
        }
    }

    price_cols = [col for col in df.columns if col.startswith('PRECO_MAXIMO_AO_CONSUMIDOR')]
    for col in price_cols:
        es_mapping["properties"][col] = {"type": "scaled_float", "scaling_factor": 100}

    try:
        logger.info(f"Conectando ao Elasticsearch em {config.ES_URL}...")
        es = Elasticsearch(config.ES_URL)

        if es.indices.exists(index=config.ES_INDEX_NAME):
            es.indices.delete(index=config.ES_INDEX_NAME)

        logger.info(f"Criando o índice '{config.ES_INDEX_NAME}' com mapeamento explícito.")
        es.indices.create(index=config.ES_INDEX_NAME, mappings=es_mapping)

        actions = [
            {
                "_index": config.ES_INDEX_NAME,
                "_id": record["REGISTRO_CMED"],
                "_source": record
            }
            for record in df.to_dict(orient='records')
        ]

        logger.info(f"Indexando {len(actions)} documentos...")
        helpers.bulk(es, actions)
        logger.info("Dados indexados com sucesso no Elasticsearch.")

    except BulkIndexError as e:
        logger.critical(f"Falha ao indexar {len(e.errors)} documento(s).", exc_info=False)
        for i, error in enumerate(e.errors[:5]):
            logger.error(f"  Erro #{i+1}: {error}")
        raise
    except Exception as e:
        logger.critical(f"Falha ao carregar dados para o Elasticsearch: {e}", exc_info=True)
        raise


def run(df: pd.DataFrame):
    logger.info("--- Iniciando Etapa de Carga de Dados ---")
    try:
        load_to_postgres(df)
        load_to_elasticsearch(df)
        logger.info("--- Etapa de Carga de Dados Concluída com Sucesso ---")
    except Exception as e:
        logger.critical(f"Ocorreu um erro durante a etapa de carga: {e}", exc_info=True)
        raise