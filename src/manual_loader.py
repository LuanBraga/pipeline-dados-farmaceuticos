import logging
import os
import pandas as pd
import argparse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import String, Boolean, Integer, Numeric
from elasticsearch import Elasticsearch, helpers
from src import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_manual_data(csv_filename: str, db_identifier: str):
    logger.info(f"--- Iniciando Carga de Dados Manuais: {csv_filename} ---")

    try:
        file_path = os.path.join(config.MANUAL_DATA_DIR, csv_filename)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}.")

        df = pd.read_csv(file_path)
        if df.empty:
            logger.warning("DataFrame vazio. Nenhuma carga será realizada.")
            return

        # Garante que os nomes das colunas no DataFrame correspondam ao arquivo
        df.columns = [col.upper() for col in df.columns]

        _load_to_postgres(df, db_identifier)
        _load_to_elasticsearch(df, db_identifier)

        logger.info(f"--- Carga de '{csv_filename}' Concluída com Sucesso ---")

    except Exception as e:
        logger.critical(f"Erro na carga de '{csv_filename}': {e}", exc_info=True)
        raise


def _load_to_postgres(df: pd.DataFrame, table_name: str):
    # Mapeamento com nomes de colunas em maiúsculas
    dtype_mapping = {
        'ID': Integer,
        'UF': String(2),
        'ESTADO': String(50),
        'ALIQUOTA': Numeric(5, 2),
        'GENERICO': Boolean
    }
    try:
        logger.info("Conectando ao PostgreSQL...")
        engine = create_engine(config.DATABASE_URL)

        # O SQLAlchemy por padrão converte nomes de colunas para minúsculas.
        # Carregamos os dados e depois renomeamos as colunas na tabela.
        df.to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype_mapping)

        with engine.connect() as connection:
            logger.info(f"Definindo 'ID' como chave primária na tabela '{table_name}'.")
            # Usa aspas para preservar a caixa alta no nome da coluna
            connection.execute(text(f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ("ID");'))
            connection.commit()

        logger.info(f"{len(df)} registros carregados com sucesso em '{table_name}'.")

    except SQLAlchemyError as e:
        logger.critical(f"Falha ao carregar dados para '{table_name}': {e}", exc_info=True)
        raise


def _load_to_elasticsearch(df: pd.DataFrame, index_name: str):
    # Mapeamento com nomes de campos em maiúsculas
    es_mapping = {
        "properties": {
            "ID": {"type": "integer"},
            "UF": {"type": "keyword"},
            "ESTADO": {"type": "text", "analyzer": "brazilian"},
            "ALIQUOTA": {"type": "scaled_float", "scaling_factor": 100},
            "GENERICO": {"type": "boolean"}
        }
    }
    try:
        logger.info("Conectando ao Elasticsearch...")
        es = Elasticsearch(config.ES_URL)

        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)

        es.indices.create(index=index_name, mappings=es_mapping)

        actions = [
            # Usa os nomes em maiúsculas ao criar os documentos
            {"_index": index_name, "_id": record["ID"], "_source": record}
            for record in df.to_dict(orient='records')
        ]

        logger.info(f"Indexando {len(actions)} documentos em '{index_name}'...")
        helpers.bulk(es, actions)
        logger.info(f"Dados indexados com sucesso em '{index_name}'.")

    except Exception as e:
        logger.critical(f"Falha ao carregar dados para '{index_name}': {e}", exc_info=True)
        raise


def main():
    parser = argparse.ArgumentParser(description="Carregador de dados manuais para PostgreSQL e Elasticsearch.")
    parser.add_argument("filename", type=str, help="Nome do arquivo CSV em 'dados_manuais'.")
    parser.add_argument("--table-name", type=str, help="Nome da tabela/índice de destino.")
    args = parser.parse_args()

    db_identifier = args.table_name or os.path.splitext(args.filename)[0]
    load_manual_data(args.filename, db_identifier)


if __name__ == '__main__':
    main()