import logging
import os
import pandas as pd
import argparse
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch, helpers
from src import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_manual_data(csv_filename: str, db_identifier: str):
    """
    Orquestra a extração e carga de um ficheiro de dados manual.

    :param csv_filename: Nome do ficheiro CSV localizado em 'dados_manuais/'.
    :param db_identifier: Nome a ser usado para a tabela no PostgreSQL e para o índice no Elasticsearch.
    """
    logger.info(f"--- Iniciando Carga de Dados Manuais: {csv_filename} ---")

    try:
        # 1. Extração (Leitura do CSV)
        file_path = os.path.join(config.MANUAL_DATA_DIR, csv_filename)
        logger.info(f"Lendo dados de: {file_path}")
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}. Verifique o nome e o diretório.")

        df = pd.read_csv(file_path)
        logger.info(f"{len(df)} registros lidos de '{csv_filename}'.")

        if df.empty:
            logger.warning(f"O DataFrame de '{csv_filename}' está vazio. Nenhuma carga será realizada.")
            return

        # 2. Carga para o PostgreSQL
        _load_to_postgres(df, db_identifier)

        # 3. Carga para o Elasticsearch
        _load_to_elasticsearch(df, db_identifier)

        logger.info(f"--- Carga de '{csv_filename}' Concluída com Sucesso ---")

    except Exception as e:
        logger.critical(f"Ocorreu um erro durante a carga de '{csv_filename}': {e}", exc_info=True)
        raise


def _load_to_postgres(df: pd.DataFrame, table_name: str):
    """
    Carrega o DataFrame para uma tabela no PostgreSQL.
    """
    try:
        logger.info(f"Conectando ao banco de dados PostgreSQL...")
        engine = create_engine(config.DATABASE_URL)

        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logger.info(f"{len(df)} registros carregados com sucesso na tabela '{table_name}' do PostgreSQL.")
    except Exception as e:
        logger.critical(f"Falha ao carregar dados para a tabela '{table_name}' do PostgreSQL: {e}", exc_info=True)
        raise


def _load_to_elasticsearch(df: pd.DataFrame, index_name: str):
    """
    Carrega o DataFrame para um índice no Elasticsearch.
    """
    try:
        logger.info(f"Conectando ao Elasticsearch...")
        es = Elasticsearch(config.ES_URL)

        if es.indices.exists(index=index_name):
            logger.warning(f"Índice '{index_name}' já existe. Deletando para recriação.")
            es.indices.delete(index=index_name)

        es.indices.create(index=index_name)

        actions = [
            {"_index": index_name, "_source": record}
            for record in df.to_dict(orient='records')
        ]

        logger.info(f"Indexando {len(actions)} documentos no índice '{index_name}'...")
        helpers.bulk(es, actions)
        logger.info(f"Dados indexados com sucesso no índice '{index_name}'.")
    except Exception as e:
        logger.critical(f"Falha ao carregar dados para o índice '{index_name}' do Elasticsearch: {e}", exc_info=True)
        raise


def main():
    """
    Função principal para analisar os argumentos da linha de comando e iniciar o processo de carga.
    """
    parser = argparse.ArgumentParser(description="Carregador de dados manuais para PostgreSQL e Elasticsearch.")

    parser.add_argument("filename", type=str,
                        help="Nome do arquivo CSV a ser carregado (deve estar no diretório 'dados_manuais').")
    parser.add_argument("--table-name", type=str,
                        help="Nome da tabela de destino no DB e do índice no ES. Se omitido, será derivado do nome do arquivo.")

    args = parser.parse_args()

    # Deriva o nome da tabela/índice a partir do nome do ficheiro se não for fornecido
    db_identifier = args.table_name
    if not db_identifier:
        # Remove a extensão .csv para criar um nome limpo
        db_identifier = os.path.splitext(args.filename)[0]

    load_manual_data(args.filename, db_identifier)


if __name__ == '__main__':
    main()