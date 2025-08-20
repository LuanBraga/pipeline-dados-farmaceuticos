import logging
import os
import pandas as pd
import argparse
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import String, Boolean, Integer, Numeric
from elasticsearch import Elasticsearch, helpers
from src import config

# Configuração do sistema de logging.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_manual_data(csv_filename: str, db_identifier: str):
    """
    Orquestra a carga de um arquivo CSV manual para o PostgreSQL e Elasticsearch.
    """
    logger.info(f"--- Iniciando Carga de Dados Manuais: {csv_filename} ---")

    try:
        file_path = os.path.join(config.MANUAL_DATA_DIR, csv_filename)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}.")

        df = pd.read_csv(file_path)
        if df.empty:
            logger.warning("DataFrame vazio. Nenhuma carga será realizada.")
            return

        # Garante que os nomes das colunas estejam em maiúsculas para consistência.
        df.columns = [col.upper() for col in df.columns]

        _load_to_postgres(df, db_identifier)
        _load_to_elasticsearch(df, db_identifier)

        logger.info(f"--- Carga de '{csv_filename}' Concluída com Sucesso ---")

    except Exception as e:
        logger.critical(f"Erro na carga de '{csv_filename}': {e}", exc_info=True)
        raise


def _load_to_postgres(df: pd.DataFrame, table_name: str):
    """
    Carrega dados de um DataFrame para o PostgreSQL usando a estratégia
    de tabela temporária para garantir uma atualização sem indisponibilidade.
    """
    # Mapeamento de tipos para a tabela de alíquotas de ICMS.
    dtype_mapping = {
        'ID': Integer,
        'UF': String(2),
        'ESTADO': String(50),
        'ALIQUOTA': Numeric(5, 2),
        'GENERICO': Boolean
    }
    temp_table_name = f"{table_name}_temp_{int(time.time())}"

    try:
        logger.info("Conectando ao PostgreSQL...")
        engine = create_engine(config.DATABASE_URL)

        logger.info(f"Carregando dados na tabela temporária: {temp_table_name}")
        df.to_sql(temp_table_name, engine, if_exists='replace', index=False, dtype=dtype_mapping)

        # Inicia transação atómica para substituir os dados.
        with engine.begin() as connection:
            logger.info("Iniciando transação para substituir a tabela principal.")
            # 1. Garante a existência da tabela principal.
            connection.execute(text(f'CREATE TABLE IF NOT EXISTS "{table_name}" (LIKE "{temp_table_name}")'))

            # 2. Remove a chave primária antiga para permitir a recriação.
            connection.execute(text(f'ALTER TABLE "{table_name}" DROP CONSTRAINT IF EXISTS "{table_name}_pkey"'))

            # 3. Limpa a tabela principal.
            connection.execute(text(f'TRUNCATE TABLE "{table_name}"'))

            # 4. Insere os novos dados.
            connection.execute(text(f'INSERT INTO "{table_name}" SELECT * FROM "{temp_table_name}"'))

            # 5. Define a nova chave primária.
            logger.info(f"Definindo 'ID' como chave primária na tabela '{table_name}'.")
            connection.execute(text(f'ALTER TABLE "{table_name}" ADD PRIMARY KEY ("ID");'))

            logger.info("Tabela principal atualizada com sucesso.")

            # 6. Remove a tabela temporária.
            logger.info(f"Removendo tabela temporária: {temp_table_name}")
            connection.execute(text(f'DROP TABLE "{temp_table_name}"'))

        logger.info(f"{len(df)} registros carregados com sucesso em '{table_name}'.")

    except Exception as e:
        logger.critical(f"Falha ao carregar dados para '{table_name}': {e}", exc_info=True)
        with engine.connect() as connection:
            connection.execute(text(f'DROP TABLE IF EXISTS "{temp_table_name}"'))
            connection.commit()
        raise


def _load_to_elasticsearch(df: pd.DataFrame, index_alias: str):
    """
    Carrega dados de um DataFrame para o Elasticsearch usando a estratégia
    de "blue-green" com aliases para uma atualização sem indisponibilidade.
    """
    # Mapeamento do Elasticsearch para os dados de alíquotas.
    es_mapping = {
        "properties": {
            "ID": {"type": "integer"},
            "UF": {"type": "keyword"},
            "ESTADO": {"type": "text", "analyzer": "brazilian"},
            "ALIQUOTA": {"type": "scaled_float", "scaling_factor": 100},
            "GENERICO": {"type": "boolean"}
        }
    }
    new_index_name = f"{index_alias}-{int(time.time())}"

    try:
        logger.info("Conectando ao Elasticsearch...")
        es = Elasticsearch(config.ES_URL)

        logger.info(f"Criando o novo índice '{new_index_name}'.")
        es.indices.create(index=new_index_name, mappings=es_mapping)

        # Prepara os documentos para a indexação em massa.
        actions = [
            {"_index": new_index_name, "_id": record["ID"], "_source": record}
            for record in df.to_dict(orient='records')
        ]

        logger.info(f"Indexando {len(actions)} documentos em '{new_index_name}'...")
        helpers.bulk(es, actions)
        logger.info(f"Dados indexados com sucesso em '{new_index_name}'.")

        # Inicia o processo atómico de atualização do alias.
        logger.info(f"Atualizando o alias '{index_alias}' para apontar para '{new_index_name}'.")
        old_indices = []
        if es.indices.exists_alias(name=index_alias):
            alias_info = es.indices.get_alias(name=index_alias)
            old_indices = list(alias_info.keys())

        alias_actions = {
            "actions": [
                {"add": {"index": new_index_name, "alias": index_alias}}
            ]
        }
        for old_index in old_indices:
            alias_actions["actions"].append({"remove": {"index": old_index, "alias": index_alias}})

        es.indices.update_aliases(body=alias_actions)

        # Remove os índices antigos que já não estão em uso.
        for old_index in old_indices:
            logger.info(f"Deletando índice antigo: {old_index}")
            es.indices.delete(index=old_index)

        logger.info(f"Alias '{index_alias}' atualizado e índices antigos removidos.")

    except Exception as e:
        logger.critical(f"Falha ao carregar dados para '{index_alias}': {e}", exc_info=True)
        # Garante a limpeza do novo índice em caso de erro.
        es = Elasticsearch(config.ES_URL)
        es.indices.delete(index=new_index_name, ignore_unavailable=True)
        raise


def main():
    """
    Ponto de entrada do script, responsável por analisar os argumentos
    da linha de comando e iniciar o processo de carga.
    """
    parser = argparse.ArgumentParser(description="Carregador de dados manuais para PostgreSQL e Elasticsearch.")
    parser.add_argument("filename", type=str, help="Nome do arquivo CSV em 'dados_manuais'.")
    parser.add_argument("--table-name", type=str, help="Nome da tabela/índice de destino.")
    args = parser.parse_args()

    # Define o identificador para a tabela e índice, usando o nome do arquivo se não for especificado.
    db_identifier = args.table_name or os.path.splitext(args.filename)[0]
    load_manual_data(args.filename, db_identifier)


if __name__ == '__main__':
    main()