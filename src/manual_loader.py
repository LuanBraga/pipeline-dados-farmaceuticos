import logging
import os
import pandas as pd
import argparse
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import String, Boolean, Integer, Numeric
from src import config

# Configuração do sistema de logging.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_manual_data(csv_filename: str, db_identifier: str):
    """
    Orquestra a carga de um arquivo CSV manual para o PostgreSQL.
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


def main():
    """
    Ponto de entrada do script, responsável por analisar os argumentos
    da linha de comando e iniciar o processo de carga.
    """
    parser = argparse.ArgumentParser(description="Carregador de dados manuais para PostgreSQL.")
    parser.add_argument("filename", type=str, help="Nome do arquivo CSV em 'dados_manuais'.")
    parser.add_argument("--table-name", type=str, help="Nome da tabela de destino.")
    args = parser.parse_args()

    # Define o identificador para a tabela, usando o nome do arquivo se não for especificado.
    db_identifier = args.table_name or os.path.splitext(args.filename)[0]
    load_manual_data(args.filename, db_identifier)


if __name__ == '__main__':
    main()