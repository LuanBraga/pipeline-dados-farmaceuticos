import logging
import pandas as pd
import time
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import (
    String, Text, BigInteger, Numeric, Boolean
)
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError
from src import config

# Configuração do sistema de logging para monitorizar a execução.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_to_postgres(df: pd.DataFrame):
    """
    Carrega os dados de um DataFrame para uma tabela no PostgreSQL.
    Utiliza uma estratégia de "blue-green deployment": os dados são carregados
    numa tabela temporária e depois movidos para a tabela principal
    dentro de uma transação atómica para garantir zero downtime.
    """
    if df.empty:
        logger.warning("O DataFrame está vazio. Nenhum dado será carregado no PostgreSQL.")
        return

    # Mapeamento explícito de tipos de dados do Pandas para tipos SQL.
    # Garante a integridade dos dados e otimiza o armazenamento.
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

    # Mapeia dinamicamente todas as colunas de preço para o tipo Numeric.
    price_cols = [col for col in df.columns if col.startswith('PRECO_MAXIMO_AO_CONSUMIDOR')]
    for col in price_cols:
        dtype_mapping[col] = Numeric(10, 2)

    # Gera um nome único para a tabela temporária usando um timestamp.
    temp_table_name = f"{config.DB_TABLE_NAME}_temp_{int(time.time())}"

    try:
        logger.info(f"Conectando ao banco de dados PostgreSQL em {config.DB_HOST}...")
        engine = create_engine(config.DATABASE_URL)

        # Converte a coluna de restrição hospitalar para o tipo booleano.
        if 'RESTRICAO_HOSPITALAR' in df.columns:
            df['RESTRICAO_HOSPITALAR'] = df['RESTRICAO_HOSPITALAR'].apply(lambda x: True if x == 'Sim' else False)

        logger.info(f"Carregando dados na tabela temporária: {temp_table_name}")
        # Realiza a carga inicial dos dados para a tabela temporária.
        df.to_sql(
            temp_table_name,
            engine,
            if_exists='replace',  # Garante que a tabela temp seja sempre nova.
            index=False,
            dtype=dtype_mapping,
            chunksize=1000  # Processa a carga em lotes para otimizar o uso de memória.
        )

        # Inicia uma transação para garantir que a substituição seja atómica.
        # Ou todas as operações são bem-sucedidas, ou nenhuma é aplicada.
        with engine.begin() as connection:
            logger.info("Iniciando transação para substituir a tabela principal.")
            # 1. Garante que a tabela principal exista, copiando a estrutura da temporária.
            connection.execute(text(f'CREATE TABLE IF NOT EXISTS "{config.DB_TABLE_NAME}" (LIKE "{temp_table_name}")'))

            # 2. Remove a chave primária antiga para evitar conflitos.
            # Esta é a correção crucial para permitir reexecuções do pipeline.
            connection.execute(
                text(f'ALTER TABLE "{config.DB_TABLE_NAME}" DROP CONSTRAINT IF EXISTS "{config.DB_TABLE_NAME}_pkey"'))

            # 3. Limpa a tabela principal de forma eficiente.
            connection.execute(text(f'TRUNCATE TABLE "{config.DB_TABLE_NAME}"'))

            # 4. Insere os novos dados da tabela temporária na principal.
            connection.execute(text(f'INSERT INTO "{config.DB_TABLE_NAME}" SELECT * FROM "{temp_table_name}"'))

            # 5. Recria a chave primária para garantir a integridade e performance das consultas.
            logger.info(f"Definindo 'REGISTRO_CMED' como chave primária.")
            connection.execute(text(f'ALTER TABLE "{config.DB_TABLE_NAME}" ADD PRIMARY KEY ("REGISTRO_CMED");'))

            logger.info("Tabela principal atualizada com sucesso.")

            # 6. Remove a tabela temporária após o sucesso da operação.
            logger.info(f"Removendo tabela temporária: {temp_table_name}")
            connection.execute(text(f'DROP TABLE "{temp_table_name}"'))

        logger.info(f"{len(df)} registros carregados com sucesso no PostgreSQL.")

    except SQLAlchemyError as e:
        logger.critical(f"Falha ao carregar dados para o PostgreSQL: {e}", exc_info=True)
        # Em caso de erro, tenta limpar a tabela temporária para não deixar lixo.
        with engine.connect() as connection:
            connection.execute(text(f'DROP TABLE IF EXISTS "{temp_table_name}"'))
            connection.commit()
        raise
    except Exception as e:
        logger.critical(f"Ocorreu um erro inesperado no PostgreSQL: {e}", exc_info=True)
        raise


def load_to_elasticsearch(df: pd.DataFrame):
    """
    Carrega os dados de um DataFrame para o Elasticsearch.
    Utiliza a estratégia de "blue-green" com aliases: os dados são indexados
    num novo índice e um alias é atomicamente redirecionado para ele,
    garantindo que não haja downtime durante a atualização.
    """
    if df.empty:
        logger.warning("O DataFrame está vazio. Nenhum dado será carregado no Elasticsearch.")
        return

    # 1. Cria uma cópia do DataFrame para isolar as modificações.
    df_es = df.copy()

    # 2. Define a lista de campos a serem removidos.
    cols_to_remove = [
        'CLASSE_TERAPEUTICA',
        'CNPJ',
        'CODIGO_GGREM',
        'LISTA_DE_CONCESSAO_DE_CREDITO_TRIBUTARIO_PIS_COFINS',
        'NUMERO_REGISTRO_PRODUTO',
        'REGIME_DE_PRECO',
        'REGISTRO_CMED',
        'RESTRICAO_HOSPITALAR',
        'TARJA',
        'TIPO_PRODUTO'
    ]

    # 3. Guarda os IDs dos documentos, pois a coluna será removida.
    cmed_ids = df_es['REGISTRO_CMED']

    # 4. Remove as colunas especificadas do DataFrame.
    df_es.drop(columns=cols_to_remove, inplace=True, errors='ignore')
    logger.info(f"Removidas {len(cols_to_remove)} colunas da cópia para o Elasticsearch.")

    # 5. Identifica e remove as colunas de preço da cópia.
    price_cols_to_drop = [col for col in df_es.columns if col.startswith('PRECO_MAXIMO_AO_CONSUMIDOR')]
    if price_cols_to_drop:
        df_es.drop(columns=price_cols_to_drop, inplace=True)
        logger.info(f"Removidas {len(price_cols_to_drop)} colunas de preço da cópia para o Elasticsearch.")

    logger.info("Criando campo 'PRINCIPIO_ATIVO_UNICO' para otimização de busca.")
    df_es['PRINCIPIO_ATIVO_UNICO'] = ~df_es['PRINCIPIO_ATIVO'].str.contains('+', regex=False, na=True)

    # Mapeamento otimizado para autocomplete.
    es_settings = {
        "analysis": {
            "analyzer": {
                "brazilian_folding": {
                    "tokenizer": "standard",
                    "filter": [
                        "lowercase",
                        "asciifolding",
                        "brazilian_stop",
                        "brazilian_stemmer"
                    ]
                }
            },
            "filter": {
                "brazilian_stop": {
                    "type": "stop",
                    "stopwords": "_brazilian_"
                },
                "brazilian_stemmer": {
                    "type": "stemmer",
                    "language": "brazilian"
                }
            }
        }
    }

    # 6. Mapeamento do Elasticsearch apenas com os campos restantes.
    es_mapping = {
        "properties": {
            "PRODUTO": {
                "type": "text",
                "analyzer": "brazilian_folding",
                "fields": {"suggest": {"type": "search_as_you_type"}}
            },
            "PRINCIPIO_ATIVO": {
                "type": "text",
                "analyzer": "brazilian_folding",
                "fields": {"suggest": {"type": "search_as_you_type"}}
            },
            "APRESENTACAO": {
                "type": "text",
                "analyzer": "brazilian_folding",
                "fields": {"suggest": {"type": "search_as_you_type"}}
            },
            "LABORATORIO": {
                "type": "text",
                "analyzer": "brazilian_folding",
                "fields": {"suggest": {"type": "search_as_you_type"}}
            },
            "PRINCIPIO_ATIVO_UNICO": {"type": "boolean"},
        }
    }

    # Define um nome único para o novo índice e o nome do alias público.
    new_index_name = f"{config.ES_INDEX_NAME}-{int(time.time())}"
    alias_name = config.ES_INDEX_NAME

    try:
        logger.info(f"Conectando ao Elasticsearch em {config.ES_URL}...")
        es = Elasticsearch(config.ES_URL)

        logger.info(f"Criando o novo índice '{new_index_name}' com mapeamento explícito.")
        es.indices.create(index=new_index_name, mappings=es_mapping, settings=es_settings)

        # Prepara os documentos para a indexação em massa (bulk), usando os IDs salvos.
        actions = [
            {
                "_index": new_index_name,
                "_id": cmed_id,
                "_source": record
            }
            for cmed_id, record in zip(cmed_ids, df_es.to_dict(orient='records'))
        ]

        df_es_size_mb = df_es.memory_usage(deep=True).sum() / (1024 * 1024)
        logger.info(f"Tamanho do DataFrame em memória para indexação: {df_es_size_mb:.2f} MB.")

        logger.info(f"Indexando {len(actions)} documentos em '{new_index_name}'...")
        helpers.bulk(es, actions)
        logger.info("Dados indexados com sucesso no novo índice.")

        # Realiza a troca atómica do alias para o novo índice.
        logger.info(f"Atualizando o alias '{alias_name}' para apontar para '{new_index_name}'.")
        old_indices = []
        if es.indices.exists_alias(name=alias_name):
            alias_info = es.indices.get_alias(name=alias_name)
            old_indices = list(alias_info.keys())

        # Prepara as ações de remoção do alias dos índices antigos e adição ao novo.
        alias_actions = {
            "actions": [
                {"add": {"index": new_index_name, "alias": alias_name}}
            ]
        }
        for old_index in old_indices:
            alias_actions["actions"].append({"remove": {"index": old_index, "alias": alias_name}})

        # Executa a atualização do alias de forma atómica.
        es.indices.update_aliases(body=alias_actions)

        # Remove os índices antigos que já não estão em uso.
        for old_index in old_indices:
            logger.info(f"Deletando índice antigo: {old_index}")
            es.indices.delete(index=old_index)

        logger.info("Alias atualizado e índices antigos removidos.")

    except BulkIndexError as e:
        logger.critical(f"Falha ao indexar {len(e.errors)} documento(s).", exc_info=False)
        for i, error in enumerate(e.errors[:5]):
            logger.error(f"  Erro #{i + 1}: {error}")
        # Em caso de erro, remove o novo índice para não deixar lixo.
        es.indices.delete(index=new_index_name, ignore_unavailable=True)
        raise
    except Exception as e:
        logger.critical(f"Falha ao carregar dados para o Elasticsearch: {e}", exc_info=True)
        es = Elasticsearch(config.ES_URL)
        es.indices.delete(index=new_index_name, ignore_unavailable=True)
        raise


def run(df: pd.DataFrame):
    """
    Orquestra a execução da etapa de carga para todos os sistemas de destino.
    """
    logger.info("--- Iniciando Etapa de Carga de Dados ---")
    try:
        load_to_postgres(df)
        load_to_elasticsearch(df)
        logger.info("--- Etapa de Carga de Dados Concluída com Sucesso ---")
    except Exception as e:
        logger.critical(f"Ocorreu um erro durante a etapa de carga: {e}", exc_info=True)
        raise