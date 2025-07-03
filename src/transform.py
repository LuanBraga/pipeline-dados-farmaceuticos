import os
import pandas as pd
import logging
import glob
from src import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# encontra o arquivo XLS mais recente da CMED no diretório de dados brutos
def find_latest_cmed_file():
    search_pattern = os.path.join(config.DATA_DIR, '*.xls*')
    files = glob.glob(search_pattern)
    if not files:
        return None
    # Encontra o arquivo mais recente baseado no tempo de modificação
    latest_file = max(files, key=os.path.getmtime)
    logger.info(f"Arquivo CMED encontrado: {latest_file}")
    return latest_file

# limpa e padroniza o DataFrame de dados da ANVISA
def clean_anvisa_data(df):
    logger.info("Iniciando limpeza dos dados da ANVISA.")
    anvisa_cols = [
        'TIPO_PRODUTO', 'NOME_PRODUTO', 'CATEGORIA_REGULATORIA',
        'NUMERO_REGISTRO_PRODUTO', 'CLASSE_TERAPEUTICA',
        'EMPRESA_DETENTORA_REGISTRO', 'SITUACAO_REGISTRO'
    ]

    cols_to_use = [col for col in anvisa_cols if col in df.columns]
    df = df[cols_to_use].copy()

    # converte para string, remove não-dígitos e trunca para 9 caracteres
    if 'NUMERO_REGISTRO_PRODUTO' in df.columns:
        df['NUMERO_REGISTRO_PRODUTO'] = df['NUMERO_REGISTRO_PRODUTO'].astype(str).str.replace(r'\D', '', regex=True)
        df.dropna(subset=['NUMERO_REGISTRO_PRODUTO'], inplace=True)
        df['NUMERO_REGISTRO_PRODUTO'] = df['NUMERO_REGISTRO_PRODUTO'].str.slice(0, 9)

    # remove espaços em branco extras das colunas de texto
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].str.strip()

    logger.info("Limpeza dos dados da ANVISA concluída.")
    return df

# limpa e padroniza o DataFrame de dados da CMED
def clean_cmed_data(df):
    logger.info("Iniciando limpeza dos dados da CMED.")
    cmed_col_rename = {
        'SUBSTÂNCIA': 'SUBSTANCIA',
        'LABORATÓRIO': 'LABORATORIO',
        'CLASSE TERAPÊUTICA': 'CLASSE_TERAPEUTICA_CMED',
        'TIPO DE PRODUTO (STATUS DO PRODUTO)': 'TIPO_PRODUTO_CMED',
        'PF Sem Impostos': 'PF_SEM_IMPOSTOS'
    }
    df = df.rename(columns=cmed_col_rename)

    cmed_cols = [
        'SUBSTANCIA', 'LABORATORIO', 'CNPJ', 'REGISTRO', 'PRODUTO',
        'APRESENTAÇÃO', 'CLASSE_TERAPEUTICA_CMED', 'TIPO_PRODUTO_CMED',
        'TARJA', 'PF_SEM_IMPOSTOS'
    ]
    cols_to_use = [col for col in cmed_cols if col in df.columns]
    df = df[cols_to_use].copy()

    if 'REGISTRO' in df.columns:
        df['REGISTRO'] = df['REGISTRO'].astype(str).str.replace(r'\D', '', regex=True)
        df.dropna(subset=['REGISTRO'], inplace=True)
        # cria uma coluna base para o merge, com os 9 primeiros dígitos
        df['REGISTRO_BASE'] = df['REGISTRO'].str.slice(0, 9)

    if 'PF_SEM_IMPOSTOS' in df.columns:
        df['PF_SEM_IMPOSTOS'] = pd.to_numeric(
            df['PF_SEM_IMPOSTOS'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False),
            errors='coerce'
        )
    # remove espaços em branco extras das colunas de texto
    for col in df.select_dtypes(include=['object']).columns:
        if col != 'REGISTRO_BASE':
            df[col] = df[col].str.strip()

    logger.info("Limpeza dos dados da CMED concluída.")
    return df

# realiza o merge dos dataframes da ANVISA e CMED
def merge_datasets(anvisa_df, cmed_df):
    logger.info("Iniciando a unificação dos datasets.")
    merged_df = pd.merge(
        anvisa_df,
        cmed_df,
        left_on='NUMERO_REGISTRO_PRODUTO',
        right_on='REGISTRO_BASE',
        how='inner' # 'inner' join para manter apenas os registros que existem em ambas as bases
    )
    logger.info(f"Unificação concluída. {len(merged_df)} registros correspondentes encontrados.")
    return merged_df

# orquestra o processo de transformação: carregar, limpar, unificar e salvar os dados
def run():
    logger.info("--- Iniciando Etapa de Transformação de Dados ---")

    anvisa_path = os.path.join(config.DATA_DIR, config.ANVISA_FILENAME)
    cmed_path = find_latest_cmed_file()

    if not os.path.exists(anvisa_path) or cmed_path is None:
        error_message = "Arquivos brutos não encontrados. Execute a etapa de extração primeiro."
        logger.error(error_message)
        raise FileNotFoundError(error_message)

    try:
        logger.info(f"Carregando dados da ANVISA de: {anvisa_path}")
        df_anvisa = pd.read_csv(anvisa_path, sep=';', encoding='latin1', low_memory=False)

        logger.info(f"Carregando dados da CMED de: {cmed_path}")
        # pula as primeiras linhas que são cabeçalho no arquivo da CMED
        df_cmed = pd.read_excel(cmed_path, skiprows=41)
    except Exception as e:
        logger.critical(f"Falha ao carregar os dados brutos: {e}", exc_info=True)
        raise

    # limpeza e padronização
    df_anvisa_clean = clean_anvisa_data(df_anvisa)
    df_cmed_clean = clean_cmed_data(df_cmed)

    # unificação
    df_unified = merge_datasets(df_anvisa_clean, df_cmed_clean)

    # organização final
    logger.info("Organizando o resultado final...")

    final_columns = [
        'TIPO_PRODUTO',
        'NOME_PRODUTO',
        'CATEGORIA_REGULATORIA',
        'NUMERO_REGISTRO_PRODUTO',
        'CLASSE_TERAPEUTICA',
        'EMPRESA_DETENTORA_REGISTRO',
        'SITUACAO_REGISTRO',
        'SUBSTANCIA',
        'LABORATORIO',
        'CNPJ',
        'REGISTRO',
        'PRODUTO',
        'APRESENTAÇÃO',
        'CLASSE_TERAPEUTICA_CMED',
        'TIPO_PRODUTO_CMED',
        'TARJA',
        'PF_SEM_IMPOSTOS'
    ]

    # Garante que apenas colunas existentes sejam selecionadas
    final_columns_exist = [col for col in final_columns if col in df_unified.columns]
    df_final = df_unified[final_columns_exist]

    # salvando o resultado ---
    output_path = os.path.join(config.PROCESSED_DATA_DIR, config.UNIFIED_FILENAME)
    try:
        logger.info(f"Salvando arquivo unificado em: {output_path}")
        df_final.to_csv(output_path, index=False, sep=';', encoding='utf-8-sig')
        logger.info(f"Arquivo '{config.UNIFIED_FILENAME}' salvo com sucesso.")
    except Exception as e:
        logger.critical(f"Falha ao salvar o arquivo processado: {e}", exc_info=True)
        raise

    logger.info("--- Etapa de Transformação de Dados Concluída com Sucesso ---")
    return df_final

if __name__ == '__main__':
    try:
        run()
    except Exception as e:
        logger.error(f"Ocorreu um erro fatal durante a execução da transformação: {e}")