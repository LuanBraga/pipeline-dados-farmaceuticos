import pandas as pd
import os
import re
import logging
from unidecode import unidecode
from src import config

# Configuração do logging para o módulo de transformação
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza os nomes das colunas de um DataFrame.
    Converte para minúsculas, remove acentos, e junta as palavras.
    """
    new_columns = []
    for col in df.columns:
        col_str = str(col)
        # Remove acentos e converte para minúsculas
        normalized_col = unidecode(col_str.lower())
        # Substitui underscore e espaços por nada, juntando as palavras
        normalized_col = re.sub(r'[_-\s]+', '', normalized_col)
        # Remove caracteres não alfanuméricos restantes
        normalized_col = re.sub(r'[^a-z0-9]', '', normalized_col)
        new_columns.append(normalized_col)
    df.columns = new_columns
    return df


def _clean_text_field(series: pd.Series) -> pd.Series:
    """Limpa e padroniza uma coluna de texto."""
    return series.astype(str).str.strip().str.upper()


def _clean_registro_ms(series: pd.Series) -> pd.Series:
    """Limpa a coluna de registro MS, mantendo apenas os dígitos."""
    return series.astype(str).str.replace(r'\D', '', regex=True)


def _find_cmed_file() -> str | None:
    """Busca dinamicamente por um arquivo da CMED (XLS ou XLSX) no diretório de dados."""
    try:
        files = os.listdir(config.DATA_DIR)
        for file in files:
            if file.lower().endswith(('.xls', '.xlsx')):
                logger.info(f"Arquivo da CMED encontrado: {file}")
                return os.path.join(config.DATA_DIR, file)
        return None
    except FileNotFoundError:
        logger.error(f"Diretório de dados brutos não encontrado em: {config.DATA_DIR}")
        return None


def _load_and_process_anvisa_data() -> pd.DataFrame | None:
    """Carrega e processa os dados do arquivo da ANVISA."""
    anvisa_path = os.path.join(config.DATA_DIR, config.ANVISA_FILENAME)
    if not os.path.exists(anvisa_path):
        logger.error(f"Arquivo da ANVISA não encontrado em: {anvisa_path}")
        return None

    logger.info(f"Carregando dados da ANVISA de {config.ANVISA_FILENAME}...")
    df_anvisa = pd.read_csv(anvisa_path, sep=';', encoding='latin-1', low_memory=False)

    df_anvisa = _normalize_column_names(df_anvisa)

    anvisa_cols = {
        'numeroregistroproduto': 'registro_ms',
        'nomeproduto': 'produto',
        'principioativo': 'principio_ativo',
        'empresadetentoraregistro': 'empresa',
        'situacaoregistro': 'situacao_registro'
    }

    # Verifica se todas as colunas esperadas estão presentes após a normalização
    missing_cols = [k for k in anvisa_cols if k not in df_anvisa.columns]
    if missing_cols:
        logger.error(f"As seguintes colunas esperadas não foram encontradas no arquivo da ANVISA: {missing_cols}")
        logger.error(f"Colunas disponíveis: {df_anvisa.columns.tolist()}")
        return None

    df_anvisa = df_anvisa[list(anvisa_cols.keys())].rename(columns=anvisa_cols)

    df_anvisa['registro_ms'] = _clean_registro_ms(df_anvisa['registro_ms'])
    for col in ['produto', 'principio_ativo', 'empresa', 'situacao_registro']:
        if col in df_anvisa.columns:
            df_anvisa[col] = _clean_text_field(df_anvisa[col])

    logger.info(f"Dados da ANVISA processados. {df_anvisa.shape[0]} registros encontrados.")
    return df_anvisa


def _load_and_process_cmed_data(cmed_path: str) -> pd.DataFrame | None:
    """Carrega e processa os dados do arquivo da CMED."""
    logger.info(f"Carregando dados da CMED de {os.path.basename(cmed_path)}...")
    df_cmed = pd.read_excel(cmed_path, header=None, sheet_name=0)

    header_row_index = -1
    for i, row in df_cmed.iterrows():
        if row.astype(str).str.contains('REGISTRO|EAN|APRESENTAÇÃO', case=False, na=False, regex=True).any():
            header_row_index = i
            logger.info(f"Cabeçalho do arquivo CMED identificado na linha {i + 1}.")
            break

    if header_row_index == -1:
        logger.error("Cabeçalho não encontrado no arquivo da CMED. A estrutura pode ter mudado.")
        return None

    df_cmed.columns = df_cmed.iloc[header_row_index]
    df_cmed = df_cmed.iloc[header_row_index + 1:].reset_index(drop=True)

    df_cmed = _normalize_column_names(df_cmed)

    col_registro = next((col for col in df_cmed.columns if 'registro' in col), None)
    col_ean = next((col for col in df_cmed.columns if 'ean' in col), None)
    col_apresentacao = next((col for col in df_cmed.columns if 'apresentacao' in col or 'apresenta__o' in col), None)

    if not all([col_registro, col_ean, col_apresentacao]):
        logger.error(
            "Não foi possível encontrar as colunas essenciais (registro, ean, apresentacao) no arquivo da CMED.")
        return None

    cmed_cols = {col_registro: 'registro_ms', col_ean: 'ean_1', col_apresentacao: 'apresentacao'}
    df_cmed = df_cmed[list(cmed_cols.keys())].rename(columns=cmed_cols)

    df_cmed['registro_ms'] = _clean_registro_ms(df_cmed['registro_ms'])
    df_cmed['apresentacao'] = _clean_text_field(df_cmed['apresentacao'])

    df_cmed = df_cmed.dropna(subset=['registro_ms'])
    df_cmed = df_cmed.drop_duplicates(subset=['registro_ms'], keep='first')

    logger.info(f"Dados da CMED processados. {df_cmed.shape[0]} registros únicos encontrados.")
    return df_cmed


def run() -> pd.DataFrame | None:
    """Orquestra a etapa de transformação dos dados."""
    logger.info("--- Iniciando Etapa de Transformação de Dados ---")

    df_anvisa = _load_and_process_anvisa_data()
    if df_anvisa is None or df_anvisa.empty:
        logger.error("Pipeline interrompido: falha ao processar dados da ANVISA.")
        return None

    cmed_path = _find_cmed_file()
    if not cmed_path:
        logger.warning("Arquivo da CMED não encontrado. O pipeline continuará sem os dados de apresentação e EAN.")
        df_anvisa['ean_1'] = 'N/A'
        df_anvisa['apresentacao'] = 'N/A'
        return df_anvisa

    df_cmed = _load_and_process_cmed_data(cmed_path)
    if df_cmed is None or df_cmed.empty:
        logger.warning("Falha ao processar dados da CMED. O pipeline continuará sem os dados de apresentação e EAN.")
        df_anvisa['ean_1'] = 'N/A'
        df_anvisa['apresentacao'] = 'N/A'
        return df_anvisa

    logger.info("Unindo dados da ANVISA e CMED com base no 'registro_ms'...")
    df_final = pd.merge(
        df_anvisa,
        df_cmed[['registro_ms', 'ean_1', 'apresentacao']],
        on='registro_ms',
        how='left'
    )

    df_final.fillna('N/A', inplace=True)

    df_final = df_final[df_final['registro_ms'].str.strip().ne('')]
    df_final = df_final[df_final['registro_ms'].ne('NA')]
    df_final = df_final.dropna(subset=['registro_ms'])

    logger.info(f"Dados unificados. Total de {df_final.shape[0]} registros.")
    logger.info("--- Etapa de Transformação Concluída com Sucesso ---")

    return df_final