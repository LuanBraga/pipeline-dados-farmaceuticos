import os
import requests
import logging
import sys
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from src import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# navega na página da CMED para encontrar a URL do arquivo XLS mais recente.
# a lógica busca por um card com o título "PMC - xls" e extrai o link pai.
def find_cmed_xls_url():
    try:
        logger.info(f"Buscando a página de preços da CMED em: {config.CMED_PRICES_PAGE_URL}")
        response = requests.get(config.CMED_PRICES_PAGE_URL, timeout=30, verify=False)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'lxml')

        target_span = soup.find('span', class_='titulo', string='PMC - xls')

        if not target_span:
            logger.error("Não foi possível encontrar o <span> com o título 'PMC - xls'. A estrutura do site pode ter mudado.")
            return None

        xls_link = target_span.find_parent('a')

        if xls_link and xls_link.get('href'):
            full_url = xls_link['href']
            logger.info(f"URL do arquivo CMED encontrada: {full_url}")
            return full_url
        else:
            logger.error("Encontrou o título, mas não o link <a> associado ou o atributo 'href'.")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Erro de rede ao acessar a página da CMED: {e}")
        return None
    except Exception as e:
        logger.error(f"Ocorreu um erro inesperado durante o parsing do HTML da CMED: {e}")
        return None

# baixa um arquivo de uma URL e o salva em um caminho de destino.
def download_file(url, destination_path):
    try:
        logger.info(f"Iniciando download de {url}...")
        with requests.get(url, stream=True, timeout=120, verify=False) as r:
            r.raise_for_status()
            os.makedirs(os.path.dirname(destination_path), exist_ok=True)
            with open(destination_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        logger.info(f"Arquivo salvo com sucesso em: {destination_path}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Falha no download do arquivo {url}. Erro: {e}")
        return False

# orquestra o download dos arquivos da ANVISA e da CMED.
def run():
    logger.info("--- Iniciando Etapa de Extração de Dados ---")

    # download do arquivo da ANVISA
    logger.info("Iniciando download do arquivo da ANVISA...")
    anvisa_filename = config.ANVISA_FILENAME
    anvisa_file_path = os.path.join(config.DATA_DIR,anvisa_filename)

    if not download_file(config.ANVISA_CSV_URL, anvisa_file_path):
        error_message = "PIPELINE INTERROMPIDO: Falha crítica no download do arquivo da ANVISA. A extração não pode ser concluída."
        logger.critical(error_message)
        raise RuntimeError(error_message)

    logger.info("Download do arquivo da ANVISA concluído com sucesso.")

    # download do arquivo da CMED
    logger.info("Iniciando busca pela URL do arquivo da CMED...")
    # primeiro, encontra a URL
    cmed_url = find_cmed_xls_url()

    # prossegue com o download apenas se a URL foi encontrada
    if cmed_url:
        # parseia a URL para obter seus componentes e procura o nome do arquivo dinamicamente
        parsed_url = urlparse(cmed_url)
        path_components = parsed_url.path.split('/')
        cmed_filename = next((part for part in path_components if part.endswith(('.xls', '.xlsx'))), None)

        if not cmed_filename:
            error_message = f"PIPELINE INTERROMPIDO: Não foi possível extrair um nome de arquivo .xls/.xlsx da URL: {cmed_url}"
            logger.critical(error_message)
            raise RuntimeError(error_message)

        cmed_file_path = os.path.join(config.DATA_DIR,cmed_filename)

        if os.path.exists(cmed_file_path):
            logger.info(f"O arquivo '{cmed_filename}' já existe no destino. Download pulado.")
        else:
            logger.info(f"Nova versão do arquivo CMED detectada: '{cmed_filename}'.")

            # procura e remove qualquer versão antiga do arquivo CMED antes de baixar a nova.
            logger.info("Procurando por versões antigas para remover...")
            for filename_in_dir in os.listdir(config.DATA_DIR):
                if filename_in_dir.startswith('xls_conformidade_site_') and filename_in_dir.endswith(('.xls', '.xlsx')):
                    old_file_path = os.path.join(config.DATA_DIR, filename_in_dir)
                    try:
                        os.remove(old_file_path)
                        logger.info(f"Arquivo antigo '{filename_in_dir}' removido com sucesso.")
                    except OSError as e:
                        logger.error(f"Erro ao remover o arquivo antigo '{filename_in_dir}': {e}")

            logger.info(f"Iniciando download de '{cmed_filename}'...")
            if not download_file(cmed_url, cmed_file_path):
                error_message = "PIPELINE INTERROMPIDO: Falha crítica no download do arquivo da CMED."
                logger.critical(error_message)
                raise RuntimeError(error_message)
            logger.info("Download do arquivo da CMED concluído com sucesso.")
    else:
        error_message = "PIPELINE INTERROMPIDO: Não foi possível obter a URL do arquivo da CMED. A extração não pode ser concluída."
        logger.critical(error_message)
        raise RuntimeError(error_message)

    logger.info("--- Etapa de Extração de Dados Concluída com Sucesso ---")

if __name__ == '__main__':
    try:
        run()
        logger.info("Execução do EXTRACT concluída com sucesso.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Ocorreu um erro fatal durante a execução do pipeline: {e}")
        sys.exit(1)