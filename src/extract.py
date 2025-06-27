import os
import requests
import logging
from bs4 import BeautifulSoup
from src import config

# configuracao do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# navega na página da CMED para encontrar a URL do arquivo XLS mais recente.
# a lógica busca por um link que contenha "Lista de Preços de Medicamentos" no texto.
def find_latest_cmed_xls_url():
    try:
        logging.info(f"Buscando a página de preços da CMED em: {config.CMED_PRICES_PAGE_URL}")
        response = requests.get(config.CMED_PRICES_PAGE_URL, timeout=30)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')

        xls_link = soup.find(
            'a',

        )