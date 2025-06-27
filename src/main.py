import logging
import time
from src import extract, transform, load

# configuracao do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# orquestra a execução das etapas de extração, transformação e carga.
def main():
    start_time = time.time()
    logging.info("-----------------------------------------------------")
    logging.info("    INICIANDO O PIPELINE DE DADOS DE MEDICAMENTOS    ")
    logging.info("-----------------------------------------------------")

    try:
        extract.run()
