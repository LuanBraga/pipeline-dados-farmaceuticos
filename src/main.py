import logging
import time
from src import extract, transform

#, load

# configuracao do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# orquestra a execução das etapas de extração, transformação e carga.
def main():
    start_time = time.time()
    logger.info("=================================================")
    logger.info("  INICIANDO O PIPELINE DE DADOS DE MEDICAMENTOS  ")
    logger.info("=================================================")

    try:
        # Etapa 1: Extração dos dados
        logger.info("Iniciando a etapa de Extração.")
        extract.run()
        logger.info("Etapa de Extração concluída.")

        # Etapa 2: Transformação e limpeza dos dados
        logger.info("Iniciando a etapa de Transformação.")
        final_df = transform.run()
        logger.info("Etapa de Transformação concluída.")

        # Etapa 3: Carga dos dados no banco e no motor de busca
        # if final_df is not None and not final_df.empty:
        #    logger.info("Iniciando a etapa de Carga.")
        #    load.run(final_df)
        #    logger.info("Etapa de Carga concluída.")
        # else:
        #    logger.warning("O pipeline foi interrompido pois a etapa de transformação não retornou dados.")

    except Exception as e:
        logger.critical(f"Ocorreu um erro fatal no pipeline: {e}", exc_info=True)
    finally:
        end_time = time.time()
        total_time = end_time - start_time
        logger.info("==================================================")
        logger.info(f"PIPELINE FINALIZADO. Tempo total de execução: {total_time:.2f} segundos.")
        logger.info("==================================================")


if __name__ == '__main__':
    main()