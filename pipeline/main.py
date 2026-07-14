


import logging

from pipeline.extract import postgres_extraction, mongodb_extraction, api_extraction
from pipeline.transform import transform_postgres, transform_mongodb, transform_api
from pipeline.load import load_postgres, load_mongodb, load_api



logger = logging.getLogger(__name__)


def run_pipeline():

    try:
        logger.info("=" * 80)
        logger.info("STARTING EXTRACTION")
        logger.info("=" * 80)

        postgres_extraction()
        mongodb_extraction()
        api_extraction()

        logger.info("=" * 80)
        logger.info("STARTING TRANSFORMATION")
        logger.info("=" * 80)

        transform_postgres()
        transform_mongodb()
        transform_api()

        logger.info("=" * 80)
        logger.info("STARTING LOADING")
        logger.info("=" * 80)

        load_postgres()
        load_mongodb()
        load_api()

        logger.info("=" * 80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)


    except Exception:
        logger.exception(
            "PIPELINE FAILED"
        )
        raise


if __name__ == "__main__":
    run_pipeline()