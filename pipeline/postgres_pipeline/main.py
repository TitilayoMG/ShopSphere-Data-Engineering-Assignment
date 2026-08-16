
import logging

from pipeline.postgres_pipeline.extract import (
    api_extraction,
    mongodb_extraction,
    postgres_extraction,
)

from pipeline.postgres_pipeline.transform import (
    transform_api,
    transform_mongodb,
    transform_postgres,
)

from pipeline.postgres_pipeline.load import load_all_data_source_files


logger = logging.getLogger(__name__)


def run_pipeline():

    try:
        logger.info("=" * 80)
        logger.info("STARTING EXTRACTION")
        logger.info("=" * 80)

        postgres_extraction()
        mongodb_extraction()
        # api_extraction()

        logger.info("=" * 80)
        logger.info("STARTING TRANSFORMATION")
        logger.info("=" * 80)

        # transform_postgres()
        # transform_mongodb()
        # transform_api()

        logger.info("=" * 80)
        logger.info("STARTING LOADING")
        logger.info("=" * 80)

        # load_all_data_source_files()

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