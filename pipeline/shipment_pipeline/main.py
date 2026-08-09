


import logging

from pipeline.shipment_pipeline.extract import shipments_extraction

from pipeline.shipment_pipeline.transform import transform_shipments_data

from pipeline.shipment_pipeline.load import load_shipments_data


logger = logging.getLogger(__name__)


def run_pipeline():

    try:
        logger.info("=" * 80)
        logger.info("STARTING EXTRACTION")
        logger.info("=" * 80)

        shipments_extraction()

        logger.info("=" * 80)
        logger.info("STARTING TRANSFORMATION")
        logger.info("=" * 80)

        transform_shipments_data()

        logger.info("=" * 80)
        logger.info("STARTING LOADING")
        logger.info("=" * 80)

        load_shipments_data()

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