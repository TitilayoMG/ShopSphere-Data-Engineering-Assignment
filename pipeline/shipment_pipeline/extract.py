# -------------------------
# imports: Standard Libraries
# -------------------------
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

# -------------------------
# Imports:  Custom Libraries
# -------------------------
from dotenv import load_dotenv
import requests
from pipeline.utils import (
    get_minio_client,
    read_minio_watermark,
    records_to_parquet_buffer,
    upload_to_minio,
    write_minio_watermark,
    load_config
)

load_dotenv()

# -------------------------
# LOGGING
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------------
# Variables
# -------------------------
DATA_SOURCES, CHUNK_SIZE = load_config()
# -------------------------
# Extracting SwiftDrop shipment
# -------------------------
def shipments_extraction():
    """
    Extract data from the SwiftDrop API and store it in MinIO as Parquet files.

    Workflow:
    - Read the last processed `updated_since` watermark from MinIO metadata.
    - Iterate through all configured API endpoints.
    - Request data from each endpoint, handling pagination where applicable.
    - Apply the watermark to fetch only new or updated shipment records.
    - Flatten JSON responses into tabular format using pandas.
    - Write each batch to a timestamped Parquet file in the raw MinIO layer.
    - Log extraction progress, file details, and record counts.
    - Track the latest `updated_at` value across all extracted shipment records.
    - Update the watermark only if newer data was successfully extracted.
    """
    base_url = os.getenv("MOCK_API_BASE_URL").rstrip("/")
    datetimestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")[:-3]
    minio_client, bucket = get_minio_client()
    logger.info("=" * 80)
    logger.info("Starting SwiftDrop Shipments extraction")

    try:
        for endpoint in DATA_SOURCES["fast_api"]["datasets"]:
            table = endpoint["table"]
            logger.info(f"Extracting table: {table}")

            watermark, last_file_number = read_minio_watermark(
                minio_client,
                bucket,
                source="swiftdrop",
                object_name="shipments",
                field="updated_since"
            )
            logger.info(f"Watermark: {watermark}")
            
            latest_updated_at = watermark
            file_number = last_file_number + 1
            page = 1
            total_records = 0

            while True:
                params = {}
                if endpoint["paginated"]:
                    params["page"] = page
                    params["limit"] = 100

                    if watermark:
                        params["updated_since"] = watermark

                response = requests.get(url=f"{base_url}{endpoint['endpoint']}", params=params, timeout=60)
                response.raise_for_status()
                payload = response.json()

                if endpoint["paginated"]:
                    records = payload["shipments"]
                else:
                    file_number = 1
                    records = payload

                if not records:
                    logger.info(f"No more records found for {table}")
                    break

                dataframe, parquet_buffer = records_to_parquet_buffer(records)
                object_name = (f"raw/swiftdrop/{table}/{table}_{datetimestamp}_{file_number}.parquet")

                upload_to_minio(
                    client=minio_client,
                    bucket=bucket,
                    object_name=object_name,
                    buffer=parquet_buffer
                )

                logger.info(
                    f"{table} | "
                    f"file={file_number} | "
                    f"records={len(dataframe):,} | "
                    f"size={parquet_buffer.getbuffer().nbytes:,} bytes"
                )

                total_records += len(dataframe)
                file_number += 1

                if endpoint["paginated"]:
                    updated_values = dataframe["updated_at"].dropna().tolist()
                    if updated_values:
                        page_latest = max(updated_values)
                        if (latest_updated_at is None or page_latest > latest_updated_at):
                            latest_updated_at = page_latest

                    next_page = payload.get("next_page")
                    if next_page is None:
                        break
                    page = next_page
                else:
                    break
            logger.info(f"{table}: extracted {total_records:,} records")
        if latest_updated_at and latest_updated_at != watermark:
            write_minio_watermark(
                minio_client,
                bucket,
                source="swiftdrop",
                object_name="shipments",
                field="updated_since",
                value=latest_updated_at,
                file_number=file_number - 1
            )
            logger.info(f"Updated watermark to {latest_updated_at}")
        logger.info("SwiftDrop shipments extraction completed")
        logger.info("=" * 80)

    except Exception:
            logger.exception("Unexpected error")
            raise