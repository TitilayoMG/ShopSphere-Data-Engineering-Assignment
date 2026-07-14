# -------------------------
# imports: Standard Libraries
# -------------------------
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
import requests
from bson import ObjectId

# -------------------------
# Imports:  Custom Libraries
# -------------------------
from dotenv import load_dotenv
from pymongo import MongoClient
from utils import (
    upload_to_minio,
    get_minio_client,
    get_postgres_connection,
    records_to_parquet_buffer,
    read_minio_watermark,
    write_minio_watermark,
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
# CONFIG
# -------------------------
def load_config():
    """
    Load config.json once and unpack the values so they
    can be used anywhere in this file.
    """
    config_path = Path(__file__).parent / "config.json"
    with open(config_path, "r") as file:
        config = json.load(file)
    return (
        config["data_sources"],
        config["chunk_size"]
    )

# -------------------------
# Variables
# -------------------------
SOURCE_CONFIG, CHUNK_SIZE = load_config()
POSTGRES_CONFIG = SOURCE_CONFIG['postgres']
MONGODB_CONFIG = SOURCE_CONFIG['mongodb']
API_CONFIG = SOURCE_CONFIG['fast_api']

minio_client, bucket = get_minio_client()

# =====================================================================
# DATA SOURCE 1: Postgres
# =====================================================================
def postgres_extraction():
    """
    Extract data from PostgreSQL tables and store it as Parquet files in MinIO.

    Steps:
    - Establish a connection to the source PostgreSQL database.
    - Iterate through each configured table.
    - Perform incremental extraction using the stored `updated_at` watermark
    when available.
    - Perform a full extraction for `order_items` table without watermark support
    - Read data in chunks to minimize memory usage.
    - Convert each chunk to Apache Arrow format and write it as a Snappy-compressed
    Parquet file.
    - Upload each Parquet file to the appropriate raw data path in MinIO.
    - Track the latest `updated_at` value processed during extraction.
    - Update the watermark after a successful extraction for incremental tables.
    - Log extraction progress, file details, row counts, and execution summary.
    """

    start_time = time.time()
    postgres_conn = get_postgres_connection("SOURCE")
    logger.info("Connected to PostgreSQL.")

    general_total_rows = 0
    total_files = 0

    for table in POSTGRES_CONFIG["tables"]:
        logger.info(f"Starting extraction for table '{table}'")
       
        cursor = postgres_conn.cursor()
        cursor.execute(f"SELECT * FROM {table} LIMIT 0")
        columns = [col[0] for col in cursor.description]
        cursor.close()
        
        cursor = postgres_conn.cursor(name=f"{table}_cursor")
        cursor.itersize = CHUNK_SIZE

        if table == "order_items":
            last_file_number = 0
            logger.info("order_items has no updated_at column. Performing full extraction.")
            cursor.execute(f"SELECT * FROM {table}")
        else:
            watermark, last_file_number = read_minio_watermark(
                minio_client,
                bucket,
                source="postgres",
                object_name=table,
                field="updated_at"
            )

            if watermark:
                logger.info(f"Incremental extraction using watermark: {watermark}")
                cursor.execute(
                    f"""
                    SELECT *
                    FROM {table}
                    WHERE updated_at > %s
                    ORDER BY updated_at
                    """,
                    (watermark,)
                )
            else:
                logger.info("No watermark found. Performing full extraction.")
                cursor.execute(
                    f"""
                    SELECT *
                    FROM {table}
                    ORDER BY updated_at
                    """
                )
        datetimestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        
        file_number = last_file_number + 1
        table_rows = 0
        latest_updated_at = None

        while True:
            rows = cursor.fetchmany(CHUNK_SIZE)
            if not rows:
                break
            table_rows += len(rows)
            general_total_rows += len(rows)

            records = [dict(zip(columns, row)) for row in rows]
            if table != "order_items":
                updated_index = columns.index("updated_at")
                page_latest = max(
                    row[updated_index]
                    for row in rows
                    if row[updated_index] is not None
                )
                if (latest_updated_at is None or page_latest > latest_updated_at):
                    latest_updated_at = page_latest
    
            df, buffer= records_to_parquet_buffer(records)

            object_name = (f"raw/postgres/{table}/{table}_{datetimestamp}_{file_number}.parquet")

            upload_to_minio(
                minio_client,
                bucket,
                object_name,
                buffer
            )
            logger.info(
                f"{table} | file={file_number} | "
                f"rows={len(rows):,} | "
                f"size={buffer.getbuffer().nbytes:,} bytes"
            )

            total_files += 1
            file_number += 1

        cursor.close()
        if table != "order_items" and latest_updated_at:
            write_minio_watermark(
                minio_client,
                bucket,
                source="postgres",
                object_name=table,
                field="updated_at",
                value=latest_updated_at.isoformat(),
                file_number=file_number - 1
            )

            logger.info(
                f"Updated watermark for {table}: {latest_updated_at.isoformat()} | "
                f"file_number={file_number - 1}"
            )

        logger.info(
            f"Completed table '{table}' | "
            f"Rows={table_rows:,} | "
            f"Files={file_number - 1}"
        )

    postgres_conn.close()
    elapsed = round(time.time() - start_time, 2)

    logger.info("=" * 60)
    logger.info("POSTGRES EXTRACTION COMPLETED")
    logger.info(f"Tables Processed : {len(POSTGRES_CONFIG['tables'])}")
    logger.info(f"Total Rows       : {general_total_rows:,}")
    logger.info(f"Total Files      : {total_files}")
    logger.info(f"Execution Time   : {elapsed} seconds")
    logger.info("=" * 60)



# =====================================================================
# DATA SOURCE 2: MongoDB
# =====================================================================
def mongodb_extraction():
    """
    Extract data incrementally from MongoDB collections and store it in MinIO
    as compressed Parquet files.

    Steps:
    - Connect to the configured MongoDB database.
    - Read the last processed ObjectId (watermark) for each collection.
    - Query only documents newer than the stored watermark.
    - Process documents in batches for memory-efficient extraction.
    - Convert each batch to a Parquet table with Snappy compression.
    - Upload batch files to the appropriate raw/ MongoDB path in MinIO.
    - Upload any remaining documents that do not fill a complete batch.
    - Update the collection watermark with the latest extracted ObjectId.
    - Log per-collection and overall extraction statistics.
    - Close database connections and release resources.
    """

    start_time = time.time()

    mongo_client = MongoClient(os.getenv("MONGODB_URI"))
    database = mongo_client[os.getenv("MONGODB_DATABASE")]
    logger.info("Connected to MongoDB.")

    total_documents = 0
    total_files = 0

    for collection_name in MONGODB_CONFIG["collections"]:
        logger.info(f"Starting extraction for collection '{collection_name}'")
        watermark, last_file_number = read_minio_watermark(
                minio_client,
                bucket,
                source="mongodb",
                object_name=collection_name,
                field="last_loaded_object_id"
            )

        collection = database[collection_name]

        query = {}
        if watermark:
            query["_id"] = {"$gt": ObjectId(watermark)}
        cursor = (
            collection.find(query, no_cursor_timeout=True)
            .sort("_id", 1)
            .batch_size(CHUNK_SIZE)
        )
        file_number = last_file_number + 1
        datetimestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]

        collection_documents = 0
        batch = []
        latest_object_id = watermark

        for document in cursor:
            if latest_object_id is None or document["_id"] > ObjectId(latest_object_id):
                latest_object_id = str(document["_id"])

            document["_id"] = str(document["_id"])
            batch.append(document)

            if len(batch) == CHUNK_SIZE:
                df, buffer= records_to_parquet_buffer(batch)

                object_name = (f"raw/mongodb/{collection_name}/{collection_name}_{datetimestamp}_{file_number}.parquet")

                upload_to_minio(
                    minio_client,
                    bucket,
                    object_name,
                    buffer
                )
                logger.info(
                    f"{collection_name} | "
                    f"file={file_number} | "
                    f"documents={len(batch):,} | "
                    f"size={buffer.getbuffer().nbytes:,} bytes"
                )

                total_documents += len(batch)
                collection_documents += len(batch)
                total_files += 1

                file_number += 1
                batch = []
        #
        # Remaining documents
        #
        if batch:
            df, buffer= records_to_parquet_buffer(batch)

            object_name = (
                f"raw/mongodb/{collection_name}/"
                f"{collection_name}_{datetimestamp}_{file_number}.parquet"
            )
            upload_to_minio(
                minio_client,
                bucket,
                object_name,
                buffer
            )
            logger.info(
                f"{collection_name} | "
                f"file={file_number} | "
                f"documents={len(batch):,} | "
                f"size={buffer.getbuffer().nbytes:,} bytes"
            )

            total_documents += len(batch)
            collection_documents += len(batch)
            total_files += 1

        if latest_object_id and latest_object_id != watermark:
            write_minio_watermark(
                minio_client,
                bucket,
                source="mongodb",
                object_name=collection_name,
                field="last_loaded_object_id",
                value=latest_object_id,
                file_number=file_number - 1
            )

        cursor.close()
        logger.info(
            f"Completed collection '{collection_name}' | "
            f"Documents={collection_documents:,} | "
            f"Files={file_number}"
        )

    mongo_client.close()
    elapsed = round(time.time() - start_time, 2)

    logger.info("=" * 60)
    logger.info("MONGODB EXTRACTION COMPLETED")
    logger.info(f"Collections Processed : {len(MONGODB_CONFIG['collections'])}")
    logger.info(f"Total Documents       : {total_documents:,}")
    logger.info(f"Total Files           : {total_files}")
    logger.info(f"Execution Time        : {elapsed} seconds")
    logger.info("=" * 60)


# =====================================================================
# DATA SOURCE 3: FastAPI
# =====================================================================
def api_extraction():
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
    datetimestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]

    logger.info("=" * 80)
    logger.info("Starting SwiftDrop API extraction")
    
    for endpoint in API_CONFIG["endpoints"]:
        table = endpoint["table"]
        logger.info(f"Extracting table: {table}")

        watermark, last_file_number = read_minio_watermark(
            minio_client,
            bucket,
            source="fast_api",
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
            object_name = (f"raw/api/{table}/{table}_{datetimestamp}_{file_number}.parquet")

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
            source="fast_api",
            object_name="shipments",
            field="updated_since",
            value=latest_updated_at,
            file_number=file_number - 1
        )
        logger.info(f"Updated watermark to {latest_updated_at}")
    logger.info("SwiftDrop API extraction completed")
    logger.info("=" * 80)