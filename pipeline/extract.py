# -------------------------
# imports: Standard Libraries
# -------------------------
import io
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
import pandas as pd
import requests
from bson import ObjectId

# -------------------------
# Imports:  Custom Libraries
# -------------------------
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error
from pymongo import MongoClient

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
# COMMON UTILITIES
# -------------------------
def get_minio_client():
    """
    Create and return a MinIO client.
    """
    client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true"
    )
    logger.info("Connected to MinIO.")
    bucket = os.getenv("MINIO_BUCKET")
    return client, bucket


def upload_to_minio(
    client,
    bucket,
    object_name,
    buffer,
    content_type="application/octet-stream"
):
    """
    Upload an in-memory file to MinIO.
    """
    buffer.seek(0)
    client.put_object(
        bucket_name=bucket,
        object_name=object_name,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type=content_type
    )

def _read_watermark(client, bucket, source, object_name, field):
    watermark_file = "metadata/shopsphere_watermark.json"
    try:
        response = client.get_object(bucket, watermark_file)
        data = json.loads(response.read().decode("utf-8"))
        response.close()
        response.release_conn()

        return (
            data
            .get(source, {})
            .get(object_name, {})
            .get(field)
        )
    except S3Error as e:
        if e.code == "NoSuchKey":
            return None
        raise

def _write_watermark(client, bucket, source, object_name, field, value):
    watermark_file = "metadata/shopsphere_watermark.json"
    try:
        response = client.get_object(bucket, watermark_file)
        data = json.loads(response.read().decode("utf-8"))
        response.close()
        response.release_conn()

    except S3Error as e:
        if e.code == "NoSuchKey":
            raise

    data[source][object_name][field] = value
    buffer = io.BytesIO(
        json.dumps(data, indent=4).encode("utf-8")
    )
    upload_to_minio(
        client=client,
        bucket=bucket,
        object_name=watermark_file,
        buffer=buffer,
        content_type="application/json"
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
    start_time = time.time()

    postgres_conn = psycopg2.connect(
        host=os.getenv("SOURCE_POSTGRES_HOST"),
        port=os.getenv("SOURCE_POSTGRES_PORT"),
        dbname=os.getenv("SOURCE_POSTGRES_DB"),
        user=os.getenv("SOURCE_POSTGRES_USER"),
        password=os.getenv("SOURCE_POSTGRES_PASSWORD")
    )
    logger.info("Connected to PostgreSQL.")

    total_rows = 0
    total_files = 0

    for table in POSTGRES_CONFIG["tables"]:
        logger.info(f"Starting extraction for table '{table}'")
       
        cursor = postgres_conn.cursor()
        cursor.execute(f"SELECT * FROM {table} LIMIT 0")
        columns = [col[0] for col in cursor.description]
        cursor.close()

        watermark = None
        latest_updated_at = None
        
        cursor = postgres_conn.cursor(name=f"{table}_cursor")
        cursor.itersize = CHUNK_SIZE

        if table == "order_items":
            logger.info(
                "order_items has no updated_at column. Performing full extraction."
            )
            cursor.execute(f"SELECT * FROM {table}")

        else:
            watermark = _read_watermark(
                minio_client,
                bucket,
                source="postgres",
                object_name=table,
                field="updated_at"      
            )
            if watermark:
                logger.info(
                    f"Incremental extraction using watermark: {watermark}"
                )
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
                logger.info(
                    "No watermark found. Performing full extraction."
                )
                cursor.execute(
                    f"""
                    SELECT *
                    FROM {table}
                    ORDER BY updated_at
                    """
                )

        datetimestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]

        file_number = 1
        table_rows = 0

        while True:
            rows = cursor.fetchmany(CHUNK_SIZE)
            if not rows:
                break

            table_rows += len(rows)
            total_rows += len(rows)

            # arrow_table = pa.Table.from_pylist([dict(zip(columns, row)) for row in rows])
            records = [dict(zip(columns, row)) for row in rows]
            if table != "order_items":
                updated_index = columns.index("updated_at")
                page_latest = max(
                    row[updated_index]
                    for row in rows
                    if row[updated_index] is not None
                )
                if (
                    latest_updated_at is None
                    or page_latest > latest_updated_at
                ):
                    latest_updated_at = page_latest

            arrow_table = pa.Table.from_pylist(records)
            buffer = io.BytesIO()

            pq.write_table(
                arrow_table,
                buffer,
                compression="snappy"
            )

            object_name = (
                f"raw/{table}/"
                f"{table}_{datetimestamp}_file{file_number}.parquet"
            )

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
            _write_watermark(
                minio_client,
                bucket,
                source="postgres",
                object_name=table,
                field="updated_at",
                value=latest_updated_at.isoformat()
            )
            logger.info(
                f"Updated watermark for {table}: {latest_updated_at.isoformat()}"
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
    logger.info(f"Total Rows       : {total_rows:,}")
    logger.info(f"Total Files      : {total_files}")
    logger.info(f"Execution Time   : {elapsed} seconds")
    logger.info("=" * 60)







# =====================================================================
# DATA SOURCE 2: MongoDB
# =====================================================================
def mongodb_extraction():
    start_time = time.time()

    mongo_client = MongoClient(os.getenv("MONGODB_URI"))
    database = mongo_client[os.getenv("MONGODB_DATABASE")]
    logger.info("Connected to MongoDB.")

    total_documents = 0
    total_files = 0

    for collection_name in MONGODB_CONFIG["collections"]:
        logger.info(f"Starting extraction for collection '{collection_name}'")
        #
        # Safe rerun (full refresh)
        #
        # for obj in minio_client.list_objects(
        #     bucket,
        #     prefix=f"raw/{collection_name}/{collection_name}_",
        #     recursive=True
        # ):
        #     minio_client.remove_object(bucket, obj.object_name)
        
        watermark = _read_watermark(
            minio_client,
            bucket,
            source="mongodb",
            object_name=collection_name,
            field="last_loaded_object_id"
        )

        collection = database[collection_name]
        # cursor = collection.find({}, no_cursor_timeout=True).batch_size(CHUNK_SIZE)

        query = {}
        if watermark:
            query["_id"] = {"$gt": ObjectId(watermark)}
        cursor = (
            collection.find(query, no_cursor_timeout=True)
            .sort("_id", 1)
            .batch_size(CHUNK_SIZE)
        )

        datetimestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
        file_number = 1
        collection_documents = 0
        batch = []
        latest_object_id = watermark

        for document in cursor:
            #
            # Convert ObjectId to string
            #
            
            if latest_object_id is None or document["_id"] > ObjectId(latest_object_id):
                latest_object_id = str(document["_id"])

            document["_id"] = str(document["_id"])
            batch.append(document)

            if len(batch) == CHUNK_SIZE:
                table = pa.Table.from_pylist(batch)
                buffer = io.BytesIO()

                pq.write_table(
                    table,
                    buffer,
                    compression="snappy"
                )

                object_name = (
                    f"raw/{collection_name}/"
                    f"{collection_name}_{datetimestamp}_file{file_number}.parquet"
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

                file_number += 1
                batch = []
        #
        # Remaining documents
        #
        if batch:
            table = pa.Table.from_pylist(batch)
            buffer = io.BytesIO()

            pq.write_table(
                table,
                buffer,
                compression="snappy"
            )

            object_name = (
                f"raw/{collection_name}/"
                f"{collection_name}_{datetimestamp}_file{file_number}.parquet"
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
            _write_watermark(
                minio_client,
                bucket,
                source="mongodb",
                object_name=collection_name,
                field="last_loaded_object_id",
                value=latest_object_id
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
    watermark = _read_watermark(
        minio_client,
        bucket,
        source="fast_api",
        object_name="shipments",
        field="updated_since"
    )

    base_url = os.getenv("MOCK_API_BASE_URL").rstrip("/")

    datetimestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]

    logger.info("=" * 80)
    logger.info("Starting SwiftDrop API extraction")
    logger.info(f"Watermark: {watermark}")

    latest_updated_at = watermark

    for endpoint in API_CONFIG["endpoints"]:
        table = endpoint["table"]
        logger.info(f"Extracting table: {table}")

        page = 1
        file_number = 1
        total_records = 0

        while True:
            params = {}
            if endpoint["paginated"]:
                params["page"] = page
                params["limit"] = 100

                if watermark:
                    params["updated_since"] = watermark

            response = requests.get(
                url=f"{base_url}{endpoint['endpoint']}",
                params=params,
                timeout=60
            )

            response.raise_for_status()
            payload = response.json()

            if endpoint["paginated"]:
                records = payload["shipments"]
            else:
                records = payload

            if not records:
                logger.info(f"No more records found for {table}")
                break

            dataframe = pd.json_normalize(records, sep="_")
            parquet_buffer = io.BytesIO()
            dataframe.to_parquet(
                parquet_buffer,
                engine="pyarrow",
                index=False
            )

            object_name = (
                f"raw/{table}/"
                f"{table}_{datetimestamp}_"
                f"file{file_number}.parquet"
            )

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
                    if (
                        latest_updated_at is None
                        or page_latest > latest_updated_at
                    ):
                        latest_updated_at = page_latest

                next_page = payload.get("next_page")
                if next_page is None:
                    break
                page = next_page
            else:
                break
        logger.info(
            f"{table}: extracted {total_records:,} records"
        )
    if latest_updated_at and latest_updated_at != watermark:
        _write_watermark(
            minio_client,
            bucket,
            source="fast_api",
            object_name="shipments",
            field="updated_since",
            value=latest_updated_at
        )
        logger.info(
            f"Updated watermark to {latest_updated_at}"
        )

    logger.info("SwiftDrop API extraction completed")
    logger.info("=" * 80)



if __name__ == "__main__":
    postgres_extraction()
    mongodb_extraction()
    api_extraction()