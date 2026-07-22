
import json
import uuid
import time
import hashlib
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import psycopg2
import psycopg2.extras
from minio.error import S3Error
from dotenv import load_dotenv

from pipeline.utils import (
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


def extract_postgres():
    start_time = time.time()
    pipeline_run_id = str(uuid.uuid4())
    logger.info(f"Starting pipeline run {pipeline_run_id}")

    config = load_config()
    chunk_size = config.get("chunk_size", 100)
    tables = config.get("data_sources", {}).get("postgres", {}).get("tables", [])

    pg_conn = get_postgres_connection(prefix="SOURCE")
    minio_client, bucket = get_minio_client()

    # Verify the target bucket exists
    try:
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)
            logger.info(f"Created bucket {bucket}")
        else:
            logger.info(f"Bucket {bucket} exists")
    except S3Error as e:
        logger.exception(f"Failed to verify/create bucket: {e}")
        raise

    metrics = {
        "tables_processed": 0,
        "general_total_rows": 0,
        "total_files": 0,
        "tables_failed": [],
        "tables_skipped": [],
        "execution_time": 0,
    }

    for table in tables:
        logger.info(f"Processing table: {table}")
        cursor = None
        try:
            # --- Validate the table ---
            validate_cursor = pg_conn.cursor()
            validate_cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_name = %s
                )
                """,
                (table,),
            )
            table_exists = validate_cursor.fetchone()[0]

            if not table_exists:
                logger.warning(f"Table {table} does not exist or is not accessible. Skipping.")
                metrics["tables_skipped"].append(table)
                validate_cursor.close()
                continue

            validate_cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name = %s AND column_name = 'updated_at'
                )
                """,
                (table,),
            )
            has_updated_at = validate_cursor.fetchone()[0]
            validate_cursor.close()

            if not has_updated_at:
                logger.warning(f"Table {table} has no 'updated_at' column. Skipping.")
                metrics["tables_skipped"].append(table)
                continue

            # --- Retrieve table metadata ---
            meta_cursor = pg_conn.cursor()
            meta_cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
                """,
                (table,),
            )
            columns = [row[0] for row in meta_cursor.fetchall()]
            meta_cursor.close()

            # --- Determine extraction strategy ---
            try:
                watermark_value, last_file_number = read_minio_watermark(
                    minio_client, bucket, "postgres", table, "updated_at"
                )
            except Exception:
                watermark_value, last_file_number = None, None

            incremental = False
            if watermark_value:
                try:
                    if isinstance(watermark_value, str):
                        datetime.fromisoformat(watermark_value)
                    incremental = True
                except (ValueError, TypeError):
                    logger.warning(
                        f"Invalid watermark format for {table}. Falling back to full extraction."
                    )
                    incremental = False

            if incremental:
                query = f"""
                    SELECT * FROM {table}
                    WHERE updated_at > %s
                    ORDER BY updated_at
                """
                query_params = (watermark_value,)
                logger.info(f"Incremental extraction for {table} since {watermark_value}")
            else:
                query = f"SELECT * FROM {table} ORDER BY updated_at"
                query_params = None
                logger.info(f"Full extraction for {table}")

            # --- Initialize extraction metadata ---
            extraction_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")[:-3]
            file_number = (last_file_number + 1) if (incremental and last_file_number) else 1

            table_rows = 0
            latest_updated_at = watermark_value
            files_created = 0

            cursor = pg_conn.cursor(
                name=f"cursor_{table}_{pipeline_run_id}",
                cursor_factory=psycopg2.extras.RealDictCursor,
            )
            if query_params:
                cursor.execute(query, query_params)
            else:
                cursor.execute(query)
            cursor.itersize = chunk_size

            while True:
                rows = cursor.fetchmany(chunk_size)

                if not rows:
                    break

                records = [dict(row) for row in rows]
                df = pd.DataFrame.from_records(records)

                if "updated_at" in df.columns:
                    chunk_latest_updated_at = df["updated_at"].max()
                    if latest_updated_at is None or chunk_latest_updated_at > latest_updated_at:
                        latest_updated_at = chunk_latest_updated_at

                _, parquet_buffer = records_to_parquet_buffer(records)

                if parquet_buffer.getbuffer().nbytes == 0:
                    logger.error(f"Empty parquet buffer for {table}. Skipping chunk.")
                    continue

                object_name = (
                    f"raw/postgres/{table}/{table}_{extraction_timestamp}_{file_number}.parquet"
                )

                expected_prefix = f"raw/postgres/{table}/"
                if not object_name.startswith(expected_prefix):
                    logger.error(f"Invalid object path generated: {object_name}. Skipping chunk.")
                    continue

                parquet_buffer.seek(0)
                local_checksum = hashlib.md5(parquet_buffer.read()).hexdigest()
                parquet_buffer.seek(0)

                upload_to_minio(
                    client=minio_client,
                    bucket=bucket,
                    object_name=object_name,
                    buffer=parquet_buffer,
                    content_type="application/octet-stream",
                )

                try:
                    stat = minio_client.stat_object(bucket, object_name)
                    remote_etag = stat.etag.strip('"') if stat.etag else None
                    if remote_etag and remote_etag != local_checksum:
                        logger.warning(
                            f"Checksum mismatch for {object_name}: "
                            f"local={local_checksum} remote={remote_etag}"
                        )
                except S3Error as e:
                    logger.exception(f"Failed to verify upload for {object_name}: {e}")
                    raise

                chunk_row_count = len(records)
                table_rows += chunk_row_count
                metrics["general_total_rows"] += chunk_row_count
                metrics["total_files"] += 1
                files_created += 1
                file_number += 1

                logger.info(f"Uploaded {object_name} with {chunk_row_count} rows")

            cursor.close()
            cursor = None

            if table_rows > 0:
                write_minio_watermark(
                    client=minio_client,
                    bucket=bucket,
                    source="postgres",
                    object_name=table,
                    field="updated_at",
                    value=str(latest_updated_at),
                    file_number=file_number - 1,
                )
            else:
                logger.info(f"No new records for {table}. Watermark unchanged.")

            metrics["tables_processed"] += 1
            logger.info(
                f"Completed extraction for {table}: {table_rows} rows, {files_created} files"
            )

        except Exception as e:
            logger.exception(f"Failed to process table {table}: {e}")
            metrics["tables_failed"].append(table)
            if cursor is not None:
                try:
                    cursor.close()
                except Exception:
                    pass
            continue

    # --- Complete extraction ---
    pg_conn.close()

    execution_time = time.time() - start_time
    metrics["execution_time"] = execution_time

    if metrics["tables_failed"]:
        pipeline_status = "partial_failure" if metrics["tables_processed"] > 0 else "failure"
    else:
        pipeline_status = "success"

    pipeline_metadata = {
        "pipeline_run_id": pipeline_run_id,
        "pipeline_status": pipeline_status,
        "start_time": datetime.fromtimestamp(start_time).isoformat(),
        "execution_time": execution_time,
        "tables_processed": metrics["tables_processed"],
        "tables_failed": metrics["tables_failed"],
        "tables_skipped": metrics["tables_skipped"],
        "general_total_rows": metrics["general_total_rows"],
        "total_files": metrics["total_files"],
    }

    logger.info(
        f"Pipeline run {pipeline_run_id} completed: {json.dumps(pipeline_metadata, indent=2)}"
    )

    return pipeline_metadata


if __name__ == "__main__":
    main()