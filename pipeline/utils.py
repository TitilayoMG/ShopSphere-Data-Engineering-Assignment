# -------------------------
# Imports:  Custom Libraries
# -------------------------
import logging
import os
import psycopg2
from dotenv import load_dotenv
from minio import Minio
import pandas as pd
import io
import json


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
# Minio Connection
# -------------------------
def get_minio_client():
    """
    Create and return a MinIO client and bucket.
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

# -------------------------
# Upload to Minio 
# -------------------------
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


# ============================================================================
# PostgreSQL Connection
# ============================================================================
def get_postgres_connection(prefix: str = "WAREHOUSE"):
    """
    Create a PostgreSQL connection.
    prefix : str
        Environment variable prefix.

        Example:
            WAREHOUSE_POSTGRES_HOST
            WAREHOUSE_POSTGRES_PORT

            SOURCE_POSTGRES_HOST
            SOURCE_POSTGRES_PORT
            ...
    Returns: psycopg2.connection
    """
    return psycopg2.connect(
        host=os.getenv(f"{prefix}_POSTGRES_HOST"),
        port=os.getenv(f"{prefix}_POSTGRES_PORT"),
        database=os.getenv(f"{prefix}_POSTGRES_DB"),
        user=os.getenv(f"{prefix}_POSTGRES_USER"),
        password=os.getenv(f"{prefix}_POSTGRES_PASSWORD"),
    )

# ============================================================================
# Parquet Utilities
# ============================================================================
def records_to_parquet_buffer(records):
    df = pd.json_normalize(records, sep="_")
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False)
    buffer.seek(0)
    return df, buffer

def parquet_buffer_to_dataframe(buffer):
    buffer.seek(0)
    return pd.read_parquet(buffer)

# ============================================================================
# Minio Watermark Utilities
# ============================================================================
def read_minio_watermark(client, bucket, source, object_name, field):
    """
    Read the watermark metadata for a specific source and object from the
    MinIO watermark file.

    Args:
        client: MinIO client.
        bucket: MinIO bucket name.
        source: Data source (e.g. postgres, fast_api, mongodb).
        object_name: Table or collection name.
        field: Name of the watermark field
               (e.g. updated_at, updated_since, last_loaded_object_id).

    Returns:
        tuple:
            (
                watermark_value,
                last_file_number
            )

        Returns (None, None) if the metadata does not exist.
    """
    watermark_file = "metadata/shopsphere_watermark.json"

    try:
        response = client.get_object(bucket, watermark_file)
        data = json.loads(response.read().decode("utf-8"))
        response.close()
        response.release_conn()

        metadata = (
            data
            .get(source, {})
            .get(object_name, {})
        )

        return (
            metadata.get(field),
            metadata.get("file_number")
        )

    except Exception as e:
        logger.exception(f"Failed to read watermark file: {e}")
        raise


def write_minio_watermark(
    client,
    bucket,
    source,
    object_name,
    field,
    value,
    file_number
):
    """
    Update the watermark metadata for a specific source and object in MinIO.
    """
    watermark_file = "metadata/shopsphere_watermark.json"

    try:
        try:
            response = client.get_object(bucket, watermark_file)
            data = json.loads(response.read().decode("utf-8"))
            response.close()
            response.release_conn()
        except Exception:
            data = {}

        data.setdefault(source, {})
        data[source].setdefault(object_name, {})

        data[source][object_name][field] = value
        data[source][object_name]["file_number"] = file_number

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

        logger.info(
            f"Updated watermark for {source}/{object_name}"
        )

    except Exception as e:
        logger.exception(f"Failed to update watermark: {e}")
        raise


# ============================================================================
# Pipeline Run Helpers
# ============================================================================
def start_pipeline_run(cursor, pipeline_name, source_name, watermark_value=None):
    """
    
    """
    cursor.execute("""
        INSERT INTO control.pipeline_runs
        (pipeline_name, source_name, started_at, status,
         records_extracted, records_loaded, watermark_value)
        VALUES (%s,%s,NOW(),'started',0,0,%s)
        RETURNING run_id
    """, (pipeline_name, source_name, watermark_value))
    return cursor.fetchone()[0]


def mark_pipeline_success(cursor, run_id, records_loaded):
    """
    Mark a pipeline run as successful.
    conn : psycopg2.connection
    run_id : int
    """
    cursor.execute("""
        UPDATE control.pipeline_runs
        SET completed_at=NOW(),
            status='success',
            records_loaded=%s
        WHERE run_id=%s
    """, (records_loaded, run_id))


def mark_pipeline_fail(cursor, run_id, error_message):
    """
    Mark a pipeline run as failed.
    """
    cursor.execute("""
        UPDATE control.pipeline_runs
        SET completed_at=NOW(),
            status='failed',
            error_message=%s
        WHERE run_id=%s
    """, (str(error_message), run_id))


def read_pipeline_runs(cursor, pipeline_name, source_name, watermark):
    cursor.execute("""
        SELECT 1
        FROM control.pipeline_runs
        WHERE pipeline_name = %s
          AND source_name = %s
          AND watermark_value = %s
          AND status = 'success'
        LIMIT 1
    """, (pipeline_name, source_name, watermark))

    return cursor.fetchone() is not None


def update_pipeline_watermark(cursor, pipeline_name, source_name, watermark_column, watermark_value):
    cursor.execute("""
        INSERT INTO control.pipeline_watermarks
        (pipeline_name, source_name, watermark_column,
         watermark_value, updated_at)
        VALUES (%s,%s,%s,%s,NOW())
        ON CONFLICT (pipeline_name, source_name)
        DO UPDATE SET
            watermark_column = EXCLUDED.watermark_column,
            watermark_value = EXCLUDED.watermark_value,
            updated_at = NOW();
    """, (pipeline_name, source_name, watermark_column, watermark_value))





