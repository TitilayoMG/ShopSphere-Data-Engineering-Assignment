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


# def download_dataframe(object_name: str):
#     """
#     Download a Parquet file from MinIO.
#     Returns
#     -------
#     pandas.DataFrame
#     """
#     response = client.get_object(bucket, object_name)

#     try:
#         data = io.BytesIO(response.read())
#         return pd.read_parquet(data)
#     finally:
#         response.close()
#         response.release_conn()

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
# Minio Watermark
# ============================================================================
def read_minio_watermark(client, bucket, source, object_name, field):
    """
    Reads the last processed watermark value for a specific source, object, and field
    from the watermark metadata file stored in MinIO. 
    Returns the stored watermark if it exists, 
    returns None if the metadata file is missing, and raise
    any unexpected storage errors.
    """
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

    except Exception as e:
        logger.exception(f"No such key: {e}")
        raise

def write_minio_watermark(client, bucket, source, object_name, field, value):
    """
    Update the pipeline watermark metadata by loading the existing watermark file,
    modifying the specified tracking field for a given data source and object,
    then writing the updated JSON back to MinIO. 
    """
    watermark_file = "metadata/shopsphere_watermark.json"
    try:
        response = client.get_object(bucket, watermark_file)
        data = json.loads(response.read().decode("utf-8"))
        response.close()
        response.release_conn()

    except Exception as e:
        logger.exception(f"No such key: {e}")
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








