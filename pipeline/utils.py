# -------------------------
# Imports:  Custom Libraries
# -------------------------
import logging
import os
from dotenv import load_dotenv
from minio import Minio


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
