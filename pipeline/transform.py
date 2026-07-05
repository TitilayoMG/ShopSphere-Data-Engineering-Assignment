import io
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from minio.error import S3Error
from utils import get_minio_client, upload_to_minio


# -------------------------
# LOGGING
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)




def transform_postgres():
    """
    Read all PostgreSQL parquet files from:
        raw/postgres/<table>/<table_timestamp>.parquet
    Apply table-specific transformations.
    Write transformed files to:
        processed/postgres/<table>/<table_timestamp>.parquet
    """
    logger.info("=" * 60)
    logger.info("Starting PostgreSQL transformation...")

    client, bucket = get_minio_client()
    try:
        objects = client.list_objects(
            bucket_name=bucket,
            prefix="raw/postgres/",
            recursive=True
        )

        processed_files = 0

        for obj in objects:
            object_name = obj.object_name

            # Ignore directories and non-parquet files
            if object_name.endswith("/") or not object_name.endswith(".parquet"):
                continue
            logger.info(f"Reading {object_name}")

            try:
                # ---------------------------------------------------------
                # Read parquet from MinIO
                # ---------------------------------------------------------
                response = client.get_object(bucket, object_name)
                buffer = io.BytesIO(response.read())
                df = pd.read_parquet(buffer)

                response.close()
                response.release_conn()
                # ---------------------------------------------------------
                # Determine table name
                #
                # raw/postgres/products/products_20260705.parquet
                #                 ^
                # ---------------------------------------------------------
                parts = object_name.split("/")

                if len(parts) < 4:
                    logger.warning(f"Skipping invalid path: {object_name}")
                    continue

                table_name = parts[2]

                logger.info(f"Transforming table '{table_name}'")

                # =========================================================
                # PRODUCTS
                # =========================================================
                if table_name == "products":

                    if "brand" in df.columns:

                        null_count = df["brand"].isna().sum()

                        if null_count > 0:
                            logger.info(
                                f"Found {null_count} NULL brand values. "
                                "Replacing with 'Unknown'."
                            )

                        df["brand"] = df["brand"].fillna("Unknown")

                # =========================================================
                # Other PostgreSQL tables
                # =========================================================
                else:
                    logger.info(
                        f"No transformation configured for '{table_name}'. "
                        "Copying unchanged."
                    )

                # ---------------------------------------------------------
                # Convert dataframe to parquet
                # ---------------------------------------------------------
                output_buffer = io.BytesIO()
                table = pa.Table.from_pandas(df)
                pq.write_table(table, output_buffer)

                # ---------------------------------------------------------
                # Destination path
                #
                # raw/postgres/products/file.parquet
                #
                # ->
                #
                # processed/file.parquet
                # ---------------------------------------------------------
                filename = object_name.split("/")[-1]
                destination = f"processed/{filename}"

                upload_to_minio(
                    client=client,
                    bucket=bucket,
                    object_name=destination,
                    buffer=output_buffer,
                    content_type="application/octet-stream"
                )
                logger.info(f"Saved transformed file -> {destination}")

                processed_files += 1

            except Exception as e:
                logger.exception(
                    f"Failed transforming {object_name}: {e}"
                )
        logger.info("=" * 60)
        logger.info(
            f"PostgreSQL transformation completed. "
            f"{processed_files} file(s) processed."
        )
        logger.info("=" * 60)

    except S3Error as e:
        logger.exception(f"MinIO error: {e}")

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")


if __name__ == "__main__":
    transform_postgres()