# -------------------------
# Import: Standard Libraries
# -------------------------
import io
import csv
import logging
import pandas as pd

from utils import (get_minio_client, 
                   get_postgres_connection,
                   start_pipeline_run,
                   mark_pipeline_fail,
                   mark_pipeline_success,
                   read_pipeline_runs,
                   update_pipeline_watermark
)

# -------------------------
# LOGGING
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------------
# Creating Connections
# -------------------------
client, bucket = get_minio_client()


# ============================================================================
# Load PostgreSQL Tables
# ============================================================================
LOAD_ORDER = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
]

def load_postgres():
    """
    Load processed Parquet files into the PostgreSQL warehouse.

    Workflow
    --------
    1. Connect to PostgreSQL.
    2. Start pipeline run.
    3. List processed Parquet files.
    4. Skip previously loaded files.
    5. Load new files.
    6. Update watermark after successful load.
    7. Complete pipeline run.
    8. Roll back on failure.
    """
    conn = get_postgres_connection("WAREHOUSE")

    cursor = conn.cursor()

    try:
        for table_name in LOAD_ORDER:
            objects = client.list_objects(
                bucket,
                prefix=f"processed/postgres/{table_name}/",
                recursive=True,
            )

            for obj in objects:
                object_name = obj.object_name
                if object_name.endswith("/") or not object_name.endswith(".parquet"):
                    continue

                parts = object_name.split("/")
                # if len(parts) < 4:
                #     continue

                table_name = parts[2]
                filename = parts[-1]
                pipeline_name = table_name
                source_name = "postgres"

                logger.info(f"Processing {filename}")

                watermark = filename
                filename = parts[-1]

                watermark = filename.replace(".parquet", "").replace(f"{table_name}_", "")

                already_processed = read_pipeline_runs(
                    cursor,
                    pipeline_name,
                    source_name,
                    watermark,
                )

                if already_processed:
                    logger.info(f"Skipping previously loaded file: {filename}")
                    continue

                run_id = start_pipeline_run(
                    cursor,
                    pipeline_name,
                    source_name,
                    watermark
                )
                conn.commit()

                try:
                    response = client.get_object(bucket, object_name)
                    parquet_bytes = io.BytesIO(response.read())
                    response.close()
                    response.release_conn()

                    df = pd.read_parquet(parquet_bytes)

                    logger.info(
                        f"Loading {len(df)} rows into shopsphere_warehouse.{table_name}"
                    )

                    # Convert DataFrame to CSV in memory
                    csv_buffer = io.StringIO()

                    df.to_csv(
                        csv_buffer,
                        index=False,
                        header=False,
                        quoting=csv.QUOTE_MINIMAL,
                        na_rep="\\N"
                    )

                    csv_buffer.seek(0)

                    columns = ", ".join(df.columns)

                    cursor.copy_expert(
                        f"""
                        COPY public.{table_name}
                        ({columns})
                        FROM STDIN
                        WITH (
                            FORMAT CSV,
                            NULL '\\N'
                        )
                        """,
                        csv_buffer
                    )

                    mark_pipeline_success(
                        cursor,
                        run_id,
                        len(df)
                    )

                    update_pipeline_watermark(
                        cursor,
                        pipeline_name,
                        source_name,
                        "file_timestamp",
                        watermark
                    )

                    conn.commit()

                    logger.info(
                        f"Successfully loaded {len(df)} rows from {filename} into {table_name}"
                    )

                except Exception as e:
                    conn.rollback()

                    mark_pipeline_fail(
                        cursor,
                        run_id,
                        str(e)
                    )

                    conn.commit()

                    logger.exception(
                        f"Failed loading {filename}"
                    )
                    raise
    except Exception:
        conn.rollback()
        logger.exception("API load failed")
        raise

    finally:
        cursor.close()
        conn.close()


# ============================================================================
# Load MongoDB Tables
# ============================================================================
def load_mongodb():
    """
    Load processed MongoDB Parquet files into the PostgreSQL warehouse.
    Workflow
    --------
    1. Read processed MongoDB Parquet files from MinIO.
    2. Skip previously loaded files using pipeline watermarks.
    3. Record pipeline runs.
    4. Load new data into PostgreSQL.
    5. Update watermark only after a successful load.
    6. Roll back on failure.
    """
    LOAD_ORDER = [
        "customer_sessions",
        "product_reviews",
    ]
    conn = get_postgres_connection("WAREHOUSE")
    cursor = conn.cursor()

    try:
        for table_name in LOAD_ORDER:

            objects = client.list_objects(
                bucket,
                prefix=f"processed/mongodb/{table_name}/",
                recursive=True,
            )

            for obj in objects:
                object_name = obj.object_name
                if object_name.endswith("/") or not object_name.endswith(".parquet"):
                    continue

                parts = object_name.split("/")
                filename = parts[-1]
                pipeline_name = table_name
                source_name = "mongodb"

                logger.info(f"Processing {filename}")

                watermark = filename
                already_processed = read_pipeline_runs(
                    cursor,
                    pipeline_name,
                    source_name,
                    watermark,
                )

                if already_processed:
                    logger.info(f"Skipping previously loaded file: {filename}")
                    continue

                run_id = start_pipeline_run(
                    cursor,
                    pipeline_name,
                    source_name,
                    watermark,
                )
                conn.commit()

                try:
                    response = client.get_object(bucket, object_name)
                    parquet_bytes = io.BytesIO(response.read())

                    response.close()
                    response.release_conn()

                    df = pd.read_parquet(parquet_bytes)
                    logger.info(
                        f"Loading {len(df)} rows into public.{table_name}"
                    )

                    csv_buffer = io.StringIO()
                    df.to_csv(
                        csv_buffer,
                        index=False,
                        header=False,
                        quoting=csv.QUOTE_MINIMAL,
                        na_rep="\\N",
                    )

                    csv_buffer.seek(0)
                    columns = ", ".join(df.columns)
                    cursor.copy_expert(
                        f"""
                        COPY public.{table_name}
                        ({columns})
                        FROM STDIN
                        WITH (
                            FORMAT CSV,
                            NULL '\\N'
                        )
                        """,
                        csv_buffer,
                    )

                    mark_pipeline_success(
                        cursor,
                        run_id,
                        len(df),
                    )

                    update_pipeline_watermark(
                        cursor,
                        pipeline_name,
                        source_name,
                        "filename",
                        watermark,
                    )
                    conn.commit()

                    logger.info(
                        f"Successfully loaded {len(df)} rows "
                        f"from {filename} into {table_name}"
                    )

                except Exception as e:
                    conn.rollback()
                    mark_pipeline_fail(
                        cursor,
                        run_id,
                        str(e),
                    )
                    conn.commit()
                    logger.exception(f"Failed loading {filename}")
                    raise

    finally:
        cursor.close()



def load_api():
    """
    Load processed API parquet files into PostgreSQL warehouse.
    Load order:
    1. carriers
    2. shipments
    Uses PostgreSQL COPY for bulk loading.
    """
    LOAD_ORDER = [
        "carriers",
        "shipments",
    ]
    conn = get_postgres_connection("WAREHOUSE")
    cursor = conn.cursor()
    
    try:
        for table_name in LOAD_ORDER:
            logger.info(f"Loading API table '{table_name}'")
            objects = client.list_objects(
                bucket,
                prefix=f"processed/api/{table_name}/",
                recursive=True,
            )

            for obj in objects:
                object_name = obj.object_name
                if object_name.endswith("/") or not object_name.endswith(".parquet"):
                    continue

                parts = object_name.split("/")
                filename = parts[-1]
                pipeline_name = table_name
                source_name = "api"

                logger.info(f"Processing {filename}")

                watermark = filename
                already_processed = read_pipeline_runs(
                    cursor,
                    pipeline_name,
                    source_name,
                    watermark,
                )

                if already_processed:
                    logger.info(f"Skipping previously loaded file: {filename}")
                    continue

                run_id = start_pipeline_run(
                    cursor,
                    pipeline_name,
                    source_name,
                    watermark,
                )
                conn.commit()

                try:
                    response = client.get_object(bucket, object_name)
                    parquet_bytes = io.BytesIO(response.read())

                    response.close()
                    response.release_conn()

                    df = pd.read_parquet(parquet_bytes)
                    if df.empty:
                        logger.warning(
                            f"Skipping empty file {filename}"
                        )
                        continue
                    logger.info(
                        f"Loading {len(df)} rows into public.{table_name}"
                    )

                    csv_buffer = io.StringIO()
                    df.to_csv(
                        csv_buffer,
                        index=False,
                        header=False,
                        quoting=csv.QUOTE_MINIMAL,
                        na_rep="\\N",
                    )
                    
                    csv_buffer.seek(0)
                    columns = ", ".join(df.columns)
                    cursor.copy_expert(
                        f"""
                        COPY public.{table_name}
                        ({columns})
                        FROM STDIN
                        WITH (
                            FORMAT CSV,
                            NULL '\\N'
                        )
                        """,
                        csv_buffer,
                    )

                    mark_pipeline_success(
                        cursor,
                        run_id,
                        len(df),
                    )

                    update_pipeline_watermark(
                        cursor,
                        pipeline_name,
                        source_name,
                        "filename",
                        watermark,
                    )
                    conn.commit()

                    logger.info(
                        f"Successfully loaded {len(df)} rows "
                        f"from {filename} into {table_name}"
                    )

                except Exception:
                    conn.rollback()
                    logger.exception("API load failed")
                    raise

    finally:
        cursor.close()

    logger.info(
        "API tables loaded successfully"
    )


if __name__ == "__main__":
    load_postgres()
    load_mongodb()
    load_api()