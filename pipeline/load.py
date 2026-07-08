# -------------------------
# Import: Standard Libraries
# -------------------------
import io
import logging
import pandas as pd

from utils import get_minio_client, get_postgres_connection

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
conn = get_postgres_connection("WAREHOUSE")

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


# def read_pipeline_watermark(cursor, pipeline_name, source_name):
#     cursor.execute("""
#         SELECT watermark_value
#         FROM control.pipeline_watermarks
#         WHERE pipeline_name=%s
#           AND source_name=%s
#     """, (pipeline_name, source_name))
#     row = cursor.fetchone()
#     return row[0] if row else None

def read_pipeline_watermark(cursor, pipeline_name, source_name, watermark):
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


# ============================================================================
# Load PostgreSQL Tables
# ============================================================================
import csv
import io
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
                if len(parts) < 4:
                    continue

                table_name = parts[2]
                filename = parts[-1]

                pipeline_name = table_name
                source_name = "postgres"

                logger.info(f"Processing {filename}")

                watermark = filename
                filename = parts[-1]

                watermark = filename.replace(".parquet", "").replace(f"{table_name}_", "")

                already_processed = read_pipeline_watermark(
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

                already_processed = read_pipeline_watermark(
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
                    response = client.get_object(
                        bucket,
                        object_name,
                    )

                    parquet_bytes = io.BytesIO(response.read())

                    response.close()
                    response.release_conn()

                    df = pd.read_parquet(parquet_bytes)

                    logger.info(
                        f"Loading {len(df)} rows into public.{table_name}"
                        f"customer_sessions columns are {df.columns}"
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

                    logger.exception(
                        f"Failed loading {filename}"
                    )

                    raise

    finally:
        cursor.close()




if __name__ == "__main__":
    load_postgres()
    # load_mongodb()