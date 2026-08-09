# -------------------------
# Import: Standard Libraries
# -------------------------
import csv
import io
import logging
from pathlib import Path
import json 
import pandas as pd


from pipeline.utils import (
    get_minio_client,
    get_postgres_connection,
    mark_pipeline_fail,
    mark_pipeline_success,
    read_pipeline_runs,
    start_pipeline_run,
    update_pipeline_watermark,
    load_config
)

# -------------------------
# LOGGING
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# ============================================================================
# Load All Tables in Minio processed/ path
# ============================================================================
DATA_SOURCES, CHUNK_SIZE = load_config()


def load_shipments_data():
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
    client, bucket = get_minio_client()

    conn = get_postgres_connection("WAREHOUSE")
    cursor = conn.cursor()

    try:
        # for source_name, source_config in DATA_SOURCES.items():

        for source_config in DATA_SOURCES['fast_api']['datasets']:
            table_name = source_config['table']
            primary_keys = source_config["primary_key"]
            source_name = 'swiftdrop'
            prefix = f"processed/{source_name}/{table_name}/"

            objects = client.list_objects(
                bucket,
                prefix=prefix,
                recursive=True,
            )
            files_loaded = 0
            rows_loaded = 0

            for parquet_file in objects:
                object_name = parquet_file.object_name
                if object_name.endswith("/") or not object_name.endswith(".parquet"):
                    continue

                parts = object_name.split("/")
                if len(parts) < 4:
                    continue

                filename = parts[-1]
                pipeline_name = source_name + "_" + table_name
                watermark = filename.replace(".parquet", "").replace(f"{table_name}_", "")

                already_processed = read_pipeline_runs(
                    cursor,
                    pipeline_name,
                    source_name,
                    watermark,
                )

                if already_processed:
                    # logger.info(f"Skipping previously loaded file: {filename}")
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
                        COPY public.stg_{source_name}_{table_name}
                        ({columns})
                        FROM STDIN
                        WITH (
                            FORMAT CSV,
                            NULL '\\N'
                        )
                        """,
                        csv_buffer
                    )
                    cursor.execute(
                        f"""
                        INSERT INTO public.{source_name}_{table_name} ({columns})
                        SELECT {columns}
                        FROM public.stg_{source_name}_{table_name}
                        ON CONFLICT ({", ".join(primary_keys)}) DO NOTHING;
                        """
                    )

                    cursor.execute(f"TRUNCATE TABLE public.stg_{source_name}_{table_name};")
                    
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
                    files_loaded += 1
                    rows_loaded += len(df)
                    logger.info(f"✓ {len(df)} rows from {filename} into {table_name}")

                except Exception as e:
                    conn.rollback()
                    mark_pipeline_fail(
                        cursor,
                        run_id,
                        str(e)
                    )
                    conn.commit()

                    logger.exception(f"Failed loading {filename}")
                    raise
            logger.info(
                f"{table_name}: {files_loaded} files | "
                f"{rows_loaded:,} rows | "
            )
    except Exception:
        conn.rollback()
        logger.exception("Loading failed")
        raise

    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    load_shipments_data()