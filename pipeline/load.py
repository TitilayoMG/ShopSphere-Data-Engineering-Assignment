


# """
# Load transformed Parquet files from MinIO into the PostgreSQL warehouse.

# Features
# --------
# - Generic PostgreSQL connection (supports multiple PostgreSQL instances via env vars)
# - Reads processed Parquet files from MinIO
# - Tracks pipeline runs
# - Uses watermark table for idempotent loading
# - Safe to rerun
# - Transaction rollback on failure
# """

# import logging
# import pandas as pd
# from psycopg2.extras import execute_values

# from utils import get_minio_client, get_postgres_connection
# # ============================================================================
# # Logging
# # ============================================================================
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s | %(levelname)s | %(message)s"
# )
# logger = logging.getLogger(__name__)

# # ============================================================================
# # MinIO
# # ============================================================================
# client, bucket = get_minio_client()

# # ============================================================================
# # Pipeline Run Helpers
# # ============================================================================

# def start_pipeline_run(conn, pipeline_name: str) -> int:
#     """
#     Start a pipeline run.

#     Parameters
#     ----------
#     conn : psycopg2.connection
#     pipeline_name : str

#     Returns
#     -------
#     int
#         Pipeline run ID.
#     """
#     with conn.cursor() as cur:
#         cur.execute(
#             """
#             INSERT INTO control.pipeline_runs
#             (
#                 pipeline_name,
#                 started_at,
#                 status
#             )
#             VALUES
#             (
#                 %s,
#                 NOW(),
#                 'RUNNING'
#             )
#             RETURNING run_id;
#             """,
#             (pipeline_name,),
#         )

#         run_id = cur.fetchone()[0]
#         conn.commit()

#     logger.info("Started pipeline run %s", run_id)
#     return run_id


# def complete_pipeline_run(conn, run_id: int):
#     """
#     Mark a pipeline run as successful.

#     Parameters
#     ----------
#     conn : psycopg2.connection
#     run_id : int
#     """
#     with conn.cursor() as cur:
#         cur.execute(
#             """
#             UPDATE control.pipeline_runs
#             SET
#                 status='SUCCESS',
#                 completed_at=NOW()
#             WHERE run_id=%s;
#             """,
#             (run_id,),
#         )
#         conn.commit()

#     logger.info("Pipeline run %s completed", run_id)


# def fail_pipeline_run(conn, run_id: int, error: str):
#     """
#     Mark a pipeline run as failed.

#     Parameters
#     ----------
#     conn : psycopg2.connection
#     run_id : int
#     error : str
#     """
#     with conn.cursor() as cur:
#         cur.execute(
#             """
#             UPDATE control.pipeline_runs
#             SET
#                 status='FAILED',
#                 completed_at=NOW(),
#                 error_message=%s
#             WHERE run_id=%s;
#             """,
#             (error[:1000], run_id),
#         )
#         conn.commit()

#     logger.error("Pipeline run %s failed", run_id)


# # ============================================================================
# # Watermark Helpers
# # ============================================================================

# def get_watermark(conn, pipeline_name: str, table_name: str):
#     """
#     Get the latest loaded filename.

#     Parameters
#     ----------
#     conn : psycopg2.connection
#     pipeline_name : str
#     table_name : str

#     Returns
#     -------
#     str | None
#     """
#     with conn.cursor() as cur:
#         cur.execute(
#             """
#             SELECT last_loaded_value
#             FROM control.pipeline_watermarks
#             WHERE pipeline_name=%s
#             AND table_name=%s;
#             """,
#             (pipeline_name, table_name),
#         )

#         row = cur.fetchone()

#     if row:
#         return row[0]

#     return None


# def update_watermark(
#     conn,
#     pipeline_name: str,
#     table_name: str,
#     filename: str,
# ):
#     """
#     Update pipeline watermark.

#     Parameters
#     ----------
#     conn : psycopg2.connection
#     pipeline_name : str
#     table_name : str
#     filename : str
#     """
#     with conn.cursor() as cur:
#         cur.execute(
#             """
#             INSERT INTO control.pipeline_watermarks
#             (
#                 pipeline_name,
#                 table_name,
#                 last_loaded_value,
#                 updated_at
#             )
#             VALUES
#             (
#                 %s,
#                 %s,
#                 %s,
#                 NOW()
#             )

#             ON CONFLICT (pipeline_name, table_name)

#             DO UPDATE SET

#                 last_loaded_value=EXCLUDED.last_loaded_value,
#                 updated_at=NOW();
#             """,
#             (
#                 pipeline_name,
#                 table_name,
#                 filename,
#             ),
#         )

#         conn.commit()


# # ============================================================================
# # MinIO Helpers
# # ============================================================================

# def list_processed_files():
#     """
#     List processed parquet files.

#     Returns
#     -------
#     list[str]
#     """
#     files = []

#     objects = client.list_objects(
#         bucket,
#         prefix="processed/postgres/",
#         recursive=True,
#     )

#     for obj in objects:
#         if obj.object_name.endswith(".parquet"):
#             files.append(obj.object_name)

#     files.sort()

#     return files




# # ============================================================================
# # PostgreSQL Loader
# # ============================================================================

# def load_dataframe(conn, table_name: str, df: pd.DataFrame):
#     """
#     Insert DataFrame into PostgreSQL.

#     Parameters
#     ----------
#     conn : psycopg2.connection
#     table_name : str
#     df : pandas.DataFrame
#     """
#     if df.empty:
#         logger.info("%s is empty. Skipping.", table_name)
#         return

#     columns = list(df.columns)

#     sql = f"""
#         INSERT INTO warehouse.{table_name}
#         ({','.join(columns)})
#         VALUES %s;
#     """

#     values = [tuple(x) for x in df.itertuples(index=False)]

#     with conn.cursor() as cur:
#         execute_values(
#             cur,
#             sql,
#             values,
#             page_size=5000,
#         )


# # ============================================================================
# # Main Loader
# # ============================================================================

# def load_postgres():
#     """
#     Load processed Parquet files into the PostgreSQL warehouse.

#     Workflow
#     --------
#     1. Connect to PostgreSQL.
#     2. Start pipeline run.
#     3. List processed Parquet files.
#     4. Skip previously loaded files.
#     5. Load new files.
#     6. Update watermark after successful load.
#     7. Complete pipeline run.
#     8. Roll back on failure.
#     """
#     pipeline_name = "warehouse_load"

#     conn = get_postgres_connection("WAREHOUSE")

#     run_id = start_pipeline_run(conn, pipeline_name)

#     try:
#         files = list_processed_files()

#         logger.info("Found %s processed files.", len(files))

#         for object_name in files:

#             parts = object_name.split("/")

#             if len(parts) < 4:
#                 logger.warning("Skipping invalid path: %s", object_name)
#                 continue

#             table_name = parts[2]
#             filename = parts[-1]

#             watermark = get_watermark(
#                 conn,
#                 pipeline_name,
#                 table_name,
#             )

#             if watermark and filename <= watermark:
#                 logger.info(
#                     "Skipping already loaded file %s",
#                     filename,
#                 )
#                 continue

#             logger.info("Loading %s", filename)

#             df = download_dataframe(object_name)

#             load_dataframe(
#                 conn,
#                 table_name,
#                 df,
#             )

#             update_watermark(
#                 conn,
#                 pipeline_name,
#                 table_name,
#                 filename,
#             )

#             conn.commit()

#             logger.info(
#                 "Loaded %s (%s rows)",
#                 filename,
#                 len(df),
#             )

#         complete_pipeline_run(conn, run_id)

#     except Exception as exc:

#         conn.rollback()

#         fail_pipeline_run(
#             conn,
#             run_id,
#             str(exc),
#         )

#         logger.exception("Warehouse load failed.")

#         raise

#     finally:
#         conn.close()