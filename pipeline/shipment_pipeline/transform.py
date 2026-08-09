# -------------------------
# imports: Standard Libraries
# -------------------------
import io
import logging
from io import BytesIO

import pandas as pd

from pipeline.utils import get_minio_client, upload_to_minio

# -------------------------
# LOGGING
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# =====================================================================
# Shipments Transformations
# =====================================================================
def transform_shipments_data():
    """
    Transform raw SwiftDrop API Parquet files into clean analytical datasets.

    Workflow:
    - Read all raw API Parquet files from the MinIO `raw/api/` directory.
    - Skip invalid paths, folders, and non-Parquet files.
    - Determine the source table from the object path.
    - Load each Parquet file into a pandas DataFrame.
    - Apply table-specific transformations:
        - Shipments:
            - Explode nested shipment events into individual rows.
            - Flatten nested event dictionaries into columns.
            - Convert timestamp fields to datetime.
            - Convert numeric identifiers to nullable integer types.
            - Remove unnecessary columns.
            - Remove duplicate records.
        - Carriers:
            - Remove duplicate records.
            - Trim leading and trailing whitespace from text columns.
    - Write the transformed dataset as a Parquet file to the MinIO `processed/` layer.
    - Log transformation progress and output file locations.
    """
    logger.info("Starting API transformation from MinIO")
    client, bucket = get_minio_client()

    try:
        objects = client.list_objects(
            bucket,
            prefix="raw/swiftdrop/",
            recursive=True,
        )
        processed_files = 0
        deleted_files = 0
        for obj in objects:
            object_name = obj.object_name

            # Ignore folders and non-parquet files
            if object_name.endswith("/") or not object_name.endswith(".parquet"):
                continue

            parts = object_name.split("/")
            if len(parts) < 4:
                logger.warning(f"Skipping invalid object {object_name}")
                continue

            table_name = parts[2]
            filename = parts[-1]
            logger.info(f"Transforming swiftdrop table '{table_name}'")

            response = client.get_object(bucket, object_name)
            buffer = io.BytesIO(response.read())
            df = pd.read_parquet(buffer)

            response.close()
            response.release_conn()

            # -----------------------------
            # SHIPMENTS
            # -----------------------------
            if table_name == "shipments":
                rows_before = len(df)

                # Explode events
                df = df.explode("events", ignore_index=True)
                rows_after_explode = len(df)

                # Expand event dictionary
                events_df = pd.json_normalize(df["events"])

                # Drop nested columns
                df = df.drop(columns=["events"])

                # Merge back together
                df = pd.concat(
                    [
                        df.reset_index(drop=True),
                        events_df.reset_index(drop=True),
                    ],
                    axis=1,
                )

                # Convert timestamps
                datetime_columns = [
                    "shipped_at",
                    "estimated_delivery_at",
                    "delivered_at",
                    "updated_at",
                    "event_time",
                ]

                for col in datetime_columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce",)

                # Integer columns
                df["order_id"] = df["order_id"].astype("Int64")
                
                df = df.drop(columns=["delivery_address_extra"], errors="ignore")
                df = df.drop_duplicates()

                duplicates_removed = rows_after_explode - len(df)

                logger.info(
                    f"shipments: "
                    f"rows_before={rows_before}, "
                    f"rows_after_explode={rows_after_explode}, "
                    f"duplicates_removed={duplicates_removed}, "
                    f"final_rows={len(df)}"
                )

            # -----------------------------
            # CARRIERS
            # -----------------------------
            elif table_name == "carriers":
                rows_before = len(df)

                # Remove duplicate rows
                df = df.drop_duplicates()
                duplicates_removed = rows_before - len(df)

                # Strip whitespace from string columns
                string_columns = df.select_dtypes(include=["object", "string"]).columns

                for col in string_columns:
                    df[col] = df[col].str.strip()

                logger.info(
                    f"carriers: "
                    f"duplicates_removed={duplicates_removed}, "
                    f"final_rows={len(df)}"
                )

            else:
                logger.warning(f"Unknown table '{table_name}'")
                continue

            
            # Write processed parquet
            output_buffer = BytesIO()
            df.to_parquet(
                output_buffer,
                index=False,
                engine="pyarrow",
            )
            destination = (f"processed/swiftdrop/{table_name}/{filename}")
            upload_to_minio(
                client=client,
                bucket=bucket,
                object_name=destination,
                buffer=output_buffer,
                content_type="application/octet-stream"
            )
            # client.remove_object(bucket, object_name)
            processed_files += 1
            deleted_files += 1
            logger.info(
                f"Uploaded '{destination}' "
                f"({len(df)} records, {len(df.columns)} columns)"
            )
        logger.info("Completed Swiftdrop transformation")
        logger.info(
            f"Processed files: {processed_files} | "
            f"Deleted files: {deleted_files}"
        )

    except Exception:
        logger.exception(f"Swiftdrop transformation failed")
        raise