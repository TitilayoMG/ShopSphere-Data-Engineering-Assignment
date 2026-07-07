# -------------------------
# imports: Standard Libraries
# -------------------------
import io
import logging
import pandas as pd
from minio.error import S3Error
from io import BytesIO

from utils import get_minio_client, upload_to_minio
# -------------------------
# LOGGING
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


client, bucket = get_minio_client()

# =====================================================================
# Postgres Transformations
# =====================================================================
def transform_postgres():
    """
    Transform raw PostgreSQL Parquet files stored in MinIO.

    Workflow:
    - Read all PostgreSQL Parquet files from the raw MinIO layer.
    - Determine the source table from the object path.
    - Apply table-specific data cleaning and transformation rules.
    - Leave tables without defined transformations unchanged.
    - Convert the transformed DataFrame back to Parquet format.
    - Write the processed file to the processed MinIO layer.
    - Log progress, transformation actions, and processing results.
    - Continue processing remaining files even if an individual file fails.
    """
    logger.info("=" * 60)
    logger.info("Starting PostgreSQL transformation...")
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
                # Read parquet from MinIO
                response = client.get_object(bucket, object_name)
                buffer = io.BytesIO(response.read())
                df = pd.read_parquet(buffer)

                response.close()
                response.release_conn()
                # Determine table name
                # raw/postgres/products/products_20260705.parquet           ^
                parts = object_name.split("/")

                if len(parts) < 4:
                    logger.warning(f"Skipping invalid path: {object_name}")
                    continue
                table_name = parts[2]
                logger.info(f"Transforming table '{table_name}'")

                # PRODUCTS
                if table_name == "products":
                    if "brand" in df.columns:
                        null_count = df["brand"].isna().sum()
                        if null_count > 0:
                            logger.info(
                                f"Found {null_count} NULL brand values. "
                                "Replacing with 'Unknown'."
                            )
                        df["brand"] = df["brand"].fillna("Unknown")

                # Other PostgreSQL tables
                else:
                    logger.info(
                        f"No transformation configured for '{table_name}'. "
                        "Copying unchanged."
                    )

                # Convert dataframe to parquet
                # output_buffer = io.BytesIO()
                # table = pa.Table.from_pandas(df)
                # pq.write_table(table, output_buffer)

                output_buffer = io.BytesIO()
                df.to_parquet(
                    output_buffer,
                    engine="pyarrow",
                    index=False,
                )

                # Destination path: processed/file.parquet
                filename = parts[-1]
                destination = f"processed/postgres/{table_name}/{filename}"

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



# =====================================================================
# Mongodb Transformations
# =====================================================================
def transform_mongodb():
    """
    Transform raw MongoDB parquet files into clean analytical datasets.
    customer_sessions
        - Flatten events array
        - Flatten location
        - Flatten device
    product_reviews
        - Clean data
        - Convert datatypes
    Output:
        processed/mongodb/<filename>.parquet
    """
    logger.info("Starting MongoDB transformation from MinIO")
    try:
        objects = client.list_objects(
            bucket,
            prefix="raw/mongodb/",
            recursive=True,
        )
        for obj in objects:
            object_name = obj.object_name
            if object_name.endswith("/") or not object_name.endswith(".parquet"):
                continue
            parts = object_name.split("/")
            if len(parts) < 4:
                logger.warning(f"Skipping invalid object {object_name}")
                continue

            collection = parts[2]
            filename = parts[-1]
            logger.info(f"Transforming MongoDB collection '{collection}'")

            response = client.get_object(bucket, object_name)
            buffer = io.BytesIO(response.read())
            df = pd.read_parquet(buffer)

            response.close()
            response.release_conn()

            logger.info(
                f"Loaded {len(df)} records "
                f"({len(df.columns)} columns)"
            )

            # CUSTOMER SESSIONS
            if collection == "customer_sessions":
                rows_before = len(df)

                # explode events
                df = df.explode("events", ignore_index=True)
                rows_after_explode = len(df)

                # normalize nested fields
                event_df = pd.json_normalize(df["events"])

                df = df.drop(columns=["events"])
                df = pd.concat(
                    [
                        df.reset_index(drop=True),
                        event_df.reset_index(drop=True)
                    ],
                    axis=1,
                )
                df = df.rename(
                    columns={
                        "type": "device_type",
                        "os": "device_os",
                    }
                )
                # timestamps
                datetime_cols = [
                    "started_at",
                    "ended_at",
                    "event_time",
                ]
                for col in datetime_cols:
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                
                # integer columns
                int_cols = [
                    "customer_id",
                    "product_id",
                    "quantity",
                ]
                for col in int_cols:
                    df[col] = df[col].astype("Int64")

                # remove duplicate events
                df = df.drop_duplicates()
                duplicates_removed = rows_after_explode - len(df)
                logger.info(
                    f"customer_sessions: "
                    f"rows_before={rows_before}, "
                    f"rows_after_explode={rows_after_explode}, "
                    f"duplicates_removed={duplicates_removed}, "
                    f"final_rows={len(df)}"
                )
            
            # PRODUCT REVIEWS
            elif collection == "product_reviews":
                df["created_at"] = pd.to_datetime(
                    df["created_at"],
                    errors="coerce",
                )
                integer_cols = [
                    "customer_id",
                    "product_id",
                    "rating",
                    "helpful_votes",
                ]
                for col in integer_cols:
                    df[col] = df[col].astype("Int64")

                df["verified_purchase"] = (
                    df["verified_purchase"]
                    .fillna(False)
                    .astype(bool)
                )
                null_reviews = df["review_text"].isna().sum()
                null_titles = df["title"].isna().sum()
                null_verified = df["verified_purchase"].isna().sum()

                df["verified_purchase"] = (df["verified_purchase"].fillna(False).astype(bool))
                df["review_text"] = (df["review_text"].fillna("").str.strip())
                df["title"] = (df["title"].fillna("").str.strip())

                rows_before = len(df)
                df = df.drop_duplicates()

                duplicates_removed = rows_before - len(df)

                logger.info(
                    f"product_reviews: "
                    f"null_review_text={null_reviews}, "
                    f"null_title={null_titles}, "
                    f"null_verified_purchase={null_verified}, "
                    f"duplicates_removed={duplicates_removed}, "
                    f"final_rows={len(df)}"
                )
            else:
                logger.warning(f"Unknown collection '{collection}'")
                continue

            # Write parquet
            output_buffer = BytesIO()
            df.to_parquet(
                output_buffer,
                index=False,
                engine="pyarrow",
            )
            destination = (f"processed/mongodb/{collection}/{filename}")
            upload_to_minio(
                client=client,
                bucket=bucket,
                object_name=destination,
                buffer=output_buffer,
                content_type="application/octet-stream"
            )
            logger.info(
                f"Successfully uploaded "
                f"{len(df)} records "
                f"({len(df.columns)} columns) "
                f"to '{destination}'"
            )
        logger.info("Completed MongoDB transformation")
    
    except Exception as e:
        logger.exception(f"MongoDB transformation failed: {e}")
        raise        



# =====================================================================
# API Transformations
# =====================================================================
def transform_api():
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
    try:
        objects = client.list_objects(
            bucket,
            prefix="raw/api/",
            recursive=True,
        )
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
            logger.info(f"Transforming API table '{table_name}'")

            response = client.get_object(bucket, object_name)
            buffer = io.BytesIO(response.read())
            df = pd.read_parquet(buffer)

            response.close()
            response.release_conn()

            logger.info(
                f"Loaded {len(df)} records "
                f"({len(df.columns)} columns)"
            )
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
                    if col in df.columns:
                        df[col] = pd.to_datetime(
                            df[col],
                            errors="coerce",
                        )

                # Integer columns
                if "order_id" in df.columns:
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
            destination = (f"processed/api/{table_name}/{filename}")
            upload_to_minio(
                client=client,
                bucket=bucket,
                object_name=destination,
                buffer=output_buffer,
                content_type="application/octet-stream"
            )
            logger.info(
                f"Uploaded '{destination}' "
                f"({len(df)} records, {len(df.columns)} columns)"
            )
        logger.info("Completed API transformation")

    except Exception as e:
        logger.exception(f"API transformation failed: {e}")
        raise

if __name__ == "__main__":
    transform_postgres()
    transform_mongodb()
    transform_api()