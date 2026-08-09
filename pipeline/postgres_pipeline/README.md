

# ShopSphere Data Pipeline

ShopSphere is a batch **ETL (Extract → Transform → Load)** pipeline that consolidates data from three heterogeneous sources — a PostgreSQL OLTP database, a MongoDB store, and the external **SwiftDrop** REST API — into a single PostgreSQL analytical warehouse. Files are staged in **MinIO** (S3-compatible object storage) between each stage, and every step is tracked in a control schema so the pipeline can resume safely after failures.

---

## 1. High-Level Architecture

```
                ┌───────────────┐   ┌───────────────┐   ┌───────────────┐
                │   PostgreSQL  │   │    MongoDB    │   │  SwiftDrop    │
                │   (source)    │   │  (source)     │   │  REST API     │
                └───────┬───────┘   └───────┬───────┘   └───────┬───────┘
                        │                   │                   │
                        ▼                   ▼                   ▼
                 ┌─────────────────────────────────────────────────┐
                 │            EXTRACT  (extract.py)                │
                 │   writes Parquet files to  raw/<source>/<obj>/  │
                 └───────────────────────┬───────────────────────--┘
                                          ▼
                 ┌─────────────────────────────────────────────────┐
                 │           TRANSFORM  (transform.py)             │
                 │  cleans / flattens data, writes to               │
                 │  processed/<source>/<obj>/, deletes raw files    │
                 └───────────────────────┬───────────────────────--┘
                                          ▼
                 ┌─────────────────────────────────────────────────┐
                 │              LOAD  (load.py)                    │
                 │   COPY's processed Parquet → PostgreSQL          │
                 │           WAREHOUSE tables                      │
                 └─────────────────────────────────────────────────┘

     MinIO (bucket) is the staging layer for all raw/ and processed/ data.
     control.pipeline_runs and control.pipeline_watermarks (in the
     WAREHOUSE Postgres instance) track run history and incremental state.
```

The entire pipeline is orchestrated by **`main.py`**, which runs all three stages, for all three sources, in a fixed order:

```
extraction   → postgres_extraction() → mongodb_extraction() → api_extraction()
transformation → transform_postgres() → transform_mongodb() → transform_api()
loading      → load_postgres()      → load_mongodb()       → load_api()
```

If any stage raises an exception, the whole run fails loudly (logged and re-raised) — there is no partial "successful" pipeline run.

---

## 2. Project Structure

| File | Responsibility |
|---|---|
| `main.py` | Entry point. Runs extract → transform → load in sequence, with top-level logging and error propagation. |
| `extract.py` | Pulls raw data from Postgres, MongoDB, and the SwiftDrop API; writes it as Parquet into MinIO under `raw/`. |
| `transform.py` | Reads `raw/` Parquet files, applies per-table cleaning rules, writes results to `processed/`, and deletes the raw file once processed. |
| `load.py` | Reads `processed/` Parquet files and bulk-loads them into the PostgreSQL warehouse using `COPY`. |
| `utils.py` | Shared infrastructure helpers: MinIO client, Postgres connections, Parquet <-> buffer conversions, watermark read/write (MinIO-based and Postgres-based), and pipeline run bookkeeping. |
| `config.json` | Declarative configuration listing every table/collection/endpoint to extract, plus the chunk size used for batching. |

---

## 3. Data Sources & Configuration (`config.json`)

```json
{
  "chunk_size": 100,
  "data_sources": {
    "postgres": { "tables": [...] },
    "mongodb":  { "collections": [...] },
    "fast_api": { "endpoints": [...] }
  }
}
```

- **`chunk_size`** — the number of rows/documents/records read or written per batch across *all* sources (Postgres server-side cursor size, MongoDB batch size, and Parquet file row-count).

### 3.1 PostgreSQL (source system)
| Table |
|---|
| `customers` |
| `orders` |
| `products` |
| `order_items` |
| `payments` |

### 3.2 MongoDB (source system)
| Collection |
|---|
| `customer_sessions` |
| `product_reviews` |

### 3.3 SwiftDrop REST API (`fast_api`)
| Table | Endpoint | Paginated? |
|---|---|---|
| `carriers` | `/api/v1/carriers` | No |
| `shipments` | `/api/v1/shipments` | Yes |

---

## 4. Extraction Stage (`extract.py`)

All extraction functions load `config.json` once at import time and reuse a shared MinIO client/bucket.

### 4.1 `postgres_extraction()`
- Iterates over every table in `POSTGRES_CONFIG["tables"]`.
- Uses a **named (server-side) cursor** with `itersize = chunk_size` to stream large tables without loading them fully into memory.
- **Incremental logic:**
  - `order_items` has no `updated_at` column → **always fully re-extracted**.
  - All other tables use a stored `updated_at` watermark (read from MinIO metadata via `read_minio_watermark`). If a watermark exists, only rows with `updated_at > watermark` are pulled; otherwise a full extraction is performed.
- Rows are fetched in `chunk_size` batches (`cursor.fetchmany`), converted to Parquet (Snappy-compressed, via `records_to_parquet_buffer`), and uploaded to:
  ```
  raw/postgres/<table>/<table>_<timestamp>_<file_number>.parquet
  ```
- After a table finishes, the new high-water mark (max `updated_at` seen) and last file number are persisted back to MinIO (skipped for `order_items`).

### 4.2 `mongodb_extraction()`
- Iterates over every collection in `MONGODB_CONFIG["collections"]`.
- **Incremental logic:** uses the last processed `_id` (`last_loaded_object_id`) as a watermark. Only documents with `_id > watermark` are queried, sorted ascending by `_id`.
- Documents are streamed in `chunk_size` batches; each `ObjectId` is stringified before serialization.
- Each full batch (and any final partial batch) is written to Parquet and uploaded to:
  ```
  raw/mongodb/<collection>/<collection>_<timestamp>_<file_number>.parquet
  ```
- The watermark is updated to the highest `_id` seen.

### 4.3 `api_extraction()`
- Iterates over every endpoint in `API_CONFIG["endpoints"]` (`carriers`, `shipments`).
- **Incremental logic:** applies an `updated_since` watermark (stored under the `shipments` object name) as a query parameter, only for paginated endpoints.
- **Pagination:** paginated endpoints (`shipments`) loop through pages (`page`, `limit=100`) following a `next_page` cursor returned in the response body, until no more pages/records remain. Non-paginated endpoints (`carriers`) are fetched once.
- Each page/response is flattened to a DataFrame and uploaded to:
  ```
  raw/api/<table>/<table>_<timestamp>_<file_number>.parquet
  ```
- The watermark advances to the maximum `updated_at` seen across `shipments` records, and is only persisted if it actually changed.

> **Note:** all three extraction functions log a structured summary (rows/documents/records processed, files written, elapsed time) at completion.

---

## 5. Transformation Stage (`transform.py`)

Each transform function scans MinIO under `raw/<source>/`, applies table-specific cleaning, writes the result to `processed/<source>/<table>/<same filename>`, and **deletes the original raw file** once the processed version is uploaded successfully (raw data is not retained after transformation).

### 5.1 `transform_postgres()`
| Table | Transformation |
|---|---|
| `products` | Fills null `brand` values with `"Unknown"`. |
| all others | Copied through unchanged (no transformation defined). |

### 5.2 `transform_mongodb()`
| Collection | Transformation |
|---|---|
| `customer_sessions` | Explodes the nested `events` array (one row per event), flattens the event fields via `json_normalize`, drops the raw `events`/`device` columns, renames `type`→`device_type` and `os`→`device_os`, casts `started_at`/`ended_at`/`event_time` to datetime, casts `customer_id`/`product_id`/`quantity` to nullable `Int64`, and drops exact duplicate rows. |
| `product_reviews` | Casts `created_at` to datetime; casts `customer_id`, `product_id`, `rating`, `helpful_votes` to nullable `Int64`; fills/casts `verified_purchase` to boolean (default `False`); fills null `review_text`/`title` with an empty string and strips whitespace; drops exact duplicates. Null counts and duplicate counts are logged for observability. |
| unknown collections | Logged as a warning and skipped. |

### 5.3 `transform_api()`
| Table | Transformation |
|---|---|
| `shipments` | Explodes the nested `events` array, flattens event fields, converts `shipped_at`, `estimated_delivery_at`, `delivered_at`, `updated_at`, `event_time` to datetime, casts `order_id` to nullable `Int64`, drops the `delivery_address_extra` column, and drops duplicates. |
| `carriers` | Drops duplicate rows and strips leading/trailing whitespace from all string columns. |
| unknown tables | Logged as a warning and skipped. |

All three functions continue processing remaining files even if an individual file fails (errors are logged, not fatal to the whole batch), **except** `transform_mongodb` and `transform_api`, which re-raise on unexpected top-level errors after logging.

---

## 6. Load Stage (`load.py`)

All three load functions follow the same pattern:

1. Connect to the **WAREHOUSE** PostgreSQL instance.
2. List processed Parquet files under `processed/<source>/<table>/`.
3. For each file, derive a `watermark` value from the filename (a per-file idempotency key).
4. Check `control.pipeline_runs` (`read_pipeline_runs`) to see if this exact `(pipeline_name, source_name, watermark)` combination already loaded successfully — **if so, the file is skipped** (idempotent / safe to re-run).
5. Otherwise, start a new run record (`start_pipeline_run`), read the Parquet file, convert it to CSV in-memory, and bulk-load it with `cursor.copy_expert(... COPY ... FROM STDIN ...)`.
6. On success: mark the run successful (`mark_pipeline_success`) and update the watermark table (`update_pipeline_watermark`), then commit.
7. On failure: roll back, mark the run failed (`mark_pipeline_fail`) with the error message, commit that failure record, then re-raise.

### 6.1 `load_postgres()`
- Fixed load order (respects foreign-key dependencies):
  1. `customers`
  2. `products`
  3. `orders`
  4. `order_items`
  5. `payments`
- Watermark column recorded as `file_timestamp`.

### 6.2 `load_mongodb()`
- Load order: `customer_sessions`, then `product_reviews`.
- Watermark column recorded as `filename`.

### 6.3 `load_api()`
- Load order: `carriers`, then `shipments`.
- Watermark column recorded as `filename`.
- Additionally skips (with a warning) any file whose DataFrame is empty.

---

## 7. State & Idempotency

The pipeline maintains state in **two different places**, for two different purposes:

| Mechanism | Location | Purpose |
|---|---|---|
| **MinIO watermark file** (`metadata/shopsphere_watermark.json`) | MinIO bucket | Tracks the *extraction* high-water mark per `(source, object)` — e.g., the last `updated_at` pulled from Postgres, the last MongoDB `_id`, or the last API `updated_since` — so extraction only pulls new/changed data. |
| **`control.pipeline_runs` / `control.pipeline_watermarks`** (Postgres tables) | WAREHOUSE database | Tracks *load* run history per `(pipeline_name, source_name, watermark)`, used to make loading idempotent — a file that already loaded successfully will never be loaded twice. |

This two-tier design means extraction and loading can each be re-run safely without duplicating data or re-processing already-completed work.

---

## 8. Storage Layout in MinIO

```
<bucket>/
├── raw/
│   ├── postgres/<table>/<table>_<timestamp>_<n>.parquet
│   ├── mongodb/<collection>/<collection>_<timestamp>_<n>.parquet
│   └── api/<table>/<table>_<timestamp>_<n>.parquet
├── processed/
│   ├── postgres/<table>/<same filename as raw>
│   ├── mongodb/<collection>/<same filename as raw>
│   └── api/<table>/<same filename as raw>
└── metadata/
    └── shopsphere_watermark.json
```

Raw files are deleted from MinIO immediately after being successfully transformed — `processed/` is the only long-lived staging copy.

---

## 9. Required Environment Variables

Configured via a `.env` file (loaded with `python-dotenv`):

| Variable | Used For |
|---|---|
| `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_SECURE`, `MINIO_BUCKET` | MinIO object storage connection. |
| `SOURCE_POSTGRES_HOST`, `SOURCE_POSTGRES_PORT`, `SOURCE_POSTGRES_DB`, `SOURCE_POSTGRES_USER`, `SOURCE_POSTGRES_PASSWORD` | Source PostgreSQL (OLTP) connection, used during extraction. |
| `WAREHOUSE_POSTGRES_HOST`, `WAREHOUSE_POSTGRES_PORT`, `WAREHOUSE_POSTGRES_DB`, `WAREHOUSE_POSTGRES_USER`, `WAREHOUSE_POSTGRES_PASSWORD` | Warehouse PostgreSQL connection, used during loading and for control-table bookkeeping. |
| *(MongoDB connection variables)* | Used by `mongodb_extraction()` to build a `MongoClient` (see `extract.py`). |
| `MOCK_API_BASE_URL` | Base URL for the SwiftDrop REST API. |

> The `get_postgres_connection(prefix)` helper in `utils.py` generically builds connection parameters from any prefix (e.g. `"SOURCE"` or `"WAREHOUSE"`), so both databases share the same connection logic.

---

## 10. Required PostgreSQL Schema (Warehouse side)

The pipeline WAREHOUSE database already has:

- **`public.<table>`** — destination tables for `customers`, `orders`, `products`, `order_items`, `payments`, `customer_sessions`, `product_reviews`, `carriers`, `shipments` — with columns matching the transformed Parquet schemas, ready to receive `COPY ... FORMAT CSV` data.
- **`control.pipeline_runs`** — columns: `run_id` (PK, auto), `pipeline_name`, `source_name`, `started_at`, `completed_at`, `status`, `records_extracted`, `records_loaded`, `watermark_value`, `error_message`.
- **`control.pipeline_watermarks`** — columns: `pipeline_name`, `source_name`, `watermark_column`, `watermark_value`, `updated_at`, with a unique constraint on `(pipeline_name, source_name)` to support `ON CONFLICT ... DO UPDATE`.

---

## 11. Running the Pipeline

Delete containers and local volumes, then start from a clean state:

```bash
docker compose down -v
docker compose up -d --build
```

Run the `pipeline`
```bash
docker compose up --build pipeline
```

```bash
docker compose down
```

`main.py` will run the full extract → transform → load cycle for all three sources, log each stage boundary clearly, and exit non-zero (via the re-raised exception) if any stage fails.
---

## 12. Running the Pipeline Docker Image
The `madina345/shopsphere:1.0` image is designed to run **with the other services** defined in the project's `docker-compose.yml`.

The pipeline depends on the following services:
- Source PostgreSQL
- Source MongoDB
- MinIO
- Mock API

### Prerequisites
- Docker Desktop installed and running
- Docker Compose installed
- A `.env` file in the project root containing all required environment variables

### Start all services
From the project root directory, run:

```bash
docker compose up -d
```

This starts all services on the same Docker network, allowing the pipeline container to communicate with:
- `source-postgres`
- `source-mongodb`
- `minio`
- `mock-api`

### Verify services are running

```bash
docker compose ps
```
Ensure all containers are in the **Up** state before running the pipeline.

### Important

Do **not** run the pipeline image directly using:

```bash
docker run madina345/shopsphere:1.0
```
Running the image this way creates an isolated container that is **not connected** to the Docker Compose network. As a result, the pipeline will not be able to resolve service names such as:

- `source-postgres`
- `source-mongodb`
- `minio`
- `mock-api`

and will fail with errors similar to:

```text
psycopg2.OperationalError:
could not translate host name "source-postgres" to address
```

Always run the pipeline together with the other services using Docker Compose so that all containers share the same network.


## 13. Key Design Notes / Known Quirks

- **`order_items`** intentionally has no incremental watermark — it is fully re-extracted every run because the source table has no `updated_at` column.
- **Idempotent loads**: re-running `load_postgres()` / `load_mongodb()` / `load_api()` is safe — already-successfully-loaded files are detected via `control.pipeline_runs` and skipped.
- **Raw-file deletion**: `transform_*` functions delete the source raw Parquet file after a successful transform, so `raw/` is meant to be transient, not an archive.
- **Error isolation**: `transform_postgres()`  `transform_mongodb()` and `transform_api()`  catches and logs per-file exceptions and keeps processing the rest of the batch.
- **Empty file handling**: `load_api()` explicitly skips empty processed Parquet files with a warning rather than attempting a `COPY` with zero rows.

--

## 14. limitations of this project

- **`Data integrity`** the load stage can't handle updates
load_postgres(), load_mongodb(), and load_api() all use COPY ... FROM STDIN with no ON CONFLICT / upsert logic. Combined with the fact that extraction re-pulls any row whose updated_at (or _id) has changed.

- **`No true schema flexibility`** 
Everything about which columns need casting, renaming, or null-handling is hardcoded per table name inside transform.py (e.g. products.brand, customer_sessions event flattening). config.json only declares which tables/collections/endpoints exist — adding a new table means writing new Python, not editing config. Tables without an explicit branch are either passed through unchanged (Postgres) or dropped with a warning (Mongo/API), silently.

 **`No deletes / no CDC`** 
The pipeline only extracts inserts and updates via updated_at / _id watermarks. Hard deletes in the source systems are never reflected in the warehouse — there's no soft-delete flag, no tombstone record, no periodic full reconciliation.

 **`Scalability constraints`** 
Fully sequential: one source at a time, one table at a time, one file at a time — no parallelism anywhere in main.py or the loaders.
Each Parquet file is loaded entirely into a pandas DataFrame in memory (pd.read_parquet on the full buffer) — large exploded tables like shipments (nested events arrays) could get memory-heavy with no chunking safeguard at transform/load time.
