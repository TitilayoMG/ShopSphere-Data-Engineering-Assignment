# ShopSphere Data Pipelines
This README focuses on **how multiple pipelines are packaged and run from a
single Docker image**. Pipeline-specific transformation logic is documented
separately inside each pipeline's own folder.

---

## Table of Contents

- [Available Pipelines](#available-pipelines)
- [Repository](#repository--image-layout)
- [Pipeline Entry-Point Convention](#pipeline-entry-point-convention)
- [How Pipeline Selection Works](#how-pipeline-selection-works)
- [Building the Image](#building-the-image)
- [Running Alongside the Other Services](#running-alongside-the-other-services)
- [Running Pipelines](#running-pipelines)
- [Listing Pipelines](#listing-pipelines)
- [Running All Pipelines](#running-all-pipelines)
- [Passing Extra Arguments](#passing-extra-arguments)
- [Error Handling & Exit Codes](#error-handling--exit-codes)
- [Environment Variables & Credentials](#environment-variables--credentials)
- [Adding a New Pipeline](#adding-a-new-pipeline)
- [Non-Root Execution](#non-root-execution)
- [Testing the Runner](#testing-the-runner)
- [CI/CD](#cicd)

---

## Available Pipelines

Run `docker run pipeline list` at any time to get this list directly from the
image

| Pipeline name        | Source system            | Summary                                                                 |
|-----------------------|---------------------------|--------------------------------------------------------------------------|
| `postgres_pipeline`   | PostgreSQL (`shopsphere`) | Extracts `customers`, `products`, `orders`, `order_items`, `payments`; stages raw/processed data in MinIO; loads warehouse dimension/fact tables. |
| `shipment_pipeline`   | SwiftDrop Logistics API   | Pages through `/api/v1/shipments`, flattens nested status-event arrays, stages raw/processed data in MinIO, and loads shipment, shipment-event, and carrier tables. |

Each pipeline is self-contained: it manages its own extraction, staging,
transformation, load, watermark tracking, and control-table logging. The
sections below describe only the shared mechanism that lets one Docker image
run any of them by name.

---

## Repository

```text
.        
├── pipelines/
│   ├── __init__.py
│   ├── postgres_pipeline/  
│   └── shipment_pipeline/     
│       ├── __init__.py
│       ├── main.py            
│       ├── extract.py
│       ├── transform.py
│       └── load.py
|   ├── Dockerfile
│   └── entrypoint.sh  
|
├── tests/
│   ├── test_api_transformation.py       
│   └── test_extract.py     
├── requirements.txt
├── .env.example
└── README.md
```

Everything a pipeline needs to run lives under `pipelines/`. The entrypoint
never reaches outside that directory, which is what makes path-traversal
protection straightforward 

---

## Pipeline Entry-Point Convention

   **Folder pipeline**
   `pipeline/<pipeline_name>/main.py`
   Same contract: a `main()` function the runner can invoke, 

---

## How Pipeline Selection Works

`entrypoint.sh` is the image's `ENTRYPOINT`. It receives the
container's arguments directly from `docker run` and does the following:

1. **No argument, or `-h` / `--help` / `list`** → print usage, the list of
   available pipeline names and their description, exit `0`.
2. **First argument = pipeline name** (with or without `.py`):
   - Normalize the name: strip a trailing `.py` if present.
   - Look up the normalized name in the pipeline, which enumerates
     only folders directly inside `pipeline/` matching the entry-point
     convention above (`pipeline/<pipeline_name>`).
   - Reject the name if it isn't an exact, unambiguous match — no partial or
     fuzzy matching. This also naturally blocks path traversal: names
     containing `/`, `..`, or absolute paths are never looked up against the
     filesystem, only compared against the registry's known-good name list.
   - If the name isn't found: print the available pipeline names and exit
     non-zero.
3. **Remaining arguments** (everything after the pipeline name) are forwarded
   unchanged to the selected pipeline's `main()`
4. Every log line the runner itself emits is prefixed with the resolved
   pipeline name (e.g. `Running pipeline: ...`), so
   pipeline output is unambiguous.


---

## Running Alongside the Other Services

Pipelines are launched with **plain `docker run`**. Since a pipeline
container needs to reach PostgreSQL, MongoDB, MinIO, and the SwiftDrop API,
it must be started:

- on the **same Docker network** as those services, and
- with the **same `.env` file** used to configure them.

### 1. Make sure the required services are already running

`postgres`, `mongo`, `minio`, and the `swiftdrop` mock API must already be
up and attached to a shared network. Confirm the network name, for example:

```bash
docker network ls
```

If a dedicated network doesn't already exist, create one and attach the
services to it:

```bash
docker network create shopsphere-net
```

### 2. Run the pipeline container on that same network

```bash
docker run --rm \
  --network shopsphere-net \
  --env-file .env \
  pipeline postgres_pipeline
```

- `--network shopsphere-net` puts the pipeline container on the same Docker
  network as `postgres`, `mongo`, `minio`, and `swiftdrop API`, so hostnames like
  `postgres`, `minio`, etc. resolve exactly as they do for those services.
- `--env-file .env` supplies every connection setting (DB host/port/user/
  password, MinIO endpoint/keys, SwiftDrop base URL/key, warehouse
  credentials) at runtime. Nothing is baked into the image.
- Nothing else changes: the pipeline name and any extra arguments are still
  passed straight through, exactly as described below.

---

## Running Pipelines

Both of the following forms work identically:

```bash
# extension-less form
docker run --rm --network shopsphere-net --env-file .env pipeline postgres_pipeline

# .py form
docker run --rm --network shopsphere-net --env-file .env pipeline postgres_pipeline.py

# for shipment pipeline, same two forms
docker run --rm --network shopsphere-net --env-file .env pipeline shipment_pipeline
docker run --rm --network shopsphere-net --env-file .env pipeline shipment_pipeline.py
```

An unknown name fails fast with a helpful message, list of available pipelines, their descriptions and a non-zero exit code:

```bash
$ docker run --rm --network shopsphere-net --env-file .env pipeline nonexistent_pipeline
ERROR: unknown pipeline 'nonexistent_pipeline'
Available pipelines: 

   postgres_pipeline              Extract and load PostgreSQL, MongoDB and API data 
   shipment_pipeline              Extract and transform shipment data 
$ echo $?
1
```

---

## Listing Pipelines

```bash
docker run --rm pipeline list
docker run --rm pipeline           # same result — no args = usage + list
docker run --rm pipeline --help
```

Listing the available pipelines doesn't touch any source system, so
`--network` and `--env-file` aren't required for these forms (though passing
them is harmless).

All three print the same list, so it can never drift out of sync with what's actually in the image.

---

## Running All Pipelines

```bash
docker run --rm --network shopsphere-net --env-file .env pipeline run-all
```

`run-all` executes every registered pipeline sequentially in a stable order
(alphabetical by name), logging a `[run-all]`-prefixed summary line per
pipeline (name, exit code). Behavior on failure:

- Detects and runs only directories whose names end with _pipeline.
- Runs each pipeline one after another.
- Waits for each pipeline to finish before starting the next one.
- Stops immediately if any pipeline fails.
- Returns a non-zero exit code when a pipeline fails, allowing Docker, CI/CD, or scheduled jobs to detect the failure.
- Only reports success when all pipelines complete successfully.

---

## Passing Extra Arguments

The entrypoint supports passing extra arguments to a selected pipeline:
Any arguments provided after the pipeline name are forwarded to that pipeline's main.py.

For example:
```bash
docker run --rm --network shopsphere-net --env-file .env pipeline postgres_pipeline argument1 argument2
```

The entrypoint passes these arguments to:
```bash
python -m pipeline.postgres_pipeline.main argument1 argument2
```

`Current Behavior`
The current pipelines do not implement argument parsing, so `extra arguments` are currently ignored by the pipeline.

`For example:`
```bash
docker run --rm --network shopsphere-net --env-file .env pipeline postgres_pipeline argument1 argument2
```

will still run postgres_pipeline, but argument1 and argument2 have no effect on the pipeline's behavior.

---

## Error Handling & Exit Codes

| Situation                                   | Behavior                                              | Exit code |
|----------------------------------------------|--------------------------------------------------------|-----------|
| No pipeline name / `--help` / `list`          | Print usage + available pipelines                     | `0`       |
| Unknown or ambiguous pipeline name            | Print error + available pipelines                     | `1`       |
| Name contains `/`, `..`, or is an absolute path | Rejected as unknown (never touches the filesystem directly) | `1` |
| Pipeline runs and completes normally          | Pipeline's own exit code is passed through             | pipeline's own code |
| Pipeline raises an unhandled exception        | Logged with traceback, then re-raised                 | `1`       |
| `run-all` with one or more failures           | Summary printed; overall failure                       | `1`       |

---

## Environment Variables & Credentials

Configuration is supplied entirely through environment variables, passed at
`docker run` time via `--env-file .env`:

- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `MONGO_URI`
- `SWIFTDROP_BASE_URL`, `SWIFTDROP_API_KEY`
- `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`
- `WAREHOUSE_HOST`, `WAREHOUSE_PORT`, `WAREHOUSE_DB`, `WAREHOUSE_USER`, `WAREHOUSE_PASSWORD`

None of these are baked into the image or committed to Git. 
Because the container is started with `--network shopsphere-net`, the
hostnames in `.env` (e.g. `POSTGRES_HOST=postgres`, `MINIO_ENDPOINT=minio:9000`)
resolve using the same Docker network's internal DNS that the other services
use to talk to each other.

---

## Adding a New Pipeline

1. Create:
   - `pipelines/<new_pipeline_name>/main.py` with a `main()` function 
2. Confirm it shows up and runs:
   ```bash
   docker run pipeline list
   docker run --rm --network shopsphere-net --env-file .env pipeline <new_name>
   ```
5. Add a transformation test to `tests` if the pipeline has meaningful transformation logic

---

## Non-Root Execution
The current pipeline container does not explicitly configure a non-root user.

`Non-root` execution has not yet been implemented. This should be addressed in a future improvement to run the container with a dedicated unprivileged user and improve container security.


---