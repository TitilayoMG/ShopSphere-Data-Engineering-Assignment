# ShopSphere Data Engineering Assignment

## Background

You have joined ShopSphere as a junior data engineer. ShopSphere sells consumer products online and currently has operational data spread across multiple systems. Business teams need a central analytical warehouse for sales, customer, product, payment, website behavior, shipment, and delivery reporting.

Your task is to build the batch pipelines that move data from the source systems into the local data lake and then into the analytical warehouse.

## Existing Infrastructure

This repository already provides:

- A PostgreSQL transactional source database named `shopsphere`
- A MongoDB event database named `shopsphere_events`
- A FastAPI mock logistics service named SwiftDrop Logistics
- A MinIO object storage service for your local data lake
- A PostgreSQL warehouse database named `shopsphere_warehouse`

## Your Responsibilities

You are responsible for setting up and writing the Python pipeline implementation. Create the local pipeline project structure yourself, choose sensible dependencies, and document how your code should be run.

You must also decide how to organize MinIO storage and create any buckets your design requires before running your pipelines.

After that, extract from each source, stage raw data in MinIO, transform the data, write processed outputs, and load warehouse tables.

You should also document how your pipelines work, how they can be rerun, and what limitations remain.

## Functional Requirements

- Extract PostgreSQL tables: customers, products, orders, order_items, and payments.
- Extract MongoDB collections: customer_sessions and product_reviews.
- Extract SwiftDrop shipments through the paginated API.
- Store raw extracts in MinIO using clear object paths.
- Transform data into clean analytical datasets.
- Flatten nested MongoDB event arrays and shipment event arrays.
- Write processed data to MinIO.
- Load analytical warehouse tables.
- Prevent duplicate warehouse records during reruns.
- Track pipeline runs and watermarks in the warehouse control schema.

## Technical Requirements

- Use Python for pipeline logic.
- Create your own pipeline directory, dependency file, and execution commands.
- Package your pipelines into a Docker image.
- Maintain pipeline build and validation through CI/CD.
- Use environment variables for configuration.
- Add clear logging and error handling.
- Add tests for at least some transformation logic.
- Keep credentials out of Git.

## Deliverables

- Working extraction pipelines for all three source systems
- Raw data written to MinIO
- Processed data written to MinIO
- Warehouse analytics tables populated with useful data
- A Docker image for running the pipelines
- CI/CD configuration for building and validating the pipeline project
- Tests for transformation or validation logic
- Updated project documentation
- Git history showing meaningful progress

## Core Acceptance Criteria

1. All containers start successfully.
2. Raw extracts are written to MinIO.
3. Warehouse tables are populated.
4. Pipelines are safe to rerun.
5. Errors are logged clearly.
6. At least three meaningful data-quality checks are implemented.
7. Pipeline executions are recorded.
8. The repository contains clear documentation.
9. Tests cover at least some transformation logic.
10. No credentials are committed to Git.

## Bonus Tasks

- API retries
- Rate-limit handling
- Parquet partitioning
- Dead-letter or rejected-record handling
- CI workflow
- Additional analytics views

## Git Expectations

Use Git throughout your work. Create at least one feature branch, make meaningful commits, and merge completed work back into the main branch.

Example commit messages:

```text
feat: add postgres extraction pipeline
feat: stage mongodb events in minio
feat: implement paginated shipment extraction
feat: add customer dimension load
fix: prevent duplicate shipment records
test: add transformation validation tests
docs: document pipeline execution
```

Avoid committing `.env`, local data extracts, credentials, logs, or temporary files.
