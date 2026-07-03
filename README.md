# ShopSphere Data Engineering Case Study

This repository is a local batch data engineering case study for a beginner mentee. ShopSphere is a fictional e-commerce company with data spread across PostgreSQL, MongoDB, and a third-party logistics API. Your job is to build Python pipelines that stage raw data in MinIO, transform it, and load analytical tables into a PostgreSQL warehouse.

The infrastructure and source data are provided. The Python pipeline side is intentionally not scaffolded; the mentee is expected to create that structure themselves.

## Architecture

PostgreSQL, MongoDB, and the SwiftDrop FastAPI service act as source systems. The mentee-created Python pipelines should extract from those systems, write raw data to MinIO, write processed data back to MinIO, and load the PostgreSQL warehouse.

See [docs/architecture.md](docs/architecture.md) for the full diagram and explanation.

## Prerequisites

- Docker and Docker Compose
- Git

## Setup

Create your local environment file:

```bash
cp .env.example .env
```

Start all services:

```bash
docker compose up -d --build
```

Check service status:

```bash
docker compose ps
```

View logs for the API:

```bash
docker compose logs mock-api
```

## Service Access

Source PostgreSQL is exposed on localhost port `5433`:

```bash
docker compose exec source-postgres psql -U shopsphere_user -d shopsphere
```

Warehouse PostgreSQL is exposed on localhost port `5434`:

```bash
docker compose exec warehouse-postgres psql -U warehouse_user -d shopsphere_warehouse
```

MongoDB is exposed on localhost port `27018`:

```bash
docker compose exec source-mongodb mongosh -u shopsphere_mongo -p shopsphere_mongo_password --authenticationDatabase admin shopsphere_events
```

FastAPI documentation is available at [http://localhost:8000/docs](http://localhost:8000/docs).

MinIO console is available at [http://localhost:9001](http://localhost:9001) with the development credentials in `.env.example`.

## MinIO Buckets

Buckets are not created automatically. Decide how you want to organize object storage for your pipeline design, then create the required buckets through the MinIO console or with a MinIO client.

## Stop and Reset

Stop containers while keeping volumes:

```bash
docker compose down
```

Delete containers and local volumes, then start from a clean state:

```bash
docker compose down -v
docker compose up -d --build
```

## Files You Should Create Or Modify

You are expected to create your own Python pipeline structure, tests, Docker image, and CI/CD workflow. No starter pipeline files are provided.

You should also update:

- `docs/` for your design notes and final documentation
- `README.md` to document your completed pipeline behavior

Infrastructure files under `infrastructure/` and service definitions in `docker-compose.yml` should normally remain unchanged unless you document a clear reason.
