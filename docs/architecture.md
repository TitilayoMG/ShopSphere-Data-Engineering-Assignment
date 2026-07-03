# Architecture

```mermaid
flowchart LR
    PG[(PostgreSQL Source)]
    MG[(MongoDB Source)]
    API[SwiftDrop FastAPI]
    PIPE[Mentee Python Pipelines]
    LAKE[(MinIO Data Lake)]
    WH[(PostgreSQL Warehouse)]
    PG --> PIPE
    MG --> PIPE
    API --> PIPE
    PIPE --> LAKE
    LAKE --> PIPE
    PIPE --> WH
```

## Why MinIO Is Used

MinIO represents a local data lake. You may use a medallion architecture to organize data as it moves from source extracts through transformed datasets and into the warehouse.
