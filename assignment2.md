TODO: Support multiple runnable pipelines in one Docker image

Goal
----
Change the pipeline Docker image so that it can contain multiple pipeline task
files and/or pipeline folders. A user must be able to select which pipeline to
run by passing its name to `docker run`.

Recommended additional pipeline
-------------------------------
Create a SwiftDrop shipment pipeline. It should:

- Read every page from the `/api/v1/shipments` endpoint.
- Extract carrier and shipment data.
- Flatten each shipment's nested status-event array.
- Store raw and processed outputs in MinIO using clear object paths.
- Load shipment, shipment-event, and carrier data into the warehouse.
- Be safe to rerun without creating duplicate records.
- Record its status, row counts, errors, and watermark in the control tables.
- Log useful errors and retry transient API failures.

Required container behavior
---------------------------
The same image must contain at least two independently runnable pipelines.
The selected pipeline must run using either form below:

    docker run pipeline pipeline_name.py
    docker run pipeline pipeline_name

For example:

    docker run pipeline postgres_pipeline
    docker run pipeline shipments_pipeline.py

The image's entrypoint or runner must:

1. Accept the pipeline name as the first container argument.
2. Accept names both with and without the `.py` extension.
3. Find and execute the matching pipeline file or pipeline folder entry point.
4. Forward any remaining command-line arguments to the selected pipeline.
5. Return the selected pipeline's exit code.
6. Reject unknown or ambiguous names with a clear error.
7. List the available pipeline names when no name is supplied or help is
   requested.
8. Prevent path traversal; users must only be able to run registered pipelines
   inside the image's pipeline directory.

Design expectations
-------------------
- Define and document a predictable directory structure for pipeline tasks.
- If folders are supported, define their entry-point convention, such as
  `<pipeline_name>/main.py`.
- Avoid a long hard-coded chain of `if/elif` statements where practical.
- Keep configuration in environment variables and credentials out of the image.
- Ensure logs clearly identify the selected pipeline.
- Update the README with build commands, available pipelines, and usage examples.

Acceptance criteria
-------------------
- The Docker image builds successfully.
- The image contains at least two pipeline tasks.
- Both the extension and extensionless invocation styles work.
- Each invocation runs only the requested pipeline.
- Extra arguments are passed through to the requested pipeline.
- An invalid pipeline name exits non-zero and prints a helpful message.
- Running without a pipeline name prints usage and the available pipelines.
- Existing environment-variable configuration continues to work.
- Automated tests cover name resolution, invalid names, and argument forwarding.
- The README documents how to add and execute another pipeline.

Optional bonus
--------------
- Add a `list` command that prints all available pipelines.
- Support a pipeline registry containing names and descriptions.
- Add a `run-all` command with clear failure behavior.
- Run the container as a non-root user.




docker compose build --no-cache pipeline
docker compose run --rm pipeline postgres_pipeline