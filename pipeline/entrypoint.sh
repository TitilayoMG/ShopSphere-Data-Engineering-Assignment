#!/bin/sh
set -eu

PIPELINES_DIR="/app/pipeline"

# Pipeline registry 
# Format: "pipeline_name|description" 
PIPELINES=" 
postgres_pipeline|Extract and load PostgreSQL, MongoDB and API data 
shipment_pipeline|Extract and transform shipment data 
"

list_pipelines() { 
    echo "Available pipelines:" 
    echo 
    find "$PIPELINES_DIR" \
        -mindepth 1 \
        -maxdepth 1 \
        -type d \
        -name '*_pipeline' \
        -exec basename {} \; |
    while read -r name
    do 
        echo "$PIPELINES" | while IFS='|' read -r registry_name description
        do
            # Skip empty lines 
            [ -z "$registry_name" ] && continue 
            # Only show pipelines that actually exist 
            if  [ "$name" = "$registry_name" ]; then
                printf " %-25s %s\n" "$registry_name" "$description" 
            fi
        done
    done 
}

run_all_pipelines() {
    echo "Running all pipelines..."
    echo

    for pipeline in $(
        find "$PIPELINES_DIR" \
            -mindepth 1 \
            -maxdepth 1 \
            -type d \
            -name '*_pipeline' \
            -exec basename {} \; |
        sort
    )
    do
        echo "========================================"
        echo "Running pipeline: $pipeline"
        echo "========================================"

        if ! "$0" "$pipeline"; then
            echo
            echo "ERROR: Pipeline '$pipeline' failed."
            echo "Stopping run-all."
            return 1
        fi

        echo
        echo "Pipeline '$pipeline' completed successfully."
        echo
    done

    echo "========================================"
    echo "All pipelines completed successfully."
    echo "========================================"
}

usage() {
    echo "Usage:"
    echo "  docker run <image> <pipeline_name> [args...]"
    echo " docker run <image> list"
    echo "  docker run <image> run-all"
    echo
    list_pipelines
}

# No pipeline or help requested
if [ $# -eq 0 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ] || [ "$1" = "help" ]; then
    usage
    exit 1
fi

# List command 
if [ "$1" = "list" ]; then 
    list_pipelines 
    exit 0 
fi

# Run all pipelines
if [ "$1" = "run-all" ]; then
    run_all_pipelines
    exit $?
fi

PIPELINE="$1"
shift

# Accept names with or without .py
PIPELINE="${PIPELINE%.py}"

# Prevent path traversal
case "$PIPELINE" in
    *"/"*|*".."*)
        echo "Error: Invalid pipeline name '$PIPELINE'."
        exit 2
        ;;
esac

ENTRYPOINT_FILE="$PIPELINES_DIR/$PIPELINE/main.py"

# Reject unknown pipelines
if [ ! -f "$ENTRYPOINT_FILE" ]; then
    echo "Error: Unknown pipeline '$PIPELINE'."
    echo
    usage
    exit 2
fi

echo "========================================"
echo "Running pipeline: $PIPELINE"
echo "Entry point: $ENTRYPOINT_FILE"
echo "========================================"

exec python -m "pipeline.${PIPELINE}.main" "$@"