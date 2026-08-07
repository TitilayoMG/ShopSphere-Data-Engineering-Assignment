#!/bin/sh
set -eu

PIPELINES_DIR="/app"

usage() {
    echo "Usage:"
    echo "  docker run <image> <pipeline_name> [args...]"
    echo
    echo "Available pipelines:"
    find "$PIPELINES_DIR" -mindepth 1 -maxdepth 1 -type d -exec basename {} \; | sort
}

# No pipeline or help requested
if [ $# -eq 0 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ] || [ "$1" = "help" ]; then
    usage
    exit 1
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

exec python "$ENTRYPOINT_FILE" "$@"

# docker run --network shopsphere-net your-image shipments_pipeline