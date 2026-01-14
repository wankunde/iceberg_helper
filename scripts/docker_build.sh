#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
cd "$PROJECT_ROOT"

IMAGE_TAG="${1:-iceberg-helper:latest}"

echo "[docker-build] project_root=$PROJECT_ROOT"
echo "[docker-build] image_tag=$IMAGE_TAG"

docker build -t "$IMAGE_TAG" -f "$PROJECT_ROOT/Dockerfile" "$PROJECT_ROOT"

echo "[docker-build] done: $IMAGE_TAG"
