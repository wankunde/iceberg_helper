#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
cd "$PROJECT_ROOT"

IMAGE_TAG="${1:-iceberg-helper:latest}"
PIP_INDEX_URL="${2:-https://pypi.tuna.tsinghua.edu.cn/simple}"
PIP_TRUSTED_HOST="${3:-pypi.tuna.tsinghua.edu.cn}"

echo "[docker-build] project_root=$PROJECT_ROOT"
echo "[docker-build] image_tag=$IMAGE_TAG"
echo "[docker-build] pip_index_url=$PIP_INDEX_URL"
echo "[docker-build] pip_trusted_host=$PIP_TRUSTED_HOST"
echo "[docker-build] buildkit=1 (pip cache enabled)"

export DOCKER_BUILDKIT=1
export BUILDKIT_PROGRESS=plain

docker build \
  --build-arg PIP_INDEX_URL="$PIP_INDEX_URL" \
  --build-arg PIP_TRUSTED_HOST="$PIP_TRUSTED_HOST" \
  --network=host \
  --build-arg HTTP_PROXY=http://127.0.0.1:1080 \
  --build-arg HTTPS_PROXY=http://127.0.0.1:1080 \
  --build-arg NO_PROXY=localhost,127.0.0.1,10.0.0.0/8 \
  -t "$IMAGE_TAG" -f "$PROJECT_ROOT/Dockerfile" "$PROJECT_ROOT"

echo "[docker-build] done: $IMAGE_TAG"
