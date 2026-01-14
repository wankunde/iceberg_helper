#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./scripts/docker_start.sh /abs/path/to/table_root [image_tag] [container_name]
#
# Notes:
# - Uses --network host (Linux only)
# - Mounts the whole table root to container at /data/table (read-only)
#   so both metadata/ and data/ can be accessed inside the container.
# - Sets DEFAULT_METADATA_DIR=/data/table/metadata

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

TABLE_ROOT="${1:-}"
IMAGE_TAG="${2:-iceberg-helper:latest}"
CONTAINER_NAME="${3:-iceberg-helper}"

if [[ -z "${TABLE_ROOT}" ]]; then
  echo "ERROR: table root dir required (must contain metadata/ and data/)"
  echo "Usage: $0 /abs/path/to/table_root [image_tag] [container_name]"
  exit 2
fi

if [[ "${TABLE_ROOT:0:1}" != "/" ]]; then
  echo "ERROR: table root must be an absolute path: ${TABLE_ROOT}"
  exit 2
fi

if [[ ! -d "${TABLE_ROOT}" ]]; then
  echo "ERROR: table root not found: ${TABLE_ROOT}"
  exit 2
fi

if [[ ! -d "${TABLE_ROOT}/metadata" ]]; then
  echo "ERROR: metadata dir not found under table root: ${TABLE_ROOT}/metadata"
  exit 2
fi

# (optional) normalize to remove trailing slash for cleaner mount paths
TABLE_ROOT="${TABLE_ROOT%/}"

echo "[docker-start] image=${IMAGE_TAG}"
echo "[docker-start] name=${CONTAINER_NAME}"
echo "[docker-start] table_root=${TABLE_ROOT}"
echo "[docker-start] network=host"

# Remove existing container if any
if docker ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
  echo "[docker-start] removing existing container: ${CONTAINER_NAME}"
  docker rm -f "${CONTAINER_NAME}" >/dev/null
fi

docker run -d \
  --name "${CONTAINER_NAME}" \
  --network host \
  -e DEFAULT_METADATA_DIR="${TABLE_ROOT}/metadata" \
  -v "${TABLE_ROOT}:${TABLE_ROOT}:ro" \
  "${IMAGE_TAG}"

echo "[docker-start] started: ${CONTAINER_NAME}"
echo "[docker-start] default metadata dir: ${TABLE_ROOT}/metadata"
echo "[docker-start] logs: docker logs -f ${CONTAINER_NAME}"
