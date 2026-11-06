#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARCH="${1:-${LAMBDA_ARCHITECTURE:-x86_64}}"
IMAGE_NAME="${IMAGE_NAME:-duckdb-aws-lambda}"
IMAGE_TAG="${IMAGE_TAG:-local}"

case "${ARCH}" in
  x86_64) PLATFORM="linux/amd64" ;;
  arm64) PLATFORM="linux/arm64" ;;
  *)
    echo "Unsupported architecture '${ARCH}'. Use x86_64 or arm64." >&2
    exit 1
    ;;
esac

docker build \
  --platform "${PLATFORM}" \
  -f "${ROOT}/docker/Dockerfile" \
  -t "${IMAGE_NAME}:${IMAGE_TAG}" \
  "${ROOT}"

echo "Built ${IMAGE_NAME}:${IMAGE_TAG} for ${PLATFORM}"
