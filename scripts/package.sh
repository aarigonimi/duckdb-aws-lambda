#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-}}"
if [[ -z "${AWS_REGION}" ]]; then
  echo "Set AWS_REGION before pushing the image." >&2
  exit 1
fi

if [[ -z "${ECR_REPOSITORY_URI:-}" ]]; then
  echo "Set ECR_REPOSITORY_URI to the repository URL from 'terraform output ecr_repository_url'." >&2
  exit 1
fi

ARCH="${LAMBDA_ARCHITECTURE:-x86_64}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

"${ROOT}/scripts/build.sh" "${ARCH}"

aws ecr get-login-password --region "${AWS_REGION}" \
  | docker login --username AWS --password-stdin "${ECR_REPOSITORY_URI%/*}"

docker tag "duckdb-aws-lambda:local" "${ECR_REPOSITORY_URI}:${IMAGE_TAG}"
docker push "${ECR_REPOSITORY_URI}:${IMAGE_TAG}"

echo "Pushed ${ECR_REPOSITORY_URI}:${IMAGE_TAG}"
