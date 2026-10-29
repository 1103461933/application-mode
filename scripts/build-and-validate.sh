#!/usr/bin/env bash
set -euo pipefail
IMAGE_NAME="${IMAGE_NAME:-flink-reference-jobs}"
IMAGE_TAG="${IMAGE_TAG:-2.2.0-dev}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "1/3 - Python syntax validation"
python3 -m compileall -q application/flink/src

echo "2/3 - Unit tests"
python3 -m unittest discover -s tests/unit -v

echo "3/3 - Docker image build"
docker build -f application/flink/Dockerfile -t "${IMAGE_NAME}:${IMAGE_TAG}" .
echo "Built ${IMAGE_NAME}:${IMAGE_TAG}"
echo "NOTE: image build success does not prove Flink/Kafka/Couchbase end-to-end compatibility."
