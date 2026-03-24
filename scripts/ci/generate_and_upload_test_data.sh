#!/usr/bin/env bash
set -euo pipefail

cd data-generator
python generate_sales.py --records 50 --error-rate 0.05 --output test_flow.csv
python3 ../scripts/ci/upload_minio_object.py \
  --bucket raw-sales \
  --object-name test_flow.csv \
  --file-path test_flow.csv
