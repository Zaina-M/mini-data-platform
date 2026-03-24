#!/usr/bin/env bash
set -euo pipefail

bash scripts/ci/check_sales_row_count.sh
bash scripts/ci/print_sales_analytics_samples.sh
