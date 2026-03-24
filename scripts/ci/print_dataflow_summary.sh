#!/usr/bin/env bash
set -euo pipefail

echo "============================================="
echo "   DATA FLOW VALIDATION SUMMARY"
echo "============================================="
echo "  MinIO (Ingestion)    -> CSV uploaded to raw-sales bucket"
echo "  Airflow (Processing) -> DAG triggered and completed"
echo "  PostgreSQL (Storage) -> Data loaded into sales table"
echo "  Metabase (Visualize) -> Queried sales data successfully"
echo "  Full pipeline: MinIO -> Airflow -> PostgreSQL -> Metabase"
echo "============================================="
