"""
Sales Data Pipeline DAG (Refactored)

This DAG processes sales CSV files from MinIO, validates and cleans the data
using Pandera schemas, then loads it into PostgreSQL for analytics.

Architecture:
- Uses modular service classes for MinIO and PostgreSQL operations
- Pandera schemas for data validation at each stage
- File-based logging for production monitoring
- Separation of concerns between DAG definition and business logic
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from services.minio_service import MinIOService
from services.postgres_service import PostgresService
from transformers.data_cleaner import DataCleaner
from transformers.data_enricher import DataEnricher

# Import modular components
from utils.logging_config import PipelineLogger, get_logger, setup_logging
from validation.validators import DataValidator

from airflow import DAG

# Initialize logging
setup_logging()
logger = get_logger("sales_pipeline")

# DAG default arguments
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def detect_new_files(**context) -> List[str]:
    """
    Detect new CSV files in MinIO raw-sales bucket.

    Takes a snapshot of all CSV files currently present so that only
    these exact files are processed and archived in this run.
    Files arriving after this point will be picked up by the next run.

    Returns:
        List of CSV file names found
    """
    with PipelineLogger("detect_new_files", **context) as log:
        minio_service = MinIOService()

        try:
            csv_files = minio_service.detect_new_files()

            if not csv_files:
                log.info("No CSV files found in raw-sales bucket")
                return []

            log.info(f"Found {len(csv_files)} CSV file(s): {csv_files}")

            # Push snapshot to XCom — downstream tasks use ONLY this list
            context["ti"].xcom_push(key="csv_files", value=csv_files)
            return csv_files

        except Exception as e:
            log.error(f"Error detecting files: {e}")
            raise


def check_files_exist(**context) -> bool:
    """
    ShortCircuit: skip downstream tasks when no files are found.

    Returns:
        True if files exist (continue pipeline), False to short-circuit.
    """
    ti = context["ti"]
    csv_files = ti.xcom_pull(task_ids="detect_new_files", key="csv_files")
    return bool(csv_files)


def validate_schema(**context) -> List[str]:
    """
    Validate CSV schema and structure using Pandera.

    Returns:
        List of valid file names
    """
    with PipelineLogger("validate_schema", **context) as log:
        ti = context["ti"]
        csv_files = ti.xcom_pull(task_ids="detect_new_files", key="csv_files")

        if not csv_files:
            log.info("No files to validate")
            return []

        minio_service = MinIOService()
        validator = DataValidator()

        valid_files = []
        invalid_files = []

        for file_name in csv_files:
            try:
                # Download CSV
                df, metadata = minio_service.download_csv_as_dataframe(file_name)

                # Validate with Pandera
                result = validator.validate_raw_data(df, file_name)

                if result.is_valid:
                    valid_files.append(file_name)
                    log.info(f"File {file_name} passed validation")
                else:
                    invalid_files.append(
                        {
                            "file": file_name,
                            "errors": result.errors[:5],  # Limit error details
                            "error_count": len(result.errors),
                        }
                    )
                    log.warning(
                        f"File {file_name} failed validation: " f"{len(result.errors)} error(s)"
                    )

            except Exception as e:
                log.error(f"Error validating {file_name}: {e}")
                invalid_files.append({"file": file_name, "error": str(e)})

        # Push results to XCom
        ti.xcom_push(key="valid_files", value=valid_files)
        ti.xcom_push(key="invalid_files", value=invalid_files)

        log.info(f"Validation complete: {len(valid_files)} valid, " f"{len(invalid_files)} invalid")

        return valid_files


def clean_data(**context) -> List[Dict[str, Any]]:
    """
    Clean and transform sales data using DataCleaner.

    Returns:
        List of cleaned data dictionaries
    """
    with PipelineLogger("clean_data", **context) as log:
        ti = context["ti"]
        valid_files = ti.xcom_pull(task_ids="validate_schema", key="valid_files")

        if not valid_files:
            log.info("No valid files to clean")
            return []

        minio_service = MinIOService()
        cleaner = DataCleaner()
        validator = DataValidator()

        cleaned_data = []

        for file_name in valid_files:
            try:
                # Download CSV
                df, metadata = minio_service.download_csv_as_dataframe(file_name)
                original_count = len(df)

                # Clean the data
                df_cleaned, cleaning_result = cleaner.clean(df)

                # Validate cleaned data
                validation_result, df_validated = validator.validate_cleaned_data(
                    df_cleaned, original_count=original_count
                )

                if not validation_result.is_valid:
                    log.warning(f"Cleaned data from {file_name} failed post-clean validation")
                    # Continue anyway - data is usable

                # Convert datetime columns to ISO strings for JSON serialization
                df_serializable = df_validated.copy()
                for col in df_serializable.select_dtypes(
                    include=["datetime64", "datetime64[ns]"]
                ).columns:
                    df_serializable[col] = df_serializable[col].dt.strftime("%Y-%m-%d")

                cleaned_data.append(
                    {
                        "file_name": file_name,
                        "data": df_serializable.to_dict("records"),
                        "original_count": original_count,
                        "cleaned_count": len(df_validated),
                        "cleaning_stats": cleaning_result.to_dict(),
                    }
                )

                log.info(f"Cleaned {file_name}: {original_count} -> " f"{len(df_validated)} rows")

            except Exception as e:
                log.error(f"Error cleaning {file_name}: {e}")
                raise

        ti.xcom_push(key="cleaned_data", value=cleaned_data)
        return cleaned_data


def compute_totals(**context) -> List[Dict[str, Any]]:
    """
    Compute total_amount and add enrichment fields using DataEnricher.

    Returns:
        List of enriched data dictionaries
    """
    with PipelineLogger("compute_totals", **context) as log:
        ti = context["ti"]
        cleaned_data = ti.xcom_pull(task_ids="clean_data", key="cleaned_data")

        if not cleaned_data:
            log.info("No data to compute totals")
            return []

        enricher = DataEnricher()
        validator = DataValidator()

        enriched_data = []

        for item in cleaned_data:
            df = pd.DataFrame(item["data"])

            # Enrich the data
            df_enriched, enrichment_result = enricher.enrich(df)

            # Validate enriched data
            validation_result, df_validated = validator.validate_enriched_data(df_enriched)

            if not validation_result.is_valid:
                log.warning(
                    f"Enriched data from {item['file_name']} failed validation: "
                    f"{len(validation_result.errors)} error(s)"
                )

            # Convert datetime columns to ISO strings for JSON serialization
            df_serializable = df_validated.copy()
            for col in df_serializable.select_dtypes(
                include=["datetime64", "datetime64[ns]"]
            ).columns:
                df_serializable[col] = df_serializable[col].dt.strftime("%Y-%m-%d %H:%M:%S")

            enriched_item = {
                "file_name": item["file_name"],
                "data": df_serializable.to_dict("records"),
                "original_count": item["original_count"],
                "enriched_count": len(df_validated),
                "total_revenue": enrichment_result.total_revenue,
                "enrichment_stats": enrichment_result.to_dict(),
            }

            enriched_data.append(enriched_item)

            log.info(
                f"Enriched {item['file_name']}: "
                f"${enrichment_result.total_revenue:,.2f} total revenue"
            )

        ti.xcom_push(key="enriched_data", value=enriched_data)
        return enriched_data


def load_to_postgres(**context) -> Dict[str, Any]:
    """
    Load enriched data into PostgreSQL analytics database.

    Returns:
        Load statistics dictionary
    """
    with PipelineLogger("load_to_postgres", **context) as log:
        ti = context["ti"]
        enriched_data = ti.xcom_pull(task_ids="compute_totals", key="enriched_data")

        if not enriched_data:
            log.info("No data to load")
            return {"total_inserted": 0}

        postgres_service = PostgresService()

        # Health check
        if not postgres_service.health_check():
            raise RuntimeError("PostgreSQL health check failed")

        total_inserted = 0
        total_failed = 0

        for item in enriched_data:
            df = pd.DataFrame(item["data"])

            load_result = postgres_service.load_sales_data(df)

            total_inserted += load_result.rows_inserted
            total_failed += load_result.rows_failed

            log.info(
                f"Loaded {load_result.rows_inserted} rows from {item['file_name']} "
                f"({load_result.rows_failed} failed)"
            )

        result = {
            "total_inserted": total_inserted,
            "total_failed": total_failed,
            "success": total_failed == 0,
        }

        ti.xcom_push(key="load_result", value=result)

        log.info(f"Load complete: {total_inserted} rows inserted, {total_failed} failed")
        return result


def archive_processed_files(**context) -> Dict[str, Any]:
    """
    Move processed files to archive bucket.

    Returns:
        Archive statistics dictionary
    """
    with PipelineLogger("archive_processed_files", **context) as log:
        ti = context["ti"]
        valid_files = ti.xcom_pull(task_ids="validate_schema", key="valid_files")

        if not valid_files:
            log.info("No files to archive")
            return {"archived": 0}

        minio_service = MinIOService()

        archived_files = minio_service.archive_files(valid_files)

        result = {
            "archived": len(archived_files),
            "total_files": len(valid_files),
            "archived_paths": archived_files,
        }

        ti.xcom_push(key="archive_result", value=result)

        log.info(f"Archived {len(archived_files)} of {len(valid_files)} files")
        return result


def generate_summary(**context) -> Dict[str, Any]:
    """
    Generate pipeline execution summary.

    Returns:
        Summary statistics dictionary
    """
    with PipelineLogger("generate_summary", **context) as log:
        ti = context["ti"]

        # Collect results from all tasks
        csv_files = ti.xcom_pull(task_ids="detect_new_files", key="csv_files") or []
        valid_files = ti.xcom_pull(task_ids="validate_schema", key="valid_files") or []
        invalid_files = ti.xcom_pull(task_ids="validate_schema", key="invalid_files") or []
        load_result = ti.xcom_pull(task_ids="load_to_postgres", key="load_result") or {}
        archive_result = (
            ti.xcom_pull(task_ids="archive_processed_files", key="archive_result") or {}
        )

        summary = {
            "run_id": context.get("run_id", "unknown"),
            "execution_date": str(context.get("execution_date", "")),
            "files_detected": len(csv_files),
            "files_valid": len(valid_files),
            "files_invalid": len(invalid_files),
            "rows_loaded": load_result.get("total_inserted", 0),
            "rows_failed": load_result.get("total_failed", 0),
            "files_archived": archive_result.get("archived", 0),
            "status": "SUCCESS" if load_result.get("success", True) else "PARTIAL_SUCCESS",
        }

        log.info(f"Pipeline summary: {summary}")
        return summary


# Define the DAG
with DAG(
    "sales_data_pipeline",
    default_args=default_args,
    description="Process sales CSV files from MinIO to PostgreSQL with Pandera validation",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs so files aren't double-processed
    tags=["sales", "etl", "production", "pandera"],
) as dag:

    # Task definitions
    start = EmptyOperator(task_id="start")

    detect_files = PythonOperator(
        task_id="detect_new_files",
        python_callable=detect_new_files,
    )

    # Short-circuit: skip the rest when no files are found
    check_files = ShortCircuitOperator(
        task_id="check_files_exist",
        python_callable=check_files_exist,
    )

    validate = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema,
    )

    clean = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
    )

    compute = PythonOperator(
        task_id="compute_totals",
        python_callable=compute_totals,
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    archive = PythonOperator(
        task_id="archive_processed_files",
        python_callable=archive_processed_files,
    )

    summary = PythonOperator(
        task_id="generate_summary",
        python_callable=generate_summary,
    )

    end = EmptyOperator(task_id="end")

    # Define task dependencies
    # detect → check (short-circuit if empty) → validate → ... → end
    (
        start
        >> detect_files
        >> check_files
        >> validate
        >> clean
        >> compute
        >> load
        >> archive
        >> summary
        >> end
    )
