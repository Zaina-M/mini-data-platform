# """
# PostgreSQL Service Module

# Provides high-level operations for PostgreSQL database:
# - Data loading with upsert support
# - Query execution
# - Table operations
# """

# import time
# from dataclasses import dataclass
# from datetime import datetime
# from typing import Any, Dict, List, Optional, Tuple

# import pandas as pd
# from utils.connections import postgres_connection
# from utils.logging_config import get_logger

# logger = get_logger("postgres_service")


# @dataclass
# class LoadResult:
#     """Result of a data load operation."""

#     success: bool
#     rows_inserted: int
#     rows_updated: int
#     rows_failed: int
#     duration_seconds: float
#     error_message: Optional[str] = None

#     @property
#     def total_processed(self) -> int:
#         return self.rows_inserted + self.rows_updated

#     def to_dict(self) -> dict:
#         return {
#             "success": self.success,
#             "rows_inserted": self.rows_inserted,
#             "rows_updated": self.rows_updated,
#             "rows_failed": self.rows_failed,
#             "total_processed": self.total_processed,
#             "duration_seconds": round(self.duration_seconds, 4),
#             "error_message": self.error_message,
#         }


# class PostgresService:
#     """
#     Service class for PostgreSQL operations.

#     Provides high-level methods for common database operations
#     used in the data pipeline.
#     """

#     SALES_TABLE = "sales"

#     SALES_COLUMNS = [
#         "order_id",
#         "product_name",
#         "quantity",
#         "unit_price",
#         "order_date",
#         "customer_id",
#         "country",
#         "total_amount",
#         "ingestion_timestamp",
#     ]

#     UPSERT_QUERY = """
#         INSERT INTO sales (
#             order_id, product_name, quantity, unit_price,
#             order_date, customer_id, country, total_amount, ingestion_timestamp
#         ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
#         ON CONFLICT (order_id) DO UPDATE SET
#             product_name = EXCLUDED.product_name,
#             quantity = EXCLUDED.quantity,
#             unit_price = EXCLUDED.unit_price,
#             order_date = EXCLUDED.order_date,
#             customer_id = EXCLUDED.customer_id,
#             country = EXCLUDED.country,
#             total_amount = EXCLUDED.total_amount,
#             ingestion_timestamp = EXCLUDED.ingestion_timestamp
#     """

#     def __init__(self):
#         """Initialize PostgreSQL service."""
#         pass

#     ROW_MAX_RETRIES = 3

#     def _insert_row_with_retry(self, cursor, row, idx: int) -> bool:
#         """
#         Insert a single row with up to ROW_MAX_RETRIES attempts.

#         Returns True on success, False after all retries exhausted.
#         """
#         for attempt in range(1, self.ROW_MAX_RETRIES + 1):
#             try:
#                 cursor.execute(
#                     self.UPSERT_QUERY,
#                     (
#                         row["order_id"],
#                         row["product_name"],
#                         int(row["quantity"]),
#                         float(row["unit_price"]),
#                         row["order_date"],
#                         row["customer_id"],
#                         row["country"],
#                         float(row["total_amount"]),
#                         row["ingestion_timestamp"],
#                     ),
#                 )
#                 return True
#             except Exception as e:
#                 if attempt < self.ROW_MAX_RETRIES:
#                     logger.warning(
#                         f"Row {idx} (order_id={row.get('order_id', '?')}) "
#                         f"attempt {attempt}/{self.ROW_MAX_RETRIES} failed: {e}  — retrying"
#                     )
#                     time.sleep(0.1 * attempt)  # brief back-off
#                 else:
#                     logger.error(
#                         f"Row {idx} (order_id={row.get('order_id', '?')}) "
#                         f"REJECTED after {self.ROW_MAX_RETRIES} attempts: {e}"
#                     )
#         return False

#     def load_sales_data(self, df: pd.DataFrame, batch_size: int = 1000) -> LoadResult:
#         """
#         Load sales data into PostgreSQL with upsert.

#         Each row is retried up to ROW_MAX_RETRIES times. Permanently
#         failed rows are logged and skipped so the rest of the batch
#         continues successfully.

#         Args:
#             df: DataFrame with sales data
#             batch_size: Number of rows per batch

#         Returns:
#             LoadResult with operation details
#         """
#         start_time = datetime.now()
#         rows_processed = 0
#         rows_failed = 0
#         failed_order_ids: List[str] = []

#         logger.info(f"Starting load of {len(df)} rows to {self.SALES_TABLE}")

#         try:
#             with postgres_connection() as (conn, cursor):
#                 for idx, row in df.iterrows():
#                     if self._insert_row_with_retry(cursor, row, idx):
#                         rows_processed += 1
#                     else:
#                         rows_failed += 1
#                         failed_order_ids.append(str(row.get("order_id", "unknown")))

#                     # Log progress periodically
#                     total_done = rows_processed + rows_failed
#                     if total_done % batch_size == 0:
#                         logger.info(f"Progress: {total_done}/{len(df)} rows ({rows_failed} failed)")

#                 # Commit happens automatically via context manager

#             duration = (datetime.now() - start_time).total_seconds()

#             if failed_order_ids:
#                 logger.warning(
#                     f"{rows_failed} row(s) permanently failed after "
#                     f"{self.ROW_MAX_RETRIES} retries each: {failed_order_ids}"
#                 )

#             result = LoadResult(
#                 success=True,
#                 rows_inserted=rows_processed,
#                 rows_updated=0,
#                 rows_failed=rows_failed,
#                 duration_seconds=duration,
#             )

#             rate = rows_processed / duration if duration > 0 else float("inf")
#             logger.info(
#                 f"Load completed: {rows_processed} inserted, {rows_failed} failed "
#                 f"in {duration:.2f}s ({rate:.0f} rows/sec)"
#             )

#             return result

#         except Exception as e:
#             duration = (datetime.now() - start_time).total_seconds()
#             logger.error(f"Load failed: {e}")

#             return LoadResult(
#                 success=False,
#                 rows_inserted=rows_processed,
#                 rows_updated=0,
#                 rows_failed=len(df) - rows_processed,
#                 duration_seconds=duration,
#                 error_message=str(e),
#             )

#     def load_batch(self, records: List[Dict[str, Any]]) -> LoadResult:
#         """
#         Load a batch of records efficiently using executemany.

#         Args:
#             records: List of record dictionaries

#         Returns:
#             LoadResult with operation details
#         """
#         start_time = datetime.now()

#         if not records:
#             return LoadResult(
#                 success=True, rows_inserted=0, rows_updated=0, rows_failed=0, duration_seconds=0
#             )

#         logger.info(f"Loading batch of {len(records)} records")

#         try:
#             with postgres_connection() as (conn, cursor):
#                 values = [
#                     (
#                         r["order_id"],
#                         r["product_name"],
#                         int(r["quantity"]),
#                         float(r["unit_price"]),
#                         r["order_date"],
#                         r["customer_id"],
#                         r["country"],
#                         float(r["total_amount"]),
#                         r["ingestion_timestamp"],
#                     )
#                     for r in records
#                 ]

#                 cursor.executemany(self.UPSERT_QUERY, values)

#             duration = (datetime.now() - start_time).total_seconds()

#             return LoadResult(
#                 success=True,
#                 rows_inserted=len(records),
#                 rows_updated=0,
#                 rows_failed=0,
#                 duration_seconds=duration,
#             )

#         except Exception as e:
#             duration = (datetime.now() - start_time).total_seconds()
#             logger.error(f"Batch load failed: {e}")

#             return LoadResult(
#                 success=False,
#                 rows_inserted=0,
#                 rows_updated=0,
#                 rows_failed=len(records),
#                 duration_seconds=duration,
#                 error_message=str(e),
#             )

#     def get_row_count(self) -> int:
#         """Get current row count in sales table."""
#         try:
#             with postgres_connection() as (conn, cursor):
#                 cursor.execute(f"SELECT COUNT(*) FROM {self.SALES_TABLE}")
#                 count = cursor.fetchone()[0]
#                 return count
#         except Exception as e:
#             logger.error(f"Error getting row count: {e}")
#             return -1

#     def get_date_range(self) -> Tuple[Optional[datetime], Optional[datetime]]:
#         """Get min and max order dates in sales table."""
#         try:
#             with postgres_connection() as (conn, cursor):
#                 cursor.execute(f"SELECT MIN(order_date), MAX(order_date) FROM {self.SALES_TABLE}")
#                 result = cursor.fetchone()
#                 return result[0], result[1]
#         except Exception as e:
#             logger.error(f"Error getting date range: {e}")
#             return None, None

#     def get_summary_stats(self) -> Dict[str, Any]:
#         """Get summary statistics from sales table."""
#         try:
#             with postgres_connection() as (conn, cursor):
#                 cursor.execute(f"""
#                     SELECT
#                         COUNT(*) as total_orders,
#                         SUM(total_amount) as total_revenue,
#                         AVG(total_amount) as avg_order_value,
#                         COUNT(DISTINCT customer_id) as unique_customers,
#                         COUNT(DISTINCT country) as countries,
#                         MIN(order_date) as first_order,
#                         MAX(order_date) as last_order
#                     FROM {self.SALES_TABLE}
#                 """)

#                 row = cursor.fetchone()

#                 return {
#                     "total_orders": row[0] or 0,
#                     "total_revenue": float(row[1] or 0),
#                     "avg_order_value": float(row[2] or 0),
#                     "unique_customers": row[3] or 0,
#                     "countries": row[4] or 0,
#                     "first_order": str(row[5]) if row[5] else None,
#                     "last_order": str(row[6]) if row[6] else None,
#                 }

#         except Exception as e:
#             logger.error(f"Error getting summary stats: {e}")
#             return {}

#     def check_order_exists(self, order_id: str) -> bool:
#         """Check if an order already exists."""
#         try:
#             with postgres_connection() as (conn, cursor):
#                 cursor.execute(
#                     f"SELECT 1 FROM {self.SALES_TABLE} WHERE order_id = %s LIMIT 1", (order_id,)
#                 )
#                 return cursor.fetchone() is not None
#         except Exception as e:
#             logger.error(f"Error checking order existence: {e}")
#             return False

#     def delete_orders_by_date(self, before_date: datetime) -> int:
#         """
#         Delete orders before a specified date.

#         Args:
#             before_date: Delete orders before this date

#         Returns:
#             Number of rows deleted
#         """
#         try:
#             with postgres_connection() as (conn, cursor):
#                 cursor.execute(
#                     f"DELETE FROM {self.SALES_TABLE} WHERE order_date < %s", (before_date,)
#                 )
#                 deleted = cursor.rowcount
#                 logger.info(f"Deleted {deleted} orders before {before_date}")
#                 return deleted
#         except Exception as e:
#             logger.error(f"Error deleting orders: {e}")
#             return 0

#     def health_check(self) -> bool:
#         """Check database connectivity and table existence."""
#         try:
#             with postgres_connection() as (conn, cursor):
#                 cursor.execute(
#                     """
#                     SELECT EXISTS (
#                         SELECT FROM information_schema.tables
#                         WHERE table_name = %s
#                     )
#                 """,
#                     (self.SALES_TABLE,),
#                 )
#                 exists = cursor.fetchone()[0]

#                 if not exists:
#                     logger.warning(f"Table {self.SALES_TABLE} does not exist")
#                     return False

#                 logger.info("PostgreSQL health check passed")
#                 return True

#         except Exception as e:
#             logger.error(f"PostgreSQL health check failed: {e}")
#             return False


"""
Improved PostgreSQL Service Module

Enhancements:
- Bulk loading using staging table
- Data validation layer
- Dead-letter queue for failed records
- Better transaction handling
- Cleaner structure and logging
"""

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from utils.connections import postgres_connection
from utils.logging_config import get_logger

logger = get_logger("postgres_service")


# =========================
# RESULT MODEL
# =========================
@dataclass
class LoadResult:
    success: bool
    rows_inserted: int
    rows_updated: int
    rows_failed: int
    duration_seconds: float
    error_message: Optional[str] = None

    @property
    def total_processed(self) -> int:
        return self.rows_inserted + self.rows_updated


# =========================
# VALIDATION LAYER
# =========================
def validate_record(record: Dict[str, Any]) -> bool:
    try:
        if not record.get("order_id"):
            return False
        if int(record["quantity"]) <= 0:
            return False
        if float(record["unit_price"]) <= 0:
            return False
        if not record.get("customer_id"):
            return False
        if not record.get("country"):
            return False
        return True
    except Exception:
        return False


# =========================
# MAIN SERVICE
# =========================
class PostgresService:
    SALES_TABLE = "sales"
    STAGING_TABLE = "sales_staging"
    ERROR_TABLE = "sales_errors"

    def __init__(self):
        pass

    # =========================
    # CORE LOAD FUNCTION (IMPROVED)
    # =========================
    def load_sales_data(self, df: pd.DataFrame) -> LoadResult:
        start_time = datetime.now()

        valid_records = []
        invalid_records = []

        # -------------------------
        # 1. VALIDATION STEP
        # -------------------------
        for record in df.to_dict(orient="records"):
            if validate_record(record):
                valid_records.append(record)
            else:
                invalid_records.append(record)

        logger.info(f"Valid: {len(valid_records)}, Invalid: {len(invalid_records)}")

        try:
            with postgres_connection() as (conn, cursor):

                # -------------------------
                # 2. LOAD INTO STAGING TABLE
                # -------------------------
                self._truncate_staging(cursor)

                insert_query = f"""
                    INSERT INTO {self.STAGING_TABLE} (
                        order_id, product_name, quantity, unit_price,
                        order_date, customer_id, country,
                        total_amount, ingestion_timestamp
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """

                values = [
                    (
                        r["order_id"],
                        r["product_name"],
                        int(r["quantity"]),
                        float(r["unit_price"]),
                        r["order_date"],
                        r["customer_id"],
                        r["country"],
                        float(r["total_amount"]),
                        r["ingestion_timestamp"],
                    )
                    for r in valid_records
                ]

                cursor.executemany(insert_query, values)

                # -------------------------
                # 3. UPSERT FROM STAGING → MAIN
                # -------------------------
                upsert_query = f"""
                    INSERT INTO {self.SALES_TABLE} AS target
                    SELECT * FROM {self.STAGING_TABLE}
                    ON CONFLICT (order_id) DO UPDATE SET
                        product_name = EXCLUDED.product_name,
                        quantity = EXCLUDED.quantity,
                        unit_price = EXCLUDED.unit_price,
                        order_date = EXCLUDED.order_date,
                        customer_id = EXCLUDED.customer_id,
                        country = EXCLUDED.country,
                        total_amount = EXCLUDED.total_amount,
                        ingestion_timestamp = EXCLUDED.ingestion_timestamp
                """

                cursor.execute(upsert_query)

                # -------------------------
                # 4. DEAD LETTER QUEUE
                # -------------------------
                if invalid_records:
                    self._store_failed_records(cursor, invalid_records)

                conn.commit()

            duration = (datetime.now() - start_time).total_seconds()

            return LoadResult(
                success=True,
                rows_inserted=len(valid_records),
                rows_updated=0,  # Could be improved with tracking
                rows_failed=len(invalid_records),
                duration_seconds=duration,
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Load failed: {e}")

            return LoadResult(
                success=False,
                rows_inserted=0,
                rows_updated=0,
                rows_failed=len(df),
                duration_seconds=duration,
                error_message=str(e),
            )

    # =========================
    # STAGING MANAGEMENT
    # =========================
    def _truncate_staging(self, cursor):
        cursor.execute(f"TRUNCATE TABLE {self.STAGING_TABLE}")

    # =========================
    # DEAD LETTER QUEUE
    # =========================
    def _store_failed_records(self, cursor, records: List[Dict[str, Any]]):
        logger.warning(f"Storing {len(records)} failed records")

        insert_query = f"""
            INSERT INTO {self.ERROR_TABLE} (
                raw_data, error_timestamp
            ) VALUES (%s, %s)
        """

        values = [
            (str(record), datetime.now())
            for record in records
        ]

        cursor.executemany(insert_query, values)

    # =========================
    # HEALTH CHECK
    # =========================
    def health_check(self) -> bool:
        try:
            with postgres_connection() as (conn, cursor):
                cursor.execute("SELECT 1")
                return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    # =========================
    # SIMPLE ANALYTICS
    # =========================
    def get_summary_stats(self) -> Dict[str, Any]:
        try:
            with postgres_connection() as (conn, cursor):
                cursor.execute(f"""
                    SELECT
                        COUNT(*),
                        SUM(total_amount),
                        AVG(total_amount)
                    FROM {self.SALES_TABLE}
                """)
                row = cursor.fetchone()

                return {
                    "total_orders": row[0] or 0,
                    "total_revenue": float(row[1] or 0),
                    "avg_order_value": float(row[2] or 0),
                }
        except Exception as e:
            logger.error(f"Error fetching stats: {e}")
            return {}