"""
PostgreSQL Service Module

Provides high-level operations for PostgreSQL database:
- Data loading with upsert support
- Query execution
- Table operations
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from utils.connections import postgres_connection
from utils.logging_config import get_logger

logger = get_logger("postgres_service")


@dataclass
class LoadResult:
    """Result of a data load operation."""

    success: bool
    rows_inserted: int
    rows_updated: int
    rows_failed: int
    duration_seconds: float
    error_message: Optional[str] = None

    @property
    def total_processed(self) -> int:
        return self.rows_inserted + self.rows_updated

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_failed": self.rows_failed,
            "total_processed": self.total_processed,
            "duration_seconds": round(self.duration_seconds, 4),
            "error_message": self.error_message,
        }


class PostgresService:
    """
    Service class for PostgreSQL operations.

    Provides high-level methods for common database operations
    used in the data pipeline.
    """

    SALES_TABLE = "sales"

    SALES_COLUMNS = [
        "order_id",
        "product_name",
        "quantity",
        "unit_price",
        "order_date",
        "customer_id",
        "country",
        "total_amount",
        "ingestion_timestamp",
    ]

    UPSERT_QUERY = """
        INSERT INTO sales (
            order_id, product_name, quantity, unit_price,
            order_date, customer_id, country, total_amount, ingestion_timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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

    def __init__(self):
        """Initialize PostgreSQL service."""
        pass

    def load_sales_data(self, df: pd.DataFrame, batch_size: int = 1000) -> LoadResult:
        """
        Load sales data into PostgreSQL with upsert.

        Args:
            df: DataFrame with sales data
            batch_size: Number of rows per batch

        Returns:
            LoadResult with operation details
        """
        start_time = datetime.now()
        rows_processed = 0
        rows_failed = 0

        logger.info(f"Starting load of {len(df)} rows to {self.SALES_TABLE}")

        try:
            with postgres_connection() as (conn, cursor):
                for idx, row in df.iterrows():
                    try:
                        cursor.execute(
                            self.UPSERT_QUERY,
                            (
                                row["order_id"],
                                row["product_name"],
                                int(row["quantity"]),
                                float(row["unit_price"]),
                                row["order_date"],
                                row["customer_id"],
                                row["country"],
                                float(row["total_amount"]),
                                row["ingestion_timestamp"],
                            ),
                        )
                        rows_processed += 1

                        # Log progress periodically
                        if rows_processed % batch_size == 0:
                            logger.info(f"Processed {rows_processed}/{len(df)} rows")

                    except Exception as e:
                        logger.warning(f"Failed to insert row {idx}: {e}")
                        rows_failed += 1

                # Commit happens automatically via context manager

            duration = (datetime.now() - start_time).total_seconds()

            result = LoadResult(
                success=True,
                rows_inserted=rows_processed,  # Can't distinguish insert vs update easily
                rows_updated=0,
                rows_failed=rows_failed,
                duration_seconds=duration,
            )

            rate = rows_processed / duration if duration > 0 else float("inf")
            logger.info(
                f"Load completed: {rows_processed} rows in {duration:.2f}s "
                f"({rate:.0f} rows/sec)"
            )

            return result

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Load failed: {e}")

            return LoadResult(
                success=False,
                rows_inserted=rows_processed,
                rows_updated=0,
                rows_failed=len(df) - rows_processed,
                duration_seconds=duration,
                error_message=str(e),
            )

    def load_batch(self, records: List[Dict[str, Any]]) -> LoadResult:
        """
        Load a batch of records efficiently using executemany.

        Args:
            records: List of record dictionaries

        Returns:
            LoadResult with operation details
        """
        start_time = datetime.now()

        if not records:
            return LoadResult(
                success=True, rows_inserted=0, rows_updated=0, rows_failed=0, duration_seconds=0
            )

        logger.info(f"Loading batch of {len(records)} records")

        try:
            with postgres_connection() as (conn, cursor):
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
                    for r in records
                ]

                cursor.executemany(self.UPSERT_QUERY, values)

            duration = (datetime.now() - start_time).total_seconds()

            return LoadResult(
                success=True,
                rows_inserted=len(records),
                rows_updated=0,
                rows_failed=0,
                duration_seconds=duration,
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"Batch load failed: {e}")

            return LoadResult(
                success=False,
                rows_inserted=0,
                rows_updated=0,
                rows_failed=len(records),
                duration_seconds=duration,
                error_message=str(e),
            )

    def get_row_count(self) -> int:
        """Get current row count in sales table."""
        try:
            with postgres_connection() as (conn, cursor):
                cursor.execute(f"SELECT COUNT(*) FROM {self.SALES_TABLE}")
                count = cursor.fetchone()[0]
                return count
        except Exception as e:
            logger.error(f"Error getting row count: {e}")
            return -1

    def get_date_range(self) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Get min and max order dates in sales table."""
        try:
            with postgres_connection() as (conn, cursor):
                cursor.execute(f"SELECT MIN(order_date), MAX(order_date) FROM {self.SALES_TABLE}")
                result = cursor.fetchone()
                return result[0], result[1]
        except Exception as e:
            logger.error(f"Error getting date range: {e}")
            return None, None

    def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics from sales table."""
        try:
            with postgres_connection() as (conn, cursor):
                cursor.execute(f"""
                    SELECT
                        COUNT(*) as total_orders,
                        SUM(total_amount) as total_revenue,
                        AVG(total_amount) as avg_order_value,
                        COUNT(DISTINCT customer_id) as unique_customers,
                        COUNT(DISTINCT country) as countries,
                        MIN(order_date) as first_order,
                        MAX(order_date) as last_order
                    FROM {self.SALES_TABLE}
                """)

                row = cursor.fetchone()

                return {
                    "total_orders": row[0] or 0,
                    "total_revenue": float(row[1] or 0),
                    "avg_order_value": float(row[2] or 0),
                    "unique_customers": row[3] or 0,
                    "countries": row[4] or 0,
                    "first_order": str(row[5]) if row[5] else None,
                    "last_order": str(row[6]) if row[6] else None,
                }

        except Exception as e:
            logger.error(f"Error getting summary stats: {e}")
            return {}

    def check_order_exists(self, order_id: str) -> bool:
        """Check if an order already exists."""
        try:
            with postgres_connection() as (conn, cursor):
                cursor.execute(
                    f"SELECT 1 FROM {self.SALES_TABLE} WHERE order_id = %s LIMIT 1", (order_id,)
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking order existence: {e}")
            return False

    def delete_orders_by_date(self, before_date: datetime) -> int:
        """
        Delete orders before a specified date.

        Args:
            before_date: Delete orders before this date

        Returns:
            Number of rows deleted
        """
        try:
            with postgres_connection() as (conn, cursor):
                cursor.execute(
                    f"DELETE FROM {self.SALES_TABLE} WHERE order_date < %s", (before_date,)
                )
                deleted = cursor.rowcount
                logger.info(f"Deleted {deleted} orders before {before_date}")
                return deleted
        except Exception as e:
            logger.error(f"Error deleting orders: {e}")
            return 0

    def health_check(self) -> bool:
        """Check database connectivity and table existence."""
        try:
            with postgres_connection() as (conn, cursor):
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = %s
                    )
                """,
                    (self.SALES_TABLE,),
                )
                exists = cursor.fetchone()[0]

                if not exists:
                    logger.warning(f"Table {self.SALES_TABLE} does not exist")
                    return False

                logger.info("PostgreSQL health check passed")
                return True

        except Exception as e:
            logger.error(f"PostgreSQL health check failed: {e}")
            return False
