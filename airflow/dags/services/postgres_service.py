"""
PostgreSQL Service Module

Provides high-level operations for PostgreSQL database:
- Data loading with upsert support
- Query execution
- Table operations
"""

from dataclasses import dataclass, field
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
    failed_order_ids: List[str] = field(default_factory=list)

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
            "failed_order_ids": self.failed_order_ids,
        }


def _elapsed(start: datetime) -> float:
    return (datetime.now() - start).total_seconds()


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
            product_name         = EXCLUDED.product_name,
            quantity             = EXCLUDED.quantity,
            unit_price           = EXCLUDED.unit_price,
            order_date           = EXCLUDED.order_date,
            customer_id          = EXCLUDED.customer_id,
            country              = EXCLUDED.country,
            total_amount         = EXCLUDED.total_amount,
            ingestion_timestamp  = EXCLUDED.ingestion_timestamp
    """

    # ── helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _row_to_tuple(row: Dict[str, Any]) -> tuple:
        """Convert a row dict to the positional tuple expected by UPSERT_QUERY."""
        return (
            row["order_id"],
            row["product_name"],
            int(row["quantity"]),
            float(row["unit_price"]),
            row["order_date"],
            row["customer_id"],
            row["country"],
            float(row["total_amount"]),
            row["ingestion_timestamp"],
        )

    # ── public API ────────────────────────────────────────────────────────────

    def load_sales_data(self, df: pd.DataFrame, batch_size: int = 1000) -> LoadResult:
        """
        Load sales data into PostgreSQL with upsert, row by row.

        Rows that raise an exception are skipped and recorded in
        LoadResult.failed_order_ids; all other rows are committed.

        Args:
            df:         DataFrame with sales data.
            batch_size: How often to log progress (rows).

        Returns:
            LoadResult with operation details.
        """
        start = datetime.now()
        rows_inserted = 0
        failed_order_ids: List[str] = []

        logger.info(f"Starting load of {len(df)} rows into '{self.SALES_TABLE}'")

        try:
            with postgres_connection() as (conn, cursor):
                for idx, row in df.iterrows():
                    order_id = str(row.get("order_id", "unknown"))
                    try:
                        cursor.execute(self.UPSERT_QUERY, self._row_to_tuple(row))
                        rows_inserted += 1
                    except Exception as e:
                        logger.error(f"Row {idx} (order_id={order_id}) skipped: {e}")
                        failed_order_ids.append(order_id)

                    total_done = rows_inserted + len(failed_order_ids)
                    if total_done % batch_size == 0:
                        logger.info(
                            f"Progress: {total_done}/{len(df)} rows "
                            f"({len(failed_order_ids)} failed)"
                        )

            duration = _elapsed(start)

            if failed_order_ids:
                logger.warning(
                    f"{len(failed_order_ids)} row(s) permanently failed: {failed_order_ids}"
                )

            rate = rows_inserted / duration if duration > 0 else float("inf")
            logger.info(
                f"Load complete: {rows_inserted} inserted, {len(failed_order_ids)} failed "
                f"in {duration:.2f}s ({rate:.0f} rows/sec)"
            )

            return LoadResult(
                success=True,
                rows_inserted=rows_inserted,
                rows_updated=0,
                rows_failed=len(failed_order_ids),
                duration_seconds=duration,
                failed_order_ids=failed_order_ids,
            )

        except Exception as e:
            duration = _elapsed(start)
            logger.error(f"Load aborted: {e}")

            return LoadResult(
                success=False,
                rows_inserted=rows_inserted,
                rows_updated=0,
                rows_failed=len(df) - rows_inserted,
                duration_seconds=duration,
                error_message=str(e),
                failed_order_ids=failed_order_ids,
            )

    def load_batch(self, records: List[Dict[str, Any]]) -> LoadResult:
        """
        Load a batch of records efficiently using executemany.

        All records are sent in a single round-trip. If any record
        causes a failure the entire batch is rolled back by the
        context manager.

        Args:
            records: List of record dictionaries.

        Returns:
            LoadResult with operation details.
        """
        if not records:
            return LoadResult(
                success=True,
                rows_inserted=0,
                rows_updated=0,
                rows_failed=0,
                duration_seconds=0.0,
            )

        start = datetime.now()
        logger.info(f"Loading batch of {len(records)} records")

        try:
            values = [self._row_to_tuple(r) for r in records]

            with postgres_connection() as (conn, cursor):
                cursor.executemany(self.UPSERT_QUERY, values)

            duration = _elapsed(start)
            logger.info(f"Batch loaded: {len(records)} rows in {duration:.2f}s")

            return LoadResult(
                success=True,
                rows_inserted=len(records),
                rows_updated=0,
                rows_failed=0,
                duration_seconds=duration,
            )

        except Exception as e:
            duration = _elapsed(start)
            logger.error(f"Batch load failed: {e}")

            return LoadResult(
                success=False,
                rows_inserted=0,
                rows_updated=0,
                rows_failed=len(records),
                duration_seconds=duration,
                error_message=str(e),
            )

    # ── read helpers ──────────────────────────────────────────────────────────

    def get_row_count(self) -> int:
        """Return current row count in the sales table, or -1 on error."""
        try:
            with postgres_connection() as (conn, cursor):
                cursor.execute(f"SELECT COUNT(*) FROM {self.SALES_TABLE}")
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error getting row count: {e}")
            return -1

    def get_date_range(self) -> Tuple[Optional[datetime], Optional[datetime]]:
        """Return (min, max) order dates from the sales table."""
        try:
            with postgres_connection() as (conn, cursor):
                cursor.execute(
                    f"SELECT MIN(order_date), MAX(order_date) FROM {self.SALES_TABLE}"
                )
                return cursor.fetchone()
        except Exception as e:
            logger.error(f"Error getting date range: {e}")
            return None, None

    def get_summary_stats(self) -> Dict[str, Any]:
        """Return aggregated summary statistics from the sales table."""
        try:
            with postgres_connection() as (conn, cursor):
                cursor.execute(f"""
                    SELECT
                        COUNT(*)                    AS total_orders,
                        SUM(total_amount)           AS total_revenue,
                        AVG(total_amount)           AS avg_order_value,
                        COUNT(DISTINCT customer_id) AS unique_customers,
                        COUNT(DISTINCT country)     AS countries,
                        MIN(order_date)             AS first_order,
                        MAX(order_date)             AS last_order
                    FROM {self.SALES_TABLE}
                """)
                row = cursor.fetchone()

            return {
                "total_orders":     row[0] or 0,
                "total_revenue":    float(row[1] or 0),
                "avg_order_value":  float(row[2] or 0),
                "unique_customers": row[3] or 0,
                "countries":        row[4] or 0,
                "first_order":      str(row[5]) if row[5] else None,
                "last_order":       str(row[6]) if row[6] else None,
            }

        except Exception as e:
            logger.error(f"Error getting summary stats: {e}")
            return {}

    def check_order_exists(self, order_id: str) -> bool:
        """Return True if the given order_id exists in the sales table."""
        try:
            with postgres_connection() as (conn, cursor):
                cursor.execute(
                    f"SELECT 1 FROM {self.SALES_TABLE} WHERE order_id = %s LIMIT 1",
                    (order_id,),
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"Error checking order existence: {e}")
            return False

    # ── write helpers ─────────────────────────────────────────────────────────

    def delete_orders_by_date(self, before_date: datetime) -> int:
        """
        Delete all orders with an order_date earlier than before_date.

        Returns:
            Number of rows deleted, or 0 on error.
        """
        try:
            with postgres_connection() as (conn, cursor):
                cursor.execute(
                    f"DELETE FROM {self.SALES_TABLE} WHERE order_date < %s",
                    (before_date,),
                )
                deleted = cursor.rowcount
                logger.info(f"Deleted {deleted} orders before {before_date}")
                return deleted
        except Exception as e:
            logger.error(f"Error deleting orders: {e}")
            return 0

    # ── ops ───────────────────────────────────────────────────────────────────

    def health_check(self) -> bool:
        """Verify database connectivity and that the sales table exists."""
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
                logger.warning(f"Table '{self.SALES_TABLE}' does not exist")
                return False

            logger.info("PostgreSQL health check passed")
            return True

        except Exception as e:
            logger.error(f"PostgreSQL health check failed: {e}")
            return False