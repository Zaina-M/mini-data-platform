"""
Data Enricher Module

Provides data enrichment operations for sales data:
- Calculated field generation
- Metadata addition
- Data aggregation
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple

import pandas as pd
from utils.logging_config import get_logger

logger = get_logger("data_enricher")


@dataclass
class EnrichmentResult:
    """Result of an enrichment operation."""

    success: bool
    row_count: int
    fields_added: List[str]
    total_revenue: float
    avg_order_value: float
    duration_seconds: float = 0.0

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "row_count": self.row_count,
            "fields_added": self.fields_added,
            "total_revenue": round(self.total_revenue, 2),
            "avg_order_value": round(self.avg_order_value, 2),
            "duration_seconds": round(self.duration_seconds, 4),
        }


class DataEnricher:
    """
    Data enrichment class for sales data.

    Adds calculated fields and metadata to cleaned sales data.
    """

    def __init__(
        self,
        add_total_amount: bool = True,
        add_timestamp: bool = True,
        add_date_parts: bool = False,
        add_revenue_category: bool = False,
    ):
        """
        Initialize enricher with configuration.

        Args:
            add_total_amount: Calculate total_amount = quantity * unit_price
            add_timestamp: Add ingestion_timestamp
            add_date_parts: Add year, month, day columns
            add_revenue_category: Add revenue category classification
        """
        self.add_total_amount = add_total_amount
        self.add_timestamp = add_timestamp
        self.add_date_parts = add_date_parts
        self.add_revenue_category = add_revenue_category

    def enrich(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, EnrichmentResult]:
        """
        Execute full enrichment pipeline on DataFrame.

        Args:
            df: Input DataFrame (already cleaned)

        Returns:
            Tuple of (enriched DataFrame, EnrichmentResult)
        """
        start_time = datetime.now()
        fields_added = []

        logger.info(f"Starting enrichment of {len(df)} rows")

        # Create a copy to avoid modifying original
        df_enriched = df.copy()

        # Step 1: Calculate total amount
        if self.add_total_amount:
            df_enriched = self._add_total_amount(df_enriched)
            fields_added.append("total_amount")

        # Step 2: Add ingestion timestamp
        if self.add_timestamp:
            df_enriched = self._add_ingestion_timestamp(df_enriched)
            fields_added.append("ingestion_timestamp")

        # Step 3: Add date parts (optional)
        if self.add_date_parts:
            df_enriched = self._add_date_parts(df_enriched)
            fields_added.extend(["order_year", "order_month", "order_day", "order_weekday"])

        # Step 4: Add revenue category (optional)
        if self.add_revenue_category:
            df_enriched = self._add_revenue_category(df_enriched)
            fields_added.append("revenue_category")

        duration = (datetime.now() - start_time).total_seconds()

        # Calculate summary stats
        total_revenue = (
            float(df_enriched["total_amount"].sum()) if "total_amount" in df_enriched else 0
        )
        avg_order_value = (
            float(df_enriched["total_amount"].mean()) if "total_amount" in df_enriched else 0
        )

        result = EnrichmentResult(
            success=True,
            row_count=len(df_enriched),
            fields_added=fields_added,
            total_revenue=total_revenue,
            avg_order_value=avg_order_value,
            duration_seconds=duration,
        )

        logger.info(
            f"Enrichment completed: {len(df_enriched)} rows, "
            f"${total_revenue:,.2f} total revenue, "
            f"+{len(fields_added)} fields in {duration:.2f}s"
        )

        return df_enriched, result

    def _add_total_amount(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate total_amount = quantity * unit_price."""
        df["total_amount"] = df["quantity"] * df["unit_price"]
        df["total_amount"] = df["total_amount"].round(2)

        logger.debug(f"Added total_amount column (sum: ${df['total_amount'].sum():,.2f})")
        return df

    def _add_ingestion_timestamp(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add ingestion timestamp for tracking."""
        df["ingestion_timestamp"] = datetime.utcnow()

        logger.debug("Added ingestion_timestamp column")
        return df

    def _add_date_parts(self, df: pd.DataFrame) -> pd.DataFrame:
        """Extract date components from order_date."""
        if "order_date" not in df.columns:
            logger.warning("order_date column not found, skipping date parts")
            return df

        # Ensure datetime type
        df["order_date"] = pd.to_datetime(df["order_date"])

        df["order_year"] = df["order_date"].dt.year
        df["order_month"] = df["order_date"].dt.month
        df["order_day"] = df["order_date"].dt.day
        df["order_weekday"] = df["order_date"].dt.day_name()

        logger.debug("Added date part columns")
        return df

    def _add_revenue_category(self, df: pd.DataFrame) -> pd.DataFrame:
        """Categorize orders by revenue size."""
        if "total_amount" not in df.columns:
            logger.warning("total_amount column not found, skipping revenue category")
            return df

        def categorize(amount: float) -> str:
            if amount < 50:
                return "Small"
            elif amount < 200:
                return "Medium"
            elif amount < 1000:
                return "Large"
            else:
                return "Enterprise"

        df["revenue_category"] = df["total_amount"].apply(categorize)

        logger.debug("Added revenue_category column")
        return df

    def compute_aggregates(self, df: pd.DataFrame, by: str = "country") -> pd.DataFrame:
        """
        Compute aggregate statistics grouped by a column.

        Args:
            df: Input DataFrame
            by: Column to group by

        Returns:
            Aggregated DataFrame
        """
        if by not in df.columns:
            logger.error(f"Column '{by}' not found in DataFrame")
            return pd.DataFrame()

        agg_df = (
            df.groupby(by)
            .agg(
                {
                    "order_id": "count",
                    "quantity": "sum",
                    "total_amount": ["sum", "mean", "min", "max"],
                    "customer_id": "nunique",
                }
            )
            .round(2)
        )

        # Flatten column names
        agg_df.columns = [
            "total_orders",
            "total_units",
            "total_revenue",
            "avg_order_value",
            "min_order",
            "max_order",
            "unique_customers",
        ]

        agg_df = agg_df.reset_index()

        logger.info(f"Computed aggregates by {by}: {len(agg_df)} groups")
        return agg_df

    def add_running_totals(self, df: pd.DataFrame, date_column: str = "order_date") -> pd.DataFrame:
        """Add running total columns for time series analysis."""
        if date_column not in df.columns:
            logger.warning(f"Date column '{date_column}' not found")
            return df

        # Sort by date
        df = df.sort_values(date_column)

        # Compute running totals
        df["running_revenue"] = df["total_amount"].cumsum()
        df["running_orders"] = range(1, len(df) + 1)

        logger.debug("Added running total columns")
        return df

    def compute_daily_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute daily summary statistics."""
        if "order_date" not in df.columns:
            return pd.DataFrame()

        daily = (
            df.groupby(df["order_date"].dt.date)
            .agg(
                {
                    "order_id": "count",
                    "total_amount": "sum",
                    "quantity": "sum",
                    "customer_id": "nunique",
                }
            )
            .round(2)
        )

        daily.columns = ["orders", "revenue", "units", "customers"]
        daily.index.name = "date"
        daily = daily.reset_index()

        return daily
