"""
Data Cleaner Module

Provides comprehensive data cleaning operations for sales data:
- Deduplication
- Type conversion
- Missing value handling
- Whitespace normalization
- Invalid data removal
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Tuple

import pandas as pd
from utils.logging_config import get_logger
from validation.schemas import VALID_COUNTRIES

logger = get_logger("data_cleaner")


@dataclass
class CleaningResult:
    """Result of a cleaning operation."""

    success: bool
    original_count: int
    cleaned_count: int
    rows_removed: int
    duplicates_removed: int
    invalid_dates_removed: int
    invalid_numerics_removed: int
    missing_filled: Dict[str, int] = field(default_factory=dict)
    duration_seconds: float = 0.0

    @property
    def removal_rate(self) -> float:
        """Calculate percentage of rows removed."""
        if self.original_count == 0:
            return 0.0
        return self.rows_removed / self.original_count

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "success": self.success,
            "original_count": self.original_count,
            "cleaned_count": self.cleaned_count,
            "rows_removed": self.rows_removed,
            "removal_rate": f"{self.removal_rate:.1%}",
            "duplicates_removed": self.duplicates_removed,
            "invalid_dates_removed": self.invalid_dates_removed,
            "invalid_numerics_removed": self.invalid_numerics_removed,
            "missing_filled": self.missing_filled,
            "duration_seconds": round(self.duration_seconds, 4),
        }


class DataCleaner:
    """
    Data cleaning class for sales data.

    Provides methods for comprehensive data cleaning with
    detailed tracking of changes made.
    """

    # Default fill values
    DEFAULT_CUSTOMER_ID = "UNKNOWN"
    DEFAULT_COUNTRY = "Unknown"
    DEFAULT_PRODUCT = "Unknown Product"

    # String columns to clean
    STRING_COLUMNS = ["product_name", "customer_id", "country"]

    def __init__(
        self,
        remove_duplicates: bool = True,
        fill_missing: bool = True,
        validate_dates: bool = True,
        validate_numerics: bool = True,
        normalize_strings: bool = True,
    ):
        """
        Initialize cleaner with configuration.

        Args:
            remove_duplicates: Remove duplicate order_ids
            fill_missing: Fill missing values with defaults
            validate_dates: Remove invalid dates
            validate_numerics: Remove invalid numeric values
            normalize_strings: Trim and normalize string values
        """
        self.remove_duplicates = remove_duplicates
        self.fill_missing = fill_missing
        self.validate_dates = validate_dates
        self.validate_numerics = validate_numerics
        self.normalize_strings = normalize_strings

    def clean(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, CleaningResult]:
        """
        Execute full cleaning pipeline on DataFrame.

        Args:
            df: Input DataFrame

        Returns:
            Tuple of (cleaned DataFrame, CleaningResult)
        """
        start_time = datetime.now()
        original_count = len(df)

        logger.info(f"Starting cleaning of {original_count} rows")

        # Track statistics
        stats = {
            "duplicates_removed": 0,
            "invalid_dates_removed": 0,
            "invalid_numerics_removed": 0,
            "missing_filled": {},
        }

        # Create a copy to avoid modifying original
        df_clean = df.copy()

        # Step 1: Remove duplicates
        if self.remove_duplicates:
            df_clean, stats["duplicates_removed"] = self._remove_duplicates(df_clean)

        # Step 2: Clean dates
        if self.validate_dates:
            df_clean, stats["invalid_dates_removed"] = self._clean_dates(df_clean)

        # Step 3: Clean numeric columns
        if self.validate_numerics:
            df_clean, stats["invalid_numerics_removed"] = self._clean_numerics(df_clean)

        # Step 4: Fill missing values
        if self.fill_missing:
            df_clean, stats["missing_filled"] = self._fill_missing(df_clean)

        # Step 5: Normalize strings
        if self.normalize_strings:
            df_clean = self._normalize_strings(df_clean)

        # Step 6: Normalize country names
        df_clean = self._normalize_countries(df_clean)

        duration = (datetime.now() - start_time).total_seconds()
        cleaned_count = len(df_clean)

        result = CleaningResult(
            success=True,
            original_count=original_count,
            cleaned_count=cleaned_count,
            rows_removed=original_count - cleaned_count,
            duplicates_removed=stats["duplicates_removed"],
            invalid_dates_removed=stats["invalid_dates_removed"],
            invalid_numerics_removed=stats["invalid_numerics_removed"],
            missing_filled=stats["missing_filled"],
            duration_seconds=duration,
        )

        logger.info(
            f"Cleaning completed: {original_count} -> {cleaned_count} rows "
            f"({result.removal_rate:.1%} removed) in {duration:.2f}s"
        )

        return df_clean, result

    def _remove_duplicates(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
        """Remove duplicate rows based on order_id."""
        original_count = len(df)
        df = df.drop_duplicates(subset=["order_id"], keep="first")
        removed = original_count - len(df)

        if removed > 0:
            logger.info(f"Removed {removed} duplicate order_ids")

        return df, removed

    def _clean_dates(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
        """Parse and validate order_date column."""
        original_count = len(df)

        # Convert to datetime
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")

        # Remove rows with invalid dates
        df = df.dropna(subset=["order_date"])

        # Remove future dates (data quality issue)
        today = pd.Timestamp.now()
        df = df[df["order_date"] <= today]

        # Remove very old dates (likely data issues)
        min_date = pd.Timestamp("2020-01-01")
        df = df[df["order_date"] >= min_date]

        removed = original_count - len(df)

        if removed > 0:
            logger.info(f"Removed {removed} rows with invalid dates")

        return df, removed

    def _clean_numerics(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
        """Clean and validate numeric columns (quantity, unit_price)."""
        original_count = len(df)

        # Convert quantity to numeric
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

        # Convert unit_price to numeric
        df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

        # Remove rows with NaN numeric values
        df = df.dropna(subset=["quantity", "unit_price"])

        # Remove rows with invalid values (non-positive)
        df = df[(df["quantity"] > 0) & (df["unit_price"] > 0)]

        # Remove unreasonable values
        df = df[(df["quantity"] <= 10000) & (df["unit_price"] <= 100000)]

        # Ensure quantity is integer
        df["quantity"] = df["quantity"].astype(int)

        removed = original_count - len(df)

        if removed > 0:
            logger.info(f"Removed {removed} rows with invalid numeric values")

        return df, removed

    def _fill_missing(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
        """Fill missing values in non-critical columns."""
        filled = {}

        # Fill customer_id
        mask = df["customer_id"].isna() | (df["customer_id"] == "")
        filled["customer_id"] = mask.sum()
        df.loc[mask, "customer_id"] = self.DEFAULT_CUSTOMER_ID

        # Fill country
        mask = df["country"].isna() | (df["country"] == "")
        filled["country"] = mask.sum()
        df.loc[mask, "country"] = self.DEFAULT_COUNTRY

        # Fill product_name
        mask = df["product_name"].isna() | (df["product_name"] == "")
        filled["product_name"] = mask.sum()
        df.loc[mask, "product_name"] = self.DEFAULT_PRODUCT

        total_filled = sum(filled.values())
        if total_filled > 0:
            logger.info(f"Filled {total_filled} missing values: {filled}")

        return df, filled

    def _normalize_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize string columns (trim, remove extra spaces)."""
        for col in self.STRING_COLUMNS:
            if col in df.columns:
                # Convert to string
                df[col] = df[col].astype(str)
                # Strip whitespace
                df[col] = df[col].str.strip()
                # Replace multiple spaces with single space
                df[col] = df[col].str.replace(r"\s+", " ", regex=True)

        return df

    def _normalize_countries(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize country names to valid list."""
        # Create mapping for common variations
        country_mapping = {
            "usa": "United States",
            "us": "United States",
            "uk": "United Kingdom",
            "gb": "United Kingdom",
            "great britain": "United Kingdom",
            "": "Unknown",
            "nan": "Unknown",
            "none": "Unknown",
        }

        # Apply mapping (case-insensitive)
        df["country"] = df["country"].apply(
            lambda x: country_mapping.get(
                str(x).lower().strip(), x if x in VALID_COUNTRIES else "Unknown"
            )
        )

        # Ensure all countries are in valid list
        df.loc[~df["country"].isin(VALID_COUNTRIES), "country"] = "Unknown"

        return df

    def clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize column names (lowercase, underscore)."""
        df.columns = (
            df.columns.str.strip()
            .str.lower()
            .str.replace(r"\s+", "_", regex=True)
            .str.replace(r"[^\w]", "", regex=True)
        )
        return df

    def remove_empty_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove rows that are entirely empty."""
        return df.dropna(how="all")
