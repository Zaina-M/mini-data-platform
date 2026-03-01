"""
Tests for Data Cleaner

Tests the DataCleaner class for data cleaning transformations.
"""

import os
import sys

import pandas as pd

# Add dags directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))

from transformers.data_cleaner import CleaningResult, DataCleaner


class TestCleaningResult:
    """Tests for CleaningResult dataclass."""

    def test_cleaning_result_creation(self):
        """Test creating a CleaningResult."""
        result = CleaningResult(
            success=True,
            original_count=100,
            cleaned_count=95,
            rows_removed=5,
            duplicates_removed=2,
            invalid_dates_removed=2,
            invalid_numerics_removed=1,
        )

        assert result.success is True
        assert result.original_count == 100
        assert result.cleaned_count == 95
        assert result.rows_removed == 5

    def test_removal_rate_calculation(self):
        """Test removal rate property calculation."""
        result = CleaningResult(
            success=True,
            original_count=100,
            cleaned_count=80,
            rows_removed=20,
            duplicates_removed=0,
            invalid_dates_removed=0,
            invalid_numerics_removed=0,
        )

        assert result.removal_rate == 0.2

    def test_removal_rate_zero_original(self):
        """Test removal rate when original count is zero."""
        result = CleaningResult(
            success=True,
            original_count=0,
            cleaned_count=0,
            rows_removed=0,
            duplicates_removed=0,
            invalid_dates_removed=0,
            invalid_numerics_removed=0,
        )

        assert result.removal_rate == 0.0

    def test_to_dict_includes_all_fields(self):
        """Test that to_dict includes all necessary fields."""
        result = CleaningResult(
            success=True,
            original_count=100,
            cleaned_count=95,
            rows_removed=5,
            duplicates_removed=2,
            invalid_dates_removed=2,
            invalid_numerics_removed=1,
            missing_filled={"customer_id": 3},
            duration_seconds=0.5,
        )

        d = result.to_dict()

        assert "success" in d
        assert "original_count" in d
        assert "cleaned_count" in d
        assert "rows_removed" in d
        assert "removal_rate" in d
        assert "missing_filled" in d


class TestDataCleaner:
    """Tests for DataCleaner class."""

    def test_cleaner_initialization_defaults(self):
        """Test default cleaner configuration."""
        cleaner = DataCleaner()

        assert cleaner.remove_duplicates is True
        assert cleaner.fill_missing is True
        assert cleaner.validate_dates is True
        assert cleaner.validate_numerics is True
        assert cleaner.normalize_strings is True

    def test_cleaner_custom_configuration(self):
        """Test custom cleaner configuration."""
        cleaner = DataCleaner(remove_duplicates=False, fill_missing=False)

        assert cleaner.remove_duplicates is False
        assert cleaner.fill_missing is False

    def test_clean_valid_data(self, sample_raw_data):
        """Test cleaning valid data returns same row count."""
        # Convert to proper types first (simulating CSV read)
        df = sample_raw_data.copy()

        cleaner = DataCleaner()
        cleaned_df, result = cleaner.clean(df)

        assert result.success is True
        assert len(cleaned_df) <= len(df)  # May be equal or less

    def test_clean_removes_duplicates(self):
        """Test that duplicate order_ids are removed."""
        df = pd.DataFrame(
            {
                "order_id": ["ORD-001001", "ORD-001001", "ORD-001002"],
                "product_name": ["Product A", "Product B", "Product C"],
                "quantity": ["2", "3", "1"],
                "unit_price": ["100.00", "100.00", "50.00"],
                "order_date": ["2024-01-15", "2024-01-16", "2024-01-17"],
                "customer_id": ["CUST-10001", "CUST-10001", "CUST-10002"],
                "country": ["United States", "United States", "Canada"],
            }
        )

        cleaner = DataCleaner()
        cleaned_df, result = cleaner.clean(df)

        assert result.duplicates_removed == 1
        assert len(cleaned_df) == 2
        assert cleaned_df["order_id"].nunique() == 2

    def test_clean_removes_invalid_dates(self):
        """Test that invalid dates are removed."""
        df = pd.DataFrame(
            {
                "order_id": ["ORD-001001", "ORD-001002", "ORD-001003"],
                "product_name": ["Product A", "Product B", "Product C"],
                "quantity": ["2", "3", "1"],
                "unit_price": ["100.00", "100.00", "50.00"],
                "order_date": ["2024-01-15", "invalid-date", "2024-01-17"],
                "customer_id": ["CUST-10001", "CUST-10002", "CUST-10003"],
                "country": ["United States", "Canada", "Germany"],
            }
        )

        cleaner = DataCleaner()
        cleaned_df, result = cleaner.clean(df)

        assert result.invalid_dates_removed >= 1
        assert len(cleaned_df) < 3

    def test_clean_removes_future_dates(self):
        """Test that future dates are removed."""
        df = pd.DataFrame(
            {
                "order_id": ["ORD-001001", "ORD-001002"],
                "product_name": ["Product A", "Product B"],
                "quantity": ["2", "3"],
                "unit_price": ["100.00", "100.00"],
                "order_date": ["2024-01-15", "2099-01-01"],  # Future date
                "customer_id": ["CUST-10001", "CUST-10002"],
                "country": ["United States", "Canada"],
            }
        )

        cleaner = DataCleaner()
        cleaned_df, result = cleaner.clean(df)

        assert len(cleaned_df) == 1

    def test_clean_removes_negative_quantity(self):
        """Test that negative quantities are removed."""
        df = pd.DataFrame(
            {
                "order_id": ["ORD-001001", "ORD-001002"],
                "product_name": ["Product A", "Product B"],
                "quantity": ["2", "-5"],  # Negative quantity
                "unit_price": ["100.00", "100.00"],
                "order_date": ["2024-01-15", "2024-01-16"],
                "customer_id": ["CUST-10001", "CUST-10002"],
                "country": ["United States", "Canada"],
            }
        )

        cleaner = DataCleaner()
        cleaned_df, result = cleaner.clean(df)

        assert len(cleaned_df) == 1
        assert result.invalid_numerics_removed >= 1

    def test_clean_removes_zero_price(self):
        """Test that zero prices are removed."""
        df = pd.DataFrame(
            {
                "order_id": ["ORD-001001", "ORD-001002"],
                "product_name": ["Product A", "Product B"],
                "quantity": ["2", "3"],
                "unit_price": ["100.00", "0"],  # Zero price
                "order_date": ["2024-01-15", "2024-01-16"],
                "customer_id": ["CUST-10001", "CUST-10002"],
                "country": ["United States", "Canada"],
            }
        )

        cleaner = DataCleaner()
        cleaned_df, result = cleaner.clean(df)

        assert len(cleaned_df) == 1

    def test_clean_fills_missing_customer_id(self):
        """Test that missing customer_id is filled with UNKNOWN."""
        df = pd.DataFrame(
            {
                "order_id": ["ORD-001001", "ORD-001002"],
                "product_name": ["Product A", "Product B"],
                "quantity": ["2", "3"],
                "unit_price": ["100.00", "50.00"],
                "order_date": ["2024-01-15", "2024-01-16"],
                "customer_id": ["CUST-10001", ""],  # Empty customer_id
                "country": ["United States", "Canada"],
            }
        )

        cleaner = DataCleaner()
        cleaned_df, result = cleaner.clean(df)

        assert "UNKNOWN" in cleaned_df["customer_id"].values
        assert result.missing_filled.get("customer_id", 0) >= 1

    def test_clean_fills_missing_country(self):
        """Test that missing country is filled with Unknown."""
        df = pd.DataFrame(
            {
                "order_id": ["ORD-001001", "ORD-001002"],
                "product_name": ["Product A", "Product B"],
                "quantity": ["2", "3"],
                "unit_price": ["100.00", "50.00"],
                "order_date": ["2024-01-15", "2024-01-16"],
                "customer_id": ["CUST-10001", "CUST-10002"],
                "country": ["United States", ""],  # Empty country
            }
        )

        cleaner = DataCleaner()
        cleaned_df, result = cleaner.clean(df)

        assert "Unknown" in cleaned_df["country"].values

    def test_clean_normalizes_whitespace(self):
        """Test that whitespace is trimmed from strings."""
        df = pd.DataFrame(
            {
                "order_id": ["ORD-001001"],
                "product_name": ["  Product A  "],  # Extra whitespace
                "quantity": ["2"],
                "unit_price": ["100.00"],
                "order_date": ["2024-01-15"],
                "customer_id": ["CUST-10001"],
                "country": ["  United States  "],  # Extra whitespace
            }
        )

        cleaner = DataCleaner()
        cleaned_df, result = cleaner.clean(df)

        assert cleaned_df["product_name"].iloc[0] == "Product A"
        assert cleaned_df["country"].iloc[0] == "United States"

    def test_clean_normalizes_invalid_countries(self):
        """Test that invalid countries are normalized to Unknown."""
        df = pd.DataFrame(
            {
                "order_id": ["ORD-001001"],
                "product_name": ["Product A"],
                "quantity": ["2"],
                "unit_price": ["100.00"],
                "order_date": ["2024-01-15"],
                "customer_id": ["CUST-10001"],
                "country": ["InvalidCountry"],  # Invalid country
            }
        )

        cleaner = DataCleaner()
        cleaned_df, result = cleaner.clean(df)

        assert cleaned_df["country"].iloc[0] == "Unknown"

    def test_clean_handles_country_abbreviations(self):
        """Test that country abbreviations are expanded."""
        df = pd.DataFrame(
            {
                "order_id": ["ORD-001001", "ORD-001002"],
                "product_name": ["Product A", "Product B"],
                "quantity": ["2", "3"],
                "unit_price": ["100.00", "50.00"],
                "order_date": ["2024-01-15", "2024-01-16"],
                "customer_id": ["CUST-10001", "CUST-10002"],
                "country": ["USA", "UK"],  # Abbreviations
            }
        )

        cleaner = DataCleaner()
        cleaned_df, result = cleaner.clean(df)

        assert "United States" in cleaned_df["country"].values
        assert "United Kingdom" in cleaned_df["country"].values

    def test_clean_does_not_modify_original(self, sample_raw_data):
        """Test that cleaning does not modify the original DataFrame."""
        original_len = len(sample_raw_data)
        original_copy = sample_raw_data.copy()

        cleaner = DataCleaner()
        cleaner.clean(sample_raw_data)

        assert len(sample_raw_data) == original_len
        pd.testing.assert_frame_equal(sample_raw_data, original_copy)

    def test_clean_quantity_becomes_integer(self):
        """Test that quantity is converted to integer."""
        df = pd.DataFrame(
            {
                "order_id": ["ORD-001001"],
                "product_name": ["Product A"],
                "quantity": ["5.0"],  # Float string
                "unit_price": ["100.00"],
                "order_date": ["2024-01-15"],
                "customer_id": ["CUST-10001"],
                "country": ["United States"],
            }
        )

        cleaner = DataCleaner()
        cleaned_df, result = cleaner.clean(df)

        assert cleaned_df["quantity"].dtype in [int, "int64", "int32"]

    def test_clean_with_data_quality_issues(self, sample_raw_data_with_errors):
        """Test cleaning data with multiple quality issues."""
        cleaner = DataCleaner()
        cleaned_df, result = cleaner.clean(sample_raw_data_with_errors)

        assert result.success is True
        # Should have removed some rows
        assert len(cleaned_df) < len(sample_raw_data_with_errors)
        # Should have tracked statistics
        assert (
            result.duplicates_removed > 0
            or result.invalid_dates_removed > 0
            or result.invalid_numerics_removed > 0
        )
