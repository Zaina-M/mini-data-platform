"""
Tests for Pandera Validation Schemas

Tests the RawSalesSchema, CleanedSalesSchema, and EnrichedSalesSchema
validation logic using pytest.
"""

import os
import sys

import pandas as pd
import pandera as pa
import pytest

# Add dags directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'airflow', 'dags'))

from validation.schemas import (
    RawSalesSchema,
    CleanedSalesSchema,
    EnrichedSalesSchema,
    validate_dataframe,
    VALID_COUNTRIES,
)


class TestRawSalesSchema:
    """Tests for RawSalesSchema validation."""
    
    def test_valid_raw_data_passes(self, sample_raw_data):
        """Test that valid raw data passes schema validation."""
        # This should not raise
        validated = RawSalesSchema.validate(sample_raw_data)
        assert len(validated) == len(sample_raw_data)
    
    def test_raw_data_accepts_string_types(self, sample_raw_data):
        """Test that raw schema accepts string types (pre-conversion)."""
        # Ensure all columns are strings (as they would be from CSV)
        df = sample_raw_data.astype(str)
        validated = RawSalesSchema.validate(df)
        assert len(validated) == len(sample_raw_data)
    
    def test_raw_data_requires_order_id(self, sample_raw_data):
        """Test that order_id is required and cannot be null."""
        df = sample_raw_data.copy()
        df.loc[0, "order_id"] = None
        
        with pytest.raises(pa.errors.SchemaError):
            RawSalesSchema.validate(df, lazy=False)
    
    def test_raw_data_allows_nullable_columns(self, sample_raw_data):
        """Test that some columns are allowed to be null in raw data."""
        df = sample_raw_data.copy()
        df.loc[0, "customer_id"] = None
        df.loc[1, "country"] = None
        
        # Should not raise - these are nullable in raw schema
        validated = RawSalesSchema.validate(df)
        assert len(validated) == len(sample_raw_data)
    
    def test_raw_data_accepts_extra_columns(self, sample_raw_data):
        """Test that raw schema allows extra columns (strict=False)."""
        df = sample_raw_data.copy()
        df["extra_column"] = "extra_value"
        
        # Should not raise due to strict=False
        validated = RawSalesSchema.validate(df)
        assert "extra_column" in validated.columns


class TestCleanedSalesSchema:
    """Tests for CleanedSalesSchema validation."""
    
    def test_valid_cleaned_data_passes(self, sample_cleaned_data):
        """Test that valid cleaned data passes schema validation."""
        validated = CleanedSalesSchema.validate(sample_cleaned_data)
        assert len(validated) == len(sample_cleaned_data)
    
    def test_cleaned_data_requires_correct_types(self, sample_cleaned_data):
        """Test that cleaned schema enforces correct data types."""
        df = sample_cleaned_data.copy()
        
        # Verify types after validation
        validated = CleanedSalesSchema.validate(df)
        assert validated["quantity"].dtype in [int, "int64", "int32"]
        assert validated["unit_price"].dtype in [float, "float64"]
    
    def test_cleaned_data_enforces_positive_quantity(self, sample_cleaned_data):
        """Test that quantity must be positive."""
        df = sample_cleaned_data.copy()
        df.loc[0, "quantity"] = 0
        
        with pytest.raises(pa.errors.SchemaErrors):
            CleanedSalesSchema.validate(df, lazy=True)
    
    def test_cleaned_data_enforces_positive_price(self, sample_cleaned_data):
        """Test that unit_price must be positive."""
        df = sample_cleaned_data.copy()
        df.loc[0, "unit_price"] = -10.0
        
        with pytest.raises(pa.errors.SchemaErrors):
            CleanedSalesSchema.validate(df, lazy=True)
    
    def test_cleaned_data_validates_order_id_format(self, sample_cleaned_data):
        """Test that order_id must match expected format."""
        df = sample_cleaned_data.copy()
        df.loc[0, "order_id"] = "INVALID-ID"
        
        with pytest.raises(pa.errors.SchemaErrors):
            CleanedSalesSchema.validate(df, lazy=True)
    
    def test_cleaned_data_validates_customer_id_format(self, sample_cleaned_data):
        """Test that customer_id must match expected format."""
        df = sample_cleaned_data.copy()
        df.loc[0, "customer_id"] = "INVALID"
        
        with pytest.raises(pa.errors.SchemaErrors):
            CleanedSalesSchema.validate(df, lazy=True)
    
    def test_cleaned_data_allows_unknown_customer(self, sample_cleaned_data):
        """Test that UNKNOWN is valid for customer_id."""
        df = sample_cleaned_data.copy()
        df.loc[0, "customer_id"] = "UNKNOWN"
        
        # Should not raise
        validated = CleanedSalesSchema.validate(df)
        assert validated.loc[0, "customer_id"] == "UNKNOWN"
    
    def test_cleaned_data_validates_country(self, sample_cleaned_data):
        """Test that country must be from valid list."""
        df = sample_cleaned_data.copy()
        df.loc[0, "country"] = "InvalidCountry"
        
        with pytest.raises(pa.errors.SchemaErrors):
            CleanedSalesSchema.validate(df, lazy=True)
    
    def test_cleaned_data_enforces_unique_order_id(self, sample_cleaned_data):
        """Test that order_id must be unique."""
        df = sample_cleaned_data.copy()
        df.loc[1, "order_id"] = df.loc[0, "order_id"]  # Create duplicate
        
        with pytest.raises(pa.errors.SchemaErrors):
            CleanedSalesSchema.validate(df, lazy=True)


class TestEnrichedSalesSchema:
    """Tests for EnrichedSalesSchema validation."""
    
    def test_valid_enriched_data_passes(self, sample_enriched_data):
        """Test that valid enriched data passes schema validation."""
        validated = EnrichedSalesSchema.validate(sample_enriched_data)
        assert len(validated) == len(sample_enriched_data)
    
    def test_enriched_data_requires_total_amount(self, sample_cleaned_data):
        """Test that total_amount is required."""
        df = sample_cleaned_data.copy()
        df["ingestion_timestamp"] = pd.Timestamp.now()
        # Missing total_amount
        
        with pytest.raises((pa.errors.SchemaError, pa.errors.SchemaErrors)):
            EnrichedSalesSchema.validate(df, lazy=True)
    
    def test_enriched_data_requires_ingestion_timestamp(self, sample_cleaned_data):
        """Test that ingestion_timestamp is required."""
        df = sample_cleaned_data.copy()
        df["total_amount"] = df["quantity"] * df["unit_price"]
        # Missing ingestion_timestamp
        
        with pytest.raises((pa.errors.SchemaError, pa.errors.SchemaErrors)):
            EnrichedSalesSchema.validate(df, lazy=True)
    
    def test_enriched_data_validates_total_amount_positive(self, sample_enriched_data):
        """Test that total_amount must be positive."""
        df = sample_enriched_data.copy()
        df.loc[0, "total_amount"] = -100.0
        
        with pytest.raises(pa.errors.SchemaErrors):
            EnrichedSalesSchema.validate(df, lazy=True)


class TestValidateDataframeHelper:
    """Tests for the validate_dataframe helper function."""
    
    def test_returns_true_for_valid_data(self, sample_raw_data):
        """Test that valid data returns (True, df, None)."""
        is_valid, validated_df, error = validate_dataframe(
            sample_raw_data, RawSalesSchema
        )
        
        assert is_valid is True
        assert validated_df is not None
        assert error is None
    
    def test_returns_false_for_invalid_data(self, sample_raw_data):
        """Test that invalid data returns (False, None, error_message)."""
        df = sample_raw_data.copy()
        df.loc[0, "order_id"] = None
        
        is_valid, validated_df, error = validate_dataframe(
            df, RawSalesSchema
        )
        
        assert is_valid is False
        assert error is not None


class TestValidCountries:
    """Tests for the valid countries list."""
    
    def test_valid_countries_not_empty(self):
        """Test that valid countries list is not empty."""
        assert len(VALID_COUNTRIES) > 0
    
    def test_valid_countries_includes_unknown(self):
        """Test that Unknown is in valid countries for fallback."""
        assert "Unknown" in VALID_COUNTRIES
    
    def test_valid_countries_includes_major_markets(self):
        """Test that major markets are included."""
        major_markets = ["United States", "Canada", "United Kingdom", "Germany"]
        for market in major_markets:
            assert market in VALID_COUNTRIES
