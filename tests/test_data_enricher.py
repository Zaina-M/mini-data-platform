"""
Tests for Data Enricher

Tests the DataEnricher class for data enrichment transformations.
"""

import os
import sys
from datetime import datetime

import pandas as pd
import pytest

# Add dags directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'airflow', 'dags'))

from transformers.data_enricher import DataEnricher, EnrichmentResult


class TestEnrichmentResult:
    """Tests for EnrichmentResult dataclass."""
    
    def test_enrichment_result_creation(self):
        """Test creating an EnrichmentResult."""
        result = EnrichmentResult(
            success=True,
            row_count=100,
            fields_added=["total_amount", "ingestion_timestamp"],
            total_revenue=50000.0,
            avg_order_value=500.0,
            duration_seconds=0.5
        )
        
        assert result.success is True
        assert result.row_count == 100
        assert len(result.fields_added) == 2
        assert result.total_revenue == 50000.0
    
    def test_to_dict_includes_all_fields(self):
        """Test that to_dict includes all necessary fields."""
        result = EnrichmentResult(
            success=True,
            row_count=100,
            fields_added=["total_amount"],
            total_revenue=10000.0,
            avg_order_value=100.0,
        )
        
        d = result.to_dict()
        
        assert "success" in d
        assert "row_count" in d
        assert "fields_added" in d
        assert "total_revenue" in d
        assert "avg_order_value" in d


class TestDataEnricher:
    """Tests for DataEnricher class."""
    
    def test_enricher_initialization_defaults(self):
        """Test default enricher configuration."""
        enricher = DataEnricher()
        
        assert enricher.add_total_amount is True
        assert enricher.add_timestamp is True
        assert enricher.add_date_parts is False
        assert enricher.add_revenue_category is False
    
    def test_enricher_custom_configuration(self):
        """Test custom enricher configuration."""
        enricher = DataEnricher(
            add_date_parts=True,
            add_revenue_category=True
        )
        
        assert enricher.add_date_parts is True
        assert enricher.add_revenue_category is True
    
    def test_enrich_adds_total_amount(self, sample_cleaned_data):
        """Test that total_amount is calculated correctly."""
        enricher = DataEnricher()
        enriched_df, result = enricher.enrich(sample_cleaned_data)
        
        assert "total_amount" in enriched_df.columns
        
        # Verify calculation
        expected = sample_cleaned_data["quantity"] * sample_cleaned_data["unit_price"]
        pd.testing.assert_series_equal(
            enriched_df["total_amount"].round(2),
            expected.round(2),
            check_names=False
        )
    
    def test_enrich_adds_ingestion_timestamp(self, sample_cleaned_data):
        """Test that ingestion_timestamp is added."""
        enricher = DataEnricher()
        enriched_df, result = enricher.enrich(sample_cleaned_data)
        
        assert "ingestion_timestamp" in enriched_df.columns
        assert enriched_df["ingestion_timestamp"].notna().all()
    
    def test_enrich_adds_date_parts_when_enabled(self, sample_cleaned_data):
        """Test that date parts are added when enabled."""
        enricher = DataEnricher(add_date_parts=True)
        enriched_df, result = enricher.enrich(sample_cleaned_data)
        
        assert "order_year" in enriched_df.columns
        assert "order_month" in enriched_df.columns
        assert "order_day" in enriched_df.columns
        assert "order_weekday" in enriched_df.columns
    
    def test_enrich_adds_revenue_category_when_enabled(self, sample_cleaned_data):
        """Test that revenue category is added when enabled."""
        enricher = DataEnricher(add_revenue_category=True)
        enriched_df, result = enricher.enrich(sample_cleaned_data)
        
        assert "revenue_category" in enriched_df.columns
        # Should have valid categories
        valid_categories = ["Small", "Medium", "Large", "Enterprise"]
        assert all(cat in valid_categories for cat in enriched_df["revenue_category"])
    
    def test_revenue_category_classification(self):
        """Test revenue category classification logic."""
        df = pd.DataFrame({
            "order_id": ["ORD-001001", "ORD-001002", "ORD-001003", "ORD-001004"],
            "product_name": ["A", "B", "C", "D"],
            "quantity": [1, 1, 1, 1],
            "unit_price": [25.0, 100.0, 500.0, 2000.0],
            "order_date": pd.to_datetime(["2024-01-15"] * 4),
            "customer_id": ["CUST-10001"] * 4,
            "country": ["United States"] * 4,
        })
        
        enricher = DataEnricher(add_revenue_category=True)
        enriched_df, _ = enricher.enrich(df)
        
        # Verify categorization
        expected = ["Small", "Medium", "Large", "Enterprise"]
        assert enriched_df["revenue_category"].tolist() == expected
    
    def test_enrich_calculates_revenue_stats(self, sample_cleaned_data):
        """Test that revenue statistics are calculated."""
        enricher = DataEnricher()
        enriched_df, result = enricher.enrich(sample_cleaned_data)
        
        assert result.total_revenue > 0
        assert result.avg_order_value > 0
        assert result.total_revenue == enriched_df["total_amount"].sum()
    
    def test_enrich_tracks_fields_added(self, sample_cleaned_data):
        """Test that added fields are tracked."""
        enricher = DataEnricher()
        enriched_df, result = enricher.enrich(sample_cleaned_data)
        
        assert "total_amount" in result.fields_added
        assert "ingestion_timestamp" in result.fields_added
    
    def test_enrich_does_not_modify_original(self, sample_cleaned_data):
        """Test that enrichment does not modify original DataFrame."""
        original_columns = list(sample_cleaned_data.columns)
        
        enricher = DataEnricher()
        enricher.enrich(sample_cleaned_data)
        
        assert list(sample_cleaned_data.columns) == original_columns
    
    def test_compute_aggregates_by_country(self, sample_enriched_data):
        """Test computing aggregates by country."""
        enricher = DataEnricher()
        agg_df = enricher.compute_aggregates(sample_enriched_data, by="country")
        
        assert len(agg_df) > 0
        assert "country" in agg_df.columns
        assert "total_orders" in agg_df.columns
        assert "total_revenue" in agg_df.columns
    
    def test_compute_aggregates_invalid_column(self, sample_enriched_data):
        """Test aggregation with invalid column name."""
        enricher = DataEnricher()
        agg_df = enricher.compute_aggregates(sample_enriched_data, by="invalid_col")
        
        assert len(agg_df) == 0
    
    def test_compute_daily_summary(self, sample_enriched_data):
        """Test computing daily summary."""
        enricher = DataEnricher()
        daily_df = enricher.compute_daily_summary(sample_enriched_data)
        
        assert len(daily_df) > 0
        assert "date" in daily_df.columns
        assert "orders" in daily_df.columns
        assert "revenue" in daily_df.columns
    
    def test_add_running_totals(self, sample_enriched_data):
        """Test adding running totals."""
        enricher = DataEnricher()
        df_with_totals = enricher.add_running_totals(sample_enriched_data)
        
        assert "running_revenue" in df_with_totals.columns
        assert "running_orders" in df_with_totals.columns
        
        # Running totals should be monotonically increasing
        assert df_with_totals["running_orders"].is_monotonic_increasing
    
    def test_total_amount_rounding(self, sample_cleaned_data):
        """Test that total_amount is rounded to 2 decimal places."""
        df = sample_cleaned_data.copy()
        df["quantity"] = [3, 3, 3, 3, 3]
        df["unit_price"] = [33.333, 33.333, 33.333, 33.333, 33.333]
        
        enricher = DataEnricher()
        enriched_df, _ = enricher.enrich(df)
        
        # All values should have at most 2 decimal places
        for val in enriched_df["total_amount"]:
            assert round(val, 2) == val
