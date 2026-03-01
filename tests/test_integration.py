"""
End-to-End Integration Tests

These tests verify the complete data flow through all components.
They use mocked external services to avoid requiring a real infrastructure.
"""

import os
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Add dags directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'airflow', 'dags'))

from transformers.data_cleaner import DataCleaner
from transformers.data_enricher import DataEnricher
from validation.validators import DataValidator


class TestEndToEndPipeline:
    """End-to-end tests for the data pipeline."""
    
    def test_complete_pipeline_flow(self, sample_raw_data):
        """Test complete data flow: validate -> clean -> enrich -> validate."""
        # Step 1: Validate raw data
        validator = DataValidator()
        raw_result = validator.validate_raw_data(sample_raw_data, "test.csv")
        assert raw_result.is_valid is True
        
        # Step 2: Clean the data
        cleaner = DataCleaner()
        cleaned_df, cleaning_result = cleaner.clean(sample_raw_data)
        assert cleaning_result.success is True
        
        # Step 3: Validate cleaned data
        clean_result, validated_df = validator.validate_cleaned_data(
            cleaned_df,
            original_count=len(sample_raw_data)
        )
        assert clean_result.is_valid is True
        
        # Step 4: Enrich the data
        enricher = DataEnricher()
        enriched_df, enrichment_result = enricher.enrich(validated_df)
        assert enrichment_result.success is True
        assert "total_amount" in enriched_df.columns
        
        # Step 5: Validate enriched data
        enriched_result, final_df = validator.validate_enriched_data(enriched_df)
        assert enriched_result.is_valid is True
        
        # Final verification
        assert len(final_df) > 0
        assert "ingestion_timestamp" in final_df.columns
    
    def test_pipeline_with_data_quality_issues(self, sample_raw_data_with_errors):
        """Test pipeline handles data quality issues gracefully."""
        validator = DataValidator()
        cleaner = DataCleaner()
        enricher = DataEnricher()
        
        # Raw validation should pass (schema allows nulls)
        raw_result = validator.validate_raw_data(
            sample_raw_data_with_errors,
            "errors.csv"
        )
        
        # Clean the data - this removes bad rows
        cleaned_df, cleaning_result = cleaner.clean(sample_raw_data_with_errors)
        
        # Some data should have been removed
        assert len(cleaned_df) < len(sample_raw_data_with_errors)
        
        # If we have any valid rows, continue processing
        if len(cleaned_df) > 0:
            # Validate cleaned data
            clean_result, validated_df = validator.validate_cleaned_data(
                cleaned_df,
                original_count=len(sample_raw_data_with_errors)
            )
            
            # Enrich the data
            enriched_df, enrichment_result = enricher.enrich(validated_df)
            
            # Final validation
            enriched_result, final_df = validator.validate_enriched_data(enriched_df)
            
            # Should have valid output
            assert len(final_df) > 0
    
    def test_pipeline_performance_large_dataset(self, large_sample_data):
        """Test pipeline performance with larger dataset."""
        validator = DataValidator()
        cleaner = DataCleaner()
        enricher = DataEnricher()
        
        # Time the operations
        import time
        
        start = time.time()
        
        # Clean and process
        cleaned_df, _ = cleaner.clean(large_sample_data)
        clean_result, validated_df = validator.validate_cleaned_data(cleaned_df)
        enriched_df, enrichment_result = enricher.enrich(validated_df)
        final_result, final_df = validator.validate_enriched_data(enriched_df)
        
        duration = time.time() - start
        
        # Should complete in reasonable time (< 10 seconds for 1000 rows)
        assert duration < 10.0
        assert len(final_df) > 0
    
    def test_validation_history_tracking(self, sample_raw_data, sample_cleaned_data):
        """Test that validation history is properly tracked."""
        validator = DataValidator()
        
        # Run multiple validations
        validator.validate_raw_data(sample_raw_data)
        validator.validate_cleaned_data(sample_cleaned_data)
        
        # Check history
        assert len(validator.validation_history) == 2
        
        # Get summary
        summary = validator.get_validation_summary()
        assert summary["total_validations"] == 2
        assert summary["passed"] == 2


class TestTransformerChaining:
    """Test transformer chaining and composition."""
    
    def test_cleaner_enricher_chain(self, sample_raw_data):
        """Test that cleaner output can be passed to enricher."""
        cleaner = DataCleaner()
        enricher = DataEnricher()
        
        cleaned_df, _ = cleaner.clean(sample_raw_data)
        enriched_df, result = enricher.enrich(cleaned_df)
        
        # Verify data flows correctly
        assert len(enriched_df) == len(cleaned_df)
        assert "total_amount" in enriched_df.columns
    
    def test_custom_cleaner_configuration(self, sample_raw_data):
        """Test cleaner with custom configuration."""
        # Cleaner without filling missing values
        cleaner = DataCleaner(fill_missing=False)
        cleaned_df, result = cleaner.clean(sample_raw_data)
        
        assert result.missing_filled == {}
    
    def test_custom_enricher_configuration(self, sample_cleaned_data):
        """Test enricher with all options enabled."""
        enricher = DataEnricher(
            add_total_amount=True,
            add_timestamp=True,
            add_date_parts=True,
            add_revenue_category=True
        )
        
        enriched_df, result = enricher.enrich(sample_cleaned_data)
        
        # All fields should be added
        assert "total_amount" in enriched_df.columns
        assert "ingestion_timestamp" in enriched_df.columns
        assert "order_year" in enriched_df.columns
        assert "revenue_category" in enriched_df.columns


class TestDataIntegrity:
    """Test data integrity throughout the pipeline."""
    
    def test_order_id_preserved(self, sample_raw_data):
        """Test that order IDs are preserved through pipeline."""
        cleaner = DataCleaner()
        enricher = DataEnricher()
        
        original_ids = set(sample_raw_data["order_id"])
        
        cleaned_df, _ = cleaner.clean(sample_raw_data)
        enriched_df, _ = enricher.enrich(cleaned_df)
        
        # All order IDs should still be present (no duplicates in test data)
        final_ids = set(enriched_df["order_id"])
        assert original_ids == final_ids
    
    def test_total_amount_calculation_integrity(self, sample_cleaned_data):
        """Test that total_amount is calculated correctly."""
        enricher = DataEnricher()
        enriched_df, _ = enricher.enrich(sample_cleaned_data)
        
        # Verify calculation for each row
        for idx, row in enriched_df.iterrows():
            expected = row["quantity"] * row["unit_price"]
            actual = row["total_amount"]
            assert abs(expected - actual) < 0.01
    
    def test_no_data_loss_valid_data(self, sample_cleaned_data):
        """Test that valid data is not lost during processing."""
        enricher = DataEnricher()
        enriched_df, _ = enricher.enrich(sample_cleaned_data)
        
        assert len(enriched_df) == len(sample_cleaned_data)
