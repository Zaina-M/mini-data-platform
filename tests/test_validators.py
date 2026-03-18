"""
Tests for Data Validators

Tests the DataValidator class and ValidationResult functionality.
"""

import os
import sys
from datetime import datetime

import pandas as pd
from validation.validators import DataValidator, ValidationResult, ValidationStatus

# Add dags directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_validation_result_creation(self):
        """Test creating a ValidationResult."""
        result = ValidationResult(
            status=ValidationStatus.PASSED,
            is_valid=True,
            total_rows=100,
            valid_rows=95,
            invalid_rows=5,
        )

        assert result.status == ValidationStatus.PASSED
        assert result.is_valid is True
        assert result.total_rows == 100

    def test_validation_result_to_dict(self):
        """Test converting ValidationResult to dictionary."""
        result = ValidationResult(
            status=ValidationStatus.PASSED,
            is_valid=True,
            total_rows=100,
            valid_rows=100,
            invalid_rows=0,
            schema_name="TestSchema",
            duration_seconds=0.5,
        )

        result_dict = result.to_dict()

        assert result_dict["status"] == "passed"
        assert result_dict["is_valid"] is True
        assert result_dict["total_rows"] == 100
        assert result_dict["schema_name"] == "TestSchema"

    def test_validation_result_string_representation(self):
        """Test string representation of ValidationResult."""
        result = ValidationResult(
            status=ValidationStatus.PASSED,
            is_valid=True,
            total_rows=100,
            valid_rows=95,
            invalid_rows=5,
        )

        str_repr = str(result)
        assert "ValidationResult" in str_repr
        assert "passed" in str_repr


class TestDataValidator:
    """Tests for DataValidator class."""

    def test_validator_initialization(self):
        """Test creating a DataValidator."""
        validator = DataValidator()
        assert validator.strict is False
        assert len(validator.validation_history) == 0

    def test_validator_strict_mode(self):
        """Test creating a strict validator."""
        validator = DataValidator(strict=True)
        assert validator.strict is True

    def test_validate_raw_data_success(self, sample_raw_data):
        """Test validating valid raw data."""
        validator = DataValidator()
        result = validator.validate_raw_data(sample_raw_data, "test.csv")

        assert result.is_valid is True
        assert result.status == ValidationStatus.PASSED
        assert result.total_rows == len(sample_raw_data)

    def test_validate_raw_data_missing_columns(self):
        """Test validation fails for missing required columns."""
        validator = DataValidator()
        df = pd.DataFrame(
            {
                "order_id": ["ORD-001001"],
                # Missing other required columns
            }
        )

        result = validator.validate_raw_data(df, "incomplete.csv")

        assert result.is_valid is False
        assert result.status == ValidationStatus.FAILED
        assert len(result.errors) > 0

    def test_validate_raw_data_tracks_file_name(self, sample_raw_data):
        """Test that file name is tracked in metadata."""
        validator = DataValidator()
        result = validator.validate_raw_data(sample_raw_data, "test_file.csv")

        assert result.metadata.get("file_name") == "test_file.csv"

    def test_validate_cleaned_data_success(self, sample_cleaned_data):
        """Test validating valid cleaned data."""
        validator = DataValidator()
        result, validated_df = validator.validate_cleaned_data(
            sample_cleaned_data, original_count=len(sample_cleaned_data)
        )

        assert result.is_valid is True
        assert validated_df is not None
        assert len(validated_df) == len(sample_cleaned_data)

    def test_validate_cleaned_data_high_loss_warning(self, sample_cleaned_data):
        """Test warning when data loss exceeds threshold."""
        validator = DataValidator()
        result, _ = validator.validate_cleaned_data(
            sample_cleaned_data, original_count=len(sample_cleaned_data) * 3  # Simulate 66% loss
        )

        # Should have warning about high data loss
        assert len(result.warnings) > 0
        assert any("data loss" in w.lower() for w in result.warnings)

    def test_validate_enriched_data_success(self, sample_enriched_data):
        """Test validating valid enriched data."""
        validator = DataValidator()
        result, validated_df = validator.validate_enriched_data(sample_enriched_data)

        assert result.is_valid is True
        assert validated_df is not None

    def test_validate_enriched_data_missing_total_amount(self, sample_cleaned_data):
        """Test validation fails when total_amount is missing."""
        validator = DataValidator()
        df = sample_cleaned_data.copy()
        df["ingestion_timestamp"] = datetime.utcnow()
        # Missing total_amount

        result, _ = validator.validate_enriched_data(df)

        assert result.is_valid is False
        assert any("missing_enrichment" in str(e.get("type", "")) for e in result.errors)

    def test_validation_history_tracking(self, sample_raw_data, sample_cleaned_data):
        """Test that validation history is tracked."""
        validator = DataValidator()

        validator.validate_raw_data(sample_raw_data)
        validator.validate_cleaned_data(sample_cleaned_data)

        assert len(validator.validation_history) == 2

    def test_get_validation_summary(self, sample_raw_data):
        """Test getting validation summary."""
        validator = DataValidator()
        validator.validate_raw_data(sample_raw_data)

        summary = validator.get_validation_summary()

        assert summary["total_validations"] == 1
        assert summary["passed"] == 1
        assert summary["failed"] == 0
        assert summary["success_rate"] == 1.0

    def test_empty_validation_summary(self):
        """Test summary when no validations have run."""
        validator = DataValidator()
        summary = validator.get_validation_summary()

        assert summary["total_validations"] == 0


class TestValidationStatus:
    """Tests for ValidationStatus enum."""

    def test_status_values(self):
        """Test that all expected status values exist."""
        assert ValidationStatus.PASSED.value == "passed"
        assert ValidationStatus.FAILED.value == "failed"
        assert ValidationStatus.WARNING.value == "warning"
        assert ValidationStatus.SKIPPED.value == "skipped"
