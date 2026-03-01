"""
Data Validators Module

Provides high-level validation utilities that combine Pandera schemas
with business logic validation and detailed reporting.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum

import pandas as pd
import pandera as pa

from validation.schemas import (
    RawSalesSchema,
    CleanedSalesSchema,
    EnrichedSalesSchema,
    validate_dataframe,
    VALID_COUNTRIES,
)
from utils.logging_config import get_logger

logger = get_logger("validators")


class ValidationStatus(Enum):
    """Enumeration of validation statuses."""
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"


@dataclass
class ValidationResult:
    """
    Container for validation results with detailed reporting.
    
    Attributes:
        status: Overall validation status
        is_valid: Boolean indicating if validation passed
        total_rows: Total number of rows validated
        valid_rows: Number of rows that passed validation
        invalid_rows: Number of rows that failed validation
        errors: List of error details
        warnings: List of warning messages
        schema_name: Name of the schema used
        duration_seconds: Time taken for validation
        metadata: Additional validation metadata
    """
    status: ValidationStatus
    is_valid: bool
    total_rows: int
    valid_rows: int
    invalid_rows: int
    errors: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    schema_name: str = ""
    duration_seconds: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary for serialization."""
        return {
            "status": self.status.value,
            "is_valid": self.is_valid,
            "total_rows": self.total_rows,
            "valid_rows": self.valid_rows,
            "invalid_rows": self.invalid_rows,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "errors": self.errors[:10],  # Limit for serialization
            "warnings": self.warnings[:10],
            "schema_name": self.schema_name,
            "duration_seconds": round(self.duration_seconds, 4),
            "metadata": self.metadata,
        }
    
    def __str__(self) -> str:
        return (
            f"ValidationResult(status={self.status.value}, "
            f"valid={self.valid_rows}/{self.total_rows}, "
            f"errors={len(self.errors)}, warnings={len(self.warnings)})"
        )


class DataValidator:
    """
    High-level data validator combining Pandera schemas with business rules.
    
    Provides comprehensive validation with detailed error reporting
    and support for multiple validation stages.
    """
    
    def __init__(self, strict: bool = False):
        """
        Initialize validator.
        
        Args:
            strict: If True, fail on warnings; if False, warnings are logged only
        """
        self.strict = strict
        self.validation_history: List[ValidationResult] = []
    
    def validate_raw_data(self, df: pd.DataFrame, file_name: str = "") -> ValidationResult:
        """
        Validate raw sales data from CSV source.
        
        Args:
            df: Raw DataFrame from CSV
            file_name: Source file name for logging
            
        Returns:
            ValidationResult with validation details
        """
        start_time = datetime.now()
        logger.info(f"Starting raw data validation for {file_name}")
        
        errors = []
        warnings = []
        
        # Check required columns
        required_columns = [
            "order_id", "product_name", "quantity", "unit_price",
            "order_date", "customer_id", "country"
        ]
        
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            errors.append({
                "type": "missing_columns",
                "columns": list(missing_columns),
                "message": f"Missing required columns: {missing_columns}"
            })
            
            duration = (datetime.now() - start_time).total_seconds()
            result = ValidationResult(
                status=ValidationStatus.FAILED,
                is_valid=False,
                total_rows=len(df),
                valid_rows=0,
                invalid_rows=len(df),
                errors=errors,
                schema_name="RawSalesSchema",
                duration_seconds=duration,
                metadata={"file_name": file_name}
            )
            self.validation_history.append(result)
            logger.error(f"Raw validation failed: missing columns {missing_columns}")
            return result
        
        # Run Pandera schema validation
        try:
            RawSalesSchema.validate(df, lazy=True)
            schema_valid = True
        except pa.errors.SchemaErrors as e:
            schema_valid = False
            for _, row in e.failure_cases.iterrows():
                errors.append({
                    "type": "schema_error",
                    "column": row.get("column"),
                    "check": row.get("check"),
                    "index": row.get("index"),
                    "message": f"Column '{row.get('column')}' failed check: {row.get('check')}"
                })
        
        # Additional business rule checks
        warnings.extend(self._check_raw_business_rules(df))
        
        duration = (datetime.now() - start_time).total_seconds()
        
        result = ValidationResult(
            status=ValidationStatus.PASSED if schema_valid else ValidationStatus.FAILED,
            is_valid=schema_valid,
            total_rows=len(df),
            valid_rows=len(df) if schema_valid else 0,
            invalid_rows=0 if schema_valid else len(df),
            errors=errors,
            warnings=warnings,
            schema_name="RawSalesSchema",
            duration_seconds=duration,
            metadata={"file_name": file_name}
        )
        
        self.validation_history.append(result)
        logger.info(f"Raw validation completed: {result}")
        return result
    
    def validate_cleaned_data(
        self,
        df: pd.DataFrame,
        original_count: int = 0
    ) -> Tuple[ValidationResult, pd.DataFrame]:
        """
        Validate cleaned sales data.
        
        Args:
            df: Cleaned DataFrame
            original_count: Original row count before cleaning
            
        Returns:
            Tuple of (ValidationResult, validated DataFrame)
        """
        start_time = datetime.now()
        logger.info(f"Starting cleaned data validation")
        
        errors = []
        warnings = []
        
        # Run Pandera schema validation
        try:
            validated_df = CleanedSalesSchema.validate(df, lazy=True)
            is_valid = True
        except pa.errors.SchemaErrors as e:
            validated_df = df
            is_valid = False
            for _, row in e.failure_cases.iterrows():
                errors.append({
                    "type": "schema_error",
                    "column": row.get("column"),
                    "check": row.get("check"),
                    "index": row.get("index"),
                    "failure_case": str(row.get("failure_case"))[:100]
                })
        
        # Check data loss ratio
        if original_count > 0:
            loss_ratio = 1 - (len(df) / original_count)
            if loss_ratio > 0.5:
                warnings.append(
                    f"High data loss during cleaning: {loss_ratio:.1%} "
                    f"({original_count - len(df)} of {original_count} rows)"
                )
        
        # Additional quality checks
        warnings.extend(self._check_cleaned_quality(df))
        
        duration = (datetime.now() - start_time).total_seconds()
        
        result = ValidationResult(
            status=ValidationStatus.PASSED if is_valid else ValidationStatus.FAILED,
            is_valid=is_valid,
            total_rows=len(df),
            valid_rows=len(df) if is_valid else 0,
            invalid_rows=0 if is_valid else len(df),
            errors=errors,
            warnings=warnings,
            schema_name="CleanedSalesSchema",
            duration_seconds=duration,
            metadata={"original_count": original_count, "cleaned_count": len(df)}
        )
        
        self.validation_history.append(result)
        logger.info(f"Cleaned validation completed: {result}")
        return result, validated_df
    
    def validate_enriched_data(self, df: pd.DataFrame) -> Tuple[ValidationResult, pd.DataFrame]:
        """
        Validate enriched sales data ready for loading.
        
        Args:
            df: Enriched DataFrame with calculated fields
            
        Returns:
            Tuple of (ValidationResult, validated DataFrame)
        """
        start_time = datetime.now()
        logger.info(f"Starting enriched data validation")
        
        errors = []
        warnings = []
        
        # Verify required enrichment columns exist
        enrichment_columns = ["total_amount", "ingestion_timestamp"]
        missing = set(enrichment_columns) - set(df.columns)
        if missing:
            errors.append({
                "type": "missing_enrichment",
                "columns": list(missing),
                "message": f"Missing enrichment columns: {missing}"
            })
        
        # Run Pandera schema validation
        try:
            validated_df = EnrichedSalesSchema.validate(df, lazy=True)
            is_valid = True
        except pa.errors.SchemaErrors as e:
            validated_df = df
            is_valid = False
            for _, row in e.failure_cases.iterrows():
                errors.append({
                    "type": "schema_error",
                    "column": row.get("column"),
                    "check": row.get("check"),
                    "index": row.get("index")
                })
        
        # Verify total_amount calculation
        if "total_amount" in df.columns:
            expected_total = df["quantity"] * df["unit_price"]
            mismatch = (df["total_amount"] - expected_total).abs() > 0.01
            mismatch_count = mismatch.sum()
            if mismatch_count > 0:
                errors.append({
                    "type": "calculation_error",
                    "column": "total_amount",
                    "count": int(mismatch_count),
                    "message": f"{mismatch_count} rows have incorrect total_amount calculation"
                })
                is_valid = False
        
        duration = (datetime.now() - start_time).total_seconds()
        
        result = ValidationResult(
            status=ValidationStatus.PASSED if is_valid else ValidationStatus.FAILED,
            is_valid=is_valid,
            total_rows=len(df),
            valid_rows=len(df) if is_valid else 0,
            invalid_rows=0 if is_valid else len(df),
            errors=errors,
            warnings=warnings,
            schema_name="EnrichedSalesSchema",
            duration_seconds=duration,
            metadata={"total_revenue": float(df["total_amount"].sum()) if "total_amount" in df.columns else 0}
        )
        
        self.validation_history.append(result)
        logger.info(f"Enriched validation completed: {result}")
        return result, validated_df
    
    def _check_raw_business_rules(self, df: pd.DataFrame) -> List[str]:
        """Check business rules on raw data."""
        warnings = []
        
        # Check for unusual patterns
        if df["quantity"].dtype == object:
            non_numeric = df["quantity"].apply(
                lambda x: not str(x).replace("-", "").replace(".", "").isdigit()
                if pd.notna(x) else False
            )
            non_numeric_count = non_numeric.sum()
            if non_numeric_count > 0:
                warnings.append(f"{non_numeric_count} rows have non-numeric quantity values")
        
        # Check for empty order_ids
        empty_orders = df["order_id"].isna().sum() + (df["order_id"] == "").sum()
        if empty_orders > 0:
            warnings.append(f"{empty_orders} rows have empty order_id")
        
        return warnings
    
    def _check_cleaned_quality(self, df: pd.DataFrame) -> List[str]:
        """Check quality metrics on cleaned data."""
        warnings = []
        
        # Check for suspicious patterns
        unknown_customers = (df["customer_id"] == "UNKNOWN").sum()
        if unknown_customers > len(df) * 0.1:
            warnings.append(
                f"High unknown customer rate: {unknown_customers} "
                f"({unknown_customers / len(df):.1%})"
            )
        
        unknown_countries = (df["country"] == "Unknown").sum()
        if unknown_countries > len(df) * 0.1:
            warnings.append(
                f"High unknown country rate: {unknown_countries} "
                f"({unknown_countries / len(df):.1%})"
            )
        
        # Check for date range reasonableness
        if "order_date" in df.columns:
            date_range = (df["order_date"].max() - df["order_date"].min()).days
            if date_range > 365:
                warnings.append(f"Date range spans {date_range} days - verify data freshness")
        
        return warnings
    
    def get_validation_summary(self) -> Dict[str, Any]:
        """Get summary of all validation runs."""
        if not self.validation_history:
            return {"total_validations": 0}
        
        passed = sum(1 for r in self.validation_history if r.is_valid)
        failed = len(self.validation_history) - passed
        
        return {
            "total_validations": len(self.validation_history),
            "passed": passed,
            "failed": failed,
            "success_rate": passed / len(self.validation_history) if self.validation_history else 0,
            "total_errors": sum(len(r.errors) for r in self.validation_history),
            "total_warnings": sum(len(r.warnings) for r in self.validation_history),
        }
