"""
Pandera Schema Definitions

Defines strict data validation schemas for sales data at each pipeline stage:
- RawSalesSchema: Validates incoming CSV data
- CleanedSalesSchema: Validates data after cleaning
- EnrichedSalesSchema: Validates data after enrichment

Uses Pandera for declarative, column-level validation with custom checks.
"""

from datetime import datetime
from typing import Optional

import pandas as pd
import pandera as pa
from pandera import Column, Check, DataFrameSchema
from pandera.typing import Series


# Custom check functions
def is_valid_order_id(series: pd.Series) -> pd.Series:
    """Check if order_id follows expected format (ORD-XXXXXX)."""
    return series.str.match(r"^ORD-\d{6}$", na=False)


def is_valid_customer_id(series: pd.Series) -> pd.Series:
    """Check if customer_id follows expected format (CUST-XXXXX) or UNKNOWN."""
    return series.str.match(r"^(CUST-\d{5}|UNKNOWN)$", na=False)


def is_reasonable_price(value: float) -> bool:
    """Check if price is within reasonable bounds."""
    return 0 < value <= 100000


def is_reasonable_quantity(value: int) -> bool:
    """Check if quantity is within reasonable bounds."""
    return 0 < value <= 10000


# Valid countries for validation
VALID_COUNTRIES = [
    "United States", "Canada", "United Kingdom", "Germany",
    "France", "Australia", "Japan", "Brazil", "India", "Mexico",
    "Spain", "Italy", "Netherlands", "Sweden", "Singapore", "Unknown"
]


class RawSalesSchema(pa.DataFrameModel):
    """
    Schema for raw sales data from CSV files.
    
    This schema validates the initial structure and basic data types
    before any cleaning or transformation.
    """
    order_id: Series[str] = pa.Field(
        nullable=False,
        coerce=True,
        description="Unique order identifier"
    )
    product_name: Series[str] = pa.Field(
        nullable=True,
        coerce=True,
        description="Name of the product"
    )
    quantity: Series[str] = pa.Field(
        nullable=True,
        coerce=True,
        description="Quantity ordered (may be string initially)"
    )
    unit_price: Series[str] = pa.Field(
        nullable=True,
        coerce=True,
        description="Price per unit (may be string initially)"
    )
    order_date: Series[str] = pa.Field(
        nullable=True,
        coerce=True,
        description="Date of order"
    )
    customer_id: Series[str] = pa.Field(
        nullable=True,
        coerce=True,
        description="Customer identifier"
    )
    country: Series[str] = pa.Field(
        nullable=True,
        coerce=True,
        description="Customer country"
    )
    
    class Config:
        name = "RawSalesSchema"
        strict = False  # Allow extra columns
        coerce = True


class CleanedSalesSchema(pa.DataFrameModel):
    """
    Schema for cleaned sales data.
    
    This schema validates data after cleaning transformations have been applied.
    All fields should have correct types and valid values.
    """
    order_id: Series[str] = pa.Field(
        nullable=False,
        unique=True,
        str_matches=r"^ORD-\d{6}$",
        description="Unique order identifier in format ORD-XXXXXX"
    )
    product_name: Series[str] = pa.Field(
        nullable=False,
        str_length={"min_value": 1, "max_value": 255},
        description="Name of the product (cleaned, trimmed)"
    )
    quantity: Series[int] = pa.Field(
        nullable=False,
        ge=1,
        le=10000,
        description="Quantity ordered (positive integer)"
    )
    unit_price: Series[float] = pa.Field(
        nullable=False,
        gt=0,
        le=100000,
        description="Price per unit (positive decimal)"
    )
    order_date: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Date of order (datetime)"
    )
    customer_id: Series[str] = pa.Field(
        nullable=False,
        str_matches=r"^(CUST-\d{5}|UNKNOWN)$",
        description="Customer identifier in format CUST-XXXXX or UNKNOWN"
    )
    country: Series[str] = pa.Field(
        nullable=False,
        isin=VALID_COUNTRIES,
        description="Customer country from valid list"
    )
    
    class Config:
        name = "CleanedSalesSchema"
        strict = True
        coerce = True


class EnrichedSalesSchema(pa.DataFrameModel):
    """
    Schema for enriched sales data ready for loading.
    
    This schema validates data after all transformations including
    calculated fields like total_amount and ingestion_timestamp.
    """
    order_id: Series[str] = pa.Field(
        nullable=False,
        unique=True,
        str_matches=r"^ORD-\d{6}$",
        description="Unique order identifier"
    )
    product_name: Series[str] = pa.Field(
        nullable=False,
        str_length={"min_value": 1, "max_value": 255},
        description="Name of the product"
    )
    quantity: Series[int] = pa.Field(
        nullable=False,
        ge=1,
        le=10000,
        description="Quantity ordered"
    )
    unit_price: Series[float] = pa.Field(
        nullable=False,
        gt=0,
        le=100000,
        description="Price per unit"
    )
    order_date: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Date of order"
    )
    customer_id: Series[str] = pa.Field(
        nullable=False,
        str_matches=r"^(CUST-\d{5}|UNKNOWN)$",
        description="Customer identifier"
    )
    country: Series[str] = pa.Field(
        nullable=False,
        isin=VALID_COUNTRIES,
        description="Customer country"
    )
    total_amount: Series[float] = pa.Field(
        nullable=False,
        gt=0,
        description="Calculated total (quantity * unit_price)"
    )
    ingestion_timestamp: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Timestamp when record was processed"
    )
    
    @pa.dataframe_check
    def total_amount_calculation(cls, df: pd.DataFrame) -> pd.Series:
        """Verify total_amount equals quantity * unit_price (with tolerance)."""
        expected = df["quantity"] * df["unit_price"]
        return (df["total_amount"] - expected).abs() < 0.01
    
    class Config:
        name = "EnrichedSalesSchema"
        strict = True
        coerce = True


# Functional schema definitions for more flexibility
RawSalesSchemaDict = DataFrameSchema(
    columns={
        "order_id": Column(str, nullable=False, coerce=True),
        "product_name": Column(str, nullable=True, coerce=True),
        "quantity": Column(str, nullable=True, coerce=True),
        "unit_price": Column(str, nullable=True, coerce=True),
        "order_date": Column(str, nullable=True, coerce=True),
        "customer_id": Column(str, nullable=True, coerce=True),
        "country": Column(str, nullable=True, coerce=True),
    },
    strict=False,
    coerce=True,
)


def validate_dataframe(
    df: pd.DataFrame,
    schema: type,
    lazy: bool = True
) -> tuple[bool, Optional[pd.DataFrame], Optional[str]]:
    """
    Validate a DataFrame against a Pandera schema.
    
    Args:
        df: DataFrame to validate
        schema: Pandera schema class
        lazy: If True, collect all errors; if False, fail fast
        
    Returns:
        tuple: (is_valid, validated_df, error_message)
    """
    try:
        validated_df = schema.validate(df, lazy=lazy)
        return True, validated_df, None
    except pa.errors.SchemaErrors as e:
        error_msg = f"Validation failed with {len(e.failure_cases)} error(s):\n"
        for _, row in e.failure_cases.iterrows():
            error_msg += f"  - Column '{row['column']}': {row['check']}\n"
        return False, None, error_msg
    except pa.errors.SchemaError as e:
        return False, None, str(e)
    except Exception as e:
        return False, None, f"Unexpected validation error: {e}"
