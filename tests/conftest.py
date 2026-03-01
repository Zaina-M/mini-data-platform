"""
Pytest Configuration and Fixtures

Provides shared fixtures for testing the data pipeline components.
"""

import os
import sys
from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

# Add dags directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))


@pytest.fixture
def sample_raw_data() -> pd.DataFrame:
    """Create sample raw sales data for testing."""
    return pd.DataFrame(
        {
            "order_id": ["ORD-001001", "ORD-001002", "ORD-001003", "ORD-001004", "ORD-001005"],
            "product_name": ["Laptop Pro 15", "Wireless Mouse", "USB-C Hub", "Keyboard", "Monitor"],
            "quantity": ["2", "5", "3", "1", "2"],
            "unit_price": ["999.99", "29.99", "49.99", "129.99", "349.99"],
            "order_date": ["2024-01-15", "2024-01-16", "2024-01-17", "2024-01-18", "2024-01-19"],
            "customer_id": ["CUST-10001", "CUST-10002", "CUST-10003", "CUST-10004", "CUST-10005"],
            "country": ["United States", "Canada", "United Kingdom", "Germany", "France"],
        }
    )


@pytest.fixture
def sample_raw_data_with_errors() -> pd.DataFrame:
    """Create sample raw sales data with various data quality issues."""
    return pd.DataFrame(
        {
            "order_id": [
                "ORD-001001",
                "ORD-001002",
                "ORD-001003",
                "ORD-001001",
                "ORD-001005",
            ],  # Duplicate
            "product_name": [
                "Laptop Pro 15",
                "  Wireless Mouse  ",
                "",
                "Keyboard",
                "Monitor",
            ],  # Whitespace, empty
            "quantity": ["2", "-5", "invalid", "1", "0"],  # Negative, invalid, zero
            "unit_price": ["999.99", "29.99", "49.99", "0", "349.99"],  # Zero price
            "order_date": [
                "2024-01-15",
                "invalid-date",
                "2024-01-17",
                "2024-01-18",
                "2030-01-19",
            ],  # Invalid, future
            "customer_id": ["CUST-10001", "", "CUST-10003", "CUST-10004", "CUST-10005"],  # Empty
            "country": [
                "United States",
                "  Canada  ",
                "",
                "InvalidCountry",
                "France",
            ],  # Whitespace, empty, invalid
        }
    )


@pytest.fixture
def sample_cleaned_data() -> pd.DataFrame:
    """Create sample cleaned sales data with proper types."""
    return pd.DataFrame(
        {
            "order_id": ["ORD-001001", "ORD-001002", "ORD-001003", "ORD-001004", "ORD-001005"],
            "product_name": ["Laptop Pro 15", "Wireless Mouse", "USB-C Hub", "Keyboard", "Monitor"],
            "quantity": [2, 5, 3, 1, 2],
            "unit_price": [999.99, 29.99, 49.99, 129.99, 349.99],
            "order_date": pd.to_datetime(
                ["2024-01-15", "2024-01-16", "2024-01-17", "2024-01-18", "2024-01-19"]
            ),
            "customer_id": ["CUST-10001", "CUST-10002", "CUST-10003", "CUST-10004", "CUST-10005"],
            "country": ["United States", "Canada", "United Kingdom", "Germany", "France"],
        }
    )


@pytest.fixture
def sample_enriched_data(sample_cleaned_data) -> pd.DataFrame:
    """Create sample enriched sales data with calculated fields."""
    df = sample_cleaned_data.copy()
    df["total_amount"] = df["quantity"] * df["unit_price"]
    df["ingestion_timestamp"] = datetime.utcnow()
    return df


@pytest.fixture
def mock_minio_client():
    """Create a mock MinIO client."""
    mock = MagicMock()
    mock.bucket_exists.return_value = True
    mock.list_buckets.return_value = []
    return mock


@pytest.fixture
def mock_postgres_connection():
    """Create a mock PostgreSQL connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


@pytest.fixture
def sample_csv_content() -> str:
    """Create sample CSV content."""
    return """order_id,product_name,quantity,unit_price,order_date,customer_id,country
ORD-001001,Laptop Pro 15,2,999.99,2024-01-15,CUST-10001,United States
ORD-001002,Wireless Mouse,5,29.99,2024-01-16,CUST-10002,Canada
ORD-001003,USB-C Hub,3,49.99,2024-01-17,CUST-10003,United Kingdom
"""


@pytest.fixture
def large_sample_data() -> pd.DataFrame:
    """Create larger sample data for performance testing."""
    import random

    n_rows = 1000
    products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones"]
    countries = ["United States", "Canada", "United Kingdom", "Germany", "France"]

    return pd.DataFrame(
        {
            "order_id": [f"ORD-{i:06d}" for i in range(1, n_rows + 1)],
            "product_name": [random.choice(products) for _ in range(n_rows)],
            "quantity": [random.randint(1, 10) for _ in range(n_rows)],
            "unit_price": [round(random.uniform(10, 1000), 2) for _ in range(n_rows)],
            "order_date": pd.date_range(start="2024-01-01", periods=n_rows, freq="h"),
            "customer_id": [f"CUST-{random.randint(10000, 99999)}" for _ in range(n_rows)],
            "country": [random.choice(countries) for _ in range(n_rows)],
        }
    )
