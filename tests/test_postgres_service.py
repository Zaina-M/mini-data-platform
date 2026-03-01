"""
Tests for PostgreSQL Service

Tests the PostgresService class for database operations.
Uses mocking to avoid requiring a real PostgreSQL instance.
"""

import os
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Add dags directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))

from services.postgres_service import LoadResult, PostgresService


class TestLoadResult:
    """Tests for LoadResult dataclass."""

    def test_load_result_creation(self):
        """Test creating a LoadResult."""
        result = LoadResult(
            success=True, rows_inserted=100, rows_updated=10, rows_failed=2, duration_seconds=1.5
        )

        assert result.success is True
        assert result.rows_inserted == 100
        assert result.rows_updated == 10
        assert result.rows_failed == 2

    def test_total_processed_property(self):
        """Test total processed calculation."""
        result = LoadResult(
            success=True, rows_inserted=100, rows_updated=50, rows_failed=5, duration_seconds=1.0
        )

        assert result.total_processed == 150

    def test_to_dict_includes_all_fields(self):
        """Test that to_dict includes all necessary fields."""
        result = LoadResult(
            success=True,
            rows_inserted=100,
            rows_updated=0,
            rows_failed=0,
            duration_seconds=0.5,
            error_message=None,
        )

        d = result.to_dict()

        assert "success" in d
        assert "rows_inserted" in d
        assert "rows_updated" in d
        assert "rows_failed" in d
        assert "total_processed" in d
        assert "duration_seconds" in d


class TestPostgresService:
    """Tests for PostgresService class."""

    @pytest.fixture
    def mock_connection(self):
        """Create a mock PostgreSQL connection."""
        with patch("services.postgres_service.postgres_connection") as mock:
            conn = MagicMock()
            cursor = MagicMock()
            mock.return_value.__enter__ = MagicMock(return_value=(conn, cursor))
            mock.return_value.__exit__ = MagicMock(return_value=False)
            yield mock, conn, cursor

    def test_service_initialization(self):
        """Test PostgresService initialization."""
        service = PostgresService()
        assert service.SALES_TABLE == "sales"
        assert len(service.SALES_COLUMNS) > 0

    def test_load_sales_data_success(self, mock_connection, sample_enriched_data):
        """Test successful data loading."""
        mock, conn, cursor = mock_connection

        service = PostgresService()
        result = service.load_sales_data(sample_enriched_data)

        assert result.success is True
        assert result.rows_inserted == len(sample_enriched_data)
        assert result.rows_failed == 0

    def test_load_sales_data_empty_df(self, mock_connection):
        """Test loading empty DataFrame."""
        mock, conn, cursor = mock_connection

        service = PostgresService()
        result = service.load_sales_data(pd.DataFrame())

        # Empty DataFrame should still succeed with 0 rows
        assert result.rows_inserted == 0

    def test_load_batch_success(self, mock_connection):
        """Test batch loading."""
        mock, conn, cursor = mock_connection

        records = [
            {
                "order_id": "ORD-001001",
                "product_name": "Product A",
                "quantity": 2,
                "unit_price": 100.0,
                "order_date": datetime.now(),
                "customer_id": "CUST-10001",
                "country": "United States",
                "total_amount": 200.0,
                "ingestion_timestamp": datetime.utcnow(),
            }
        ]

        service = PostgresService()
        result = service.load_batch(records)

        assert result.success is True
        assert result.rows_inserted == 1

    def test_load_batch_empty(self, mock_connection):
        """Test batch loading with empty list."""
        mock, conn, cursor = mock_connection

        service = PostgresService()
        result = service.load_batch([])

        assert result.success is True
        assert result.rows_inserted == 0

    def test_get_row_count(self, mock_connection):
        """Test getting row count."""
        mock, conn, cursor = mock_connection
        cursor.fetchone.return_value = (500,)

        service = PostgresService()
        count = service.get_row_count()

        assert count == 500

    def test_get_row_count_error(self, mock_connection):
        """Test row count when query fails."""
        mock, conn, cursor = mock_connection
        cursor.execute.side_effect = Exception("Connection error")

        service = PostgresService()
        count = service.get_row_count()

        assert count == -1

    def test_get_date_range(self, mock_connection):
        """Test getting date range."""
        mock, conn, cursor = mock_connection
        min_date = datetime(2024, 1, 1)
        max_date = datetime(2024, 1, 31)
        cursor.fetchone.return_value = (min_date, max_date)

        service = PostgresService()
        start, end = service.get_date_range()

        assert start == min_date
        assert end == max_date

    def test_get_summary_stats(self, mock_connection):
        """Test getting summary statistics."""
        mock, conn, cursor = mock_connection
        cursor.fetchone.return_value = (
            100,  # total_orders
            50000.0,  # total_revenue
            500.0,  # avg_order_value
            50,  # unique_customers
            5,  # countries
            datetime(2024, 1, 1),  # first_order
            datetime(2024, 1, 31),  # last_order
        )

        service = PostgresService()
        stats = service.get_summary_stats()

        assert stats["total_orders"] == 100
        assert stats["total_revenue"] == 50000.0
        assert stats["avg_order_value"] == 500.0
        assert stats["unique_customers"] == 50

    def test_check_order_exists_true(self, mock_connection):
        """Test checking existing order."""
        mock, conn, cursor = mock_connection
        cursor.fetchone.return_value = (1,)

        service = PostgresService()
        exists = service.check_order_exists("ORD-001001")

        assert exists is True

    def test_check_order_exists_false(self, mock_connection):
        """Test checking non-existing order."""
        mock, conn, cursor = mock_connection
        cursor.fetchone.return_value = None

        service = PostgresService()
        exists = service.check_order_exists("ORD-999999")

        assert exists is False

    def test_delete_orders_by_date(self, mock_connection):
        """Test deleting orders by date."""
        mock, conn, cursor = mock_connection
        cursor.rowcount = 25

        service = PostgresService()
        deleted = service.delete_orders_by_date(datetime(2024, 1, 1))

        assert deleted == 25

    def test_health_check_success(self, mock_connection):
        """Test successful health check."""
        mock, conn, cursor = mock_connection
        cursor.fetchone.return_value = (True,)  # Table exists

        service = PostgresService()
        is_healthy = service.health_check()

        assert is_healthy is True

    def test_health_check_table_missing(self, mock_connection):
        """Test health check when table is missing."""
        mock, conn, cursor = mock_connection
        cursor.fetchone.return_value = (False,)  # Table doesn't exist

        service = PostgresService()
        is_healthy = service.health_check()

        assert is_healthy is False

    def test_health_check_connection_error(self, mock_connection):
        """Test health check with connection error."""
        mock, conn, cursor = mock_connection
        mock.return_value.__enter__.side_effect = Exception("Connection failed")

        service = PostgresService()
        is_healthy = service.health_check()

        assert is_healthy is False
