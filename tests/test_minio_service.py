"""
Tests for MinIO Service

Tests the MinIOService class for object storage operations.
Uses mocking to avoid requiring a real MinIO instance.
"""

import os
import sys
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Add dags directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "airflow", "dags"))

from services.minio_service import MinIOFile, MinIOService


class TestMinIOFile:
    """Tests for MinIOFile dataclass."""

    def test_minio_file_creation(self):
        """Test creating a MinIOFile."""
        file = MinIOFile(
            object_name="sales_data.csv",
            bucket="raw-sales",
            size=1024,
            last_modified=datetime.utcnow(),
            etag="abc123",
        )

        assert file.object_name == "sales_data.csv"
        assert file.bucket == "raw-sales"
        assert file.size == 1024

    def test_extension_property(self):
        """Test file extension detection."""
        csv_file = MinIOFile("data.csv", "bucket", 100, datetime.utcnow())
        json_file = MinIOFile("data.json", "bucket", 100, datetime.utcnow())
        no_ext_file = MinIOFile("data", "bucket", 100, datetime.utcnow())

        assert csv_file.extension == "csv"
        assert json_file.extension == "json"
        assert no_ext_file.extension == ""

    def test_is_csv_property(self):
        """Test CSV file detection."""
        csv_file = MinIOFile("data.csv", "bucket", 100, datetime.utcnow())
        json_file = MinIOFile("data.json", "bucket", 100, datetime.utcnow())

        assert csv_file.is_csv is True
        assert json_file.is_csv is False


class TestMinIOService:
    """Tests for MinIOService class."""

    @pytest.fixture
    def mock_minio(self):
        """Create a mock MinIO client."""
        with patch("services.minio_service.get_minio_client") as mock:
            client = MagicMock()
            mock.return_value = client
            yield client

    def test_service_initialization(self):
        """Test MinIOService initialization."""
        service = MinIOService()
        assert service.RAW_BUCKET == "raw-sales"
        assert service.PROCESSED_BUCKET == "processed"
        assert service.ARCHIVE_BUCKET == "archive"

    def test_ensure_buckets_exist(self, mock_minio):
        """Test bucket creation."""
        mock_minio.bucket_exists.return_value = False

        service = MinIOService()
        service._client = mock_minio
        service.ensure_buckets_exist()

        # Should call make_bucket for each bucket
        assert mock_minio.make_bucket.call_count == 3

    def test_ensure_buckets_exist_already_exists(self, mock_minio):
        """Test when buckets already exist."""
        mock_minio.bucket_exists.return_value = True

        service = MinIOService()
        service._client = mock_minio
        service.ensure_buckets_exist()

        # Should not call make_bucket
        assert mock_minio.make_bucket.call_count == 0

    def test_list_csv_files(self, mock_minio):
        """Test listing CSV files."""
        mock_obj1 = MagicMock()
        mock_obj1.object_name = "data1.csv"
        mock_obj1.size = 1024
        mock_obj1.last_modified = datetime.utcnow()
        mock_obj1.etag = "abc"

        mock_obj2 = MagicMock()
        mock_obj2.object_name = "data2.json"  # Not CSV
        mock_obj2.size = 512
        mock_obj2.last_modified = datetime.utcnow()
        mock_obj2.etag = "def"

        mock_minio.list_objects.return_value = [mock_obj1, mock_obj2]

        service = MinIOService()
        service._client = mock_minio
        files = service.list_csv_files()

        assert len(files) == 1
        assert files[0].object_name == "data1.csv"

    def test_detect_new_files(self, mock_minio):
        """Test detecting new files."""
        mock_obj = MagicMock()
        mock_obj.object_name = "sales_20240115.csv"
        mock_obj.size = 2048
        mock_obj.last_modified = datetime.utcnow()
        mock_obj.etag = "xyz"

        mock_minio.list_objects.return_value = [mock_obj]

        service = MinIOService()
        service._client = mock_minio
        files = service.detect_new_files()

        assert len(files) == 1
        assert files[0] == "sales_20240115.csv"

    def test_detect_new_files_empty(self, mock_minio):
        """Test detecting files when bucket is empty."""
        mock_minio.list_objects.return_value = []

        service = MinIOService()
        service._client = mock_minio
        files = service.detect_new_files()

        assert len(files) == 0

    def test_download_csv_as_dataframe(self, mock_minio, sample_csv_content):
        """Test downloading CSV and converting to DataFrame."""
        mock_response = MagicMock()
        mock_response.read.return_value = sample_csv_content.encode()
        mock_minio.get_object.return_value = mock_response

        service = MinIOService()
        service._client = mock_minio
        df, metadata = service.download_csv_as_dataframe("test.csv")

        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert metadata["file_name"] == "test.csv"
        assert "row_count" in metadata

    def test_upload_dataframe_as_csv(self, mock_minio, sample_cleaned_data):
        """Test uploading DataFrame as CSV."""
        service = MinIOService()
        service._client = mock_minio

        result = service.upload_dataframe_as_csv(sample_cleaned_data, "output.csv")

        assert result == "output.csv"
        mock_minio.put_object.assert_called_once()

    def test_archive_file(self, mock_minio):
        """Test archiving a file."""
        service = MinIOService()
        service._client = mock_minio

        archive_path = service.archive_file("test.csv")

        assert archive_path is not None
        mock_minio.copy_object.assert_called_once()
        mock_minio.remove_object.assert_called_once()

    def test_archive_file_keep_source(self, mock_minio):
        """Test archiving without deleting source."""
        service = MinIOService()
        service._client = mock_minio

        service.archive_file("test.csv", delete_source=False)

        mock_minio.copy_object.assert_called_once()
        mock_minio.remove_object.assert_not_called()

    def test_archive_files_batch(self, mock_minio):
        """Test archiving multiple files."""
        service = MinIOService()
        service._client = mock_minio

        files = ["file1.csv", "file2.csv", "file3.csv"]
        archived = service.archive_files(files)

        assert len(archived) == 3
        assert mock_minio.copy_object.call_count == 3

    def test_file_exists(self, mock_minio):
        """Test checking if file exists."""
        mock_minio.stat_object.return_value = MagicMock()

        service = MinIOService()
        service._client = mock_minio

        assert service.file_exists("existing.csv") is True

    def test_file_not_exists(self, mock_minio):
        """Test checking when file doesn't exist."""
        mock_minio.stat_object.side_effect = Exception("Not found")

        service = MinIOService()
        service._client = mock_minio

        assert service.file_exists("nonexistent.csv") is False

    def test_get_file_info(self, mock_minio):
        """Test getting file info."""
        mock_stat = MagicMock()
        mock_stat.size = 2048
        mock_stat.last_modified = datetime.utcnow()
        mock_stat.etag = "abc123"
        mock_minio.stat_object.return_value = mock_stat

        service = MinIOService()
        service._client = mock_minio

        info = service.get_file_info("test.csv")

        assert info is not None
        assert info.size == 2048
