"""
MinIO Service Module

Provides high-level operations for MinIO object storage:
- File detection and listing
- File download and upload
- Archiving processed files
"""

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import List, Optional, Tuple

import pandas as pd
from minio.commonconfig import CopySource
from utils.connections import get_minio_client
from utils.logging_config import get_logger

logger = get_logger("minio_service")


@dataclass
class MinIOFile:
    """Represents a file in MinIO storage."""

    object_name: str
    bucket: str
    size: int
    last_modified: datetime
    etag: str = ""

    @property
    def extension(self) -> str:
        """Get file extension."""
        return self.object_name.rsplit(".", 1)[-1].lower() if "." in self.object_name else ""

    @property
    def is_csv(self) -> bool:
        """Check if file is a CSV."""
        return self.extension == "csv"


class MinIOService:
    """
    Service class for MinIO operations.

    Provides high-level methods for common MinIO operations
    used in the data pipeline.
    """

    # Bucket names
    RAW_BUCKET = "raw-sales"
    PROCESSED_BUCKET = "processed"
    ARCHIVE_BUCKET = "archive"

    def __init__(self):
        """Initialize MinIO service."""
        self._client = None

    @property
    def client(self):
        """Lazy-load MinIO client."""
        if self._client is None:
            self._client = get_minio_client()
        return self._client

    def ensure_buckets_exist(self) -> None:
        """Ensure all required buckets exist."""
        buckets = [self.RAW_BUCKET, self.PROCESSED_BUCKET, self.ARCHIVE_BUCKET]

        for bucket in buckets:
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
                logger.info(f"Created bucket: {bucket}")

    def list_csv_files(self, bucket: str = None) -> List[MinIOFile]:
        """
        List all CSV files in a bucket.

        """
        bucket = bucket or self.RAW_BUCKET

        try:
            objects = self.client.list_objects(bucket, recursive=True)
            csv_files = []

            for obj in objects:
                if obj.object_name.endswith(".csv"):
                    csv_files.append(
                        MinIOFile(
                            object_name=obj.object_name,
                            bucket=bucket,
                            size=obj.size,
                            last_modified=obj.last_modified,
                            etag=obj.etag or "",
                        )
                    )

            logger.info(f"Found {len(csv_files)} CSV file(s) in {bucket}")
            return csv_files

        except Exception as e:
            logger.error(f"Error listing files in {bucket}: {e}")
            raise

    def detect_new_files(self) -> List[str]:
        """
        Detect new CSV files in the raw-sales bucket.

        """
        files = self.list_csv_files(self.RAW_BUCKET)
        file_names = [f.object_name for f in files]

        if not file_names:
            logger.info("No CSV files found in raw-sales bucket")
        else:
            logger.info(f"Detected {len(file_names)} new file(s): {file_names}")

        return file_names

    def download_csv_as_dataframe(
        self, file_name: str, bucket: str = None
    ) -> Tuple[pd.DataFrame, dict]:
        """
        Download a CSV file and return as DataFrame.

        """
        bucket = bucket or self.RAW_BUCKET

        try:
            response = self.client.get_object(bucket, file_name)
            data = response.read()
            response.close()
            response.release_conn()

            df = pd.read_csv(BytesIO(data))

            metadata = {
                "file_name": file_name,
                "bucket": bucket,
                "row_count": len(df),
                "columns": list(df.columns),
                "download_timestamp": datetime.utcnow().isoformat(),
            }

            logger.info(
                f"Downloaded {file_name} from {bucket}: "
                f"{len(df)} rows, {len(df.columns)} columns"
            )

            return df, metadata

        except Exception as e:
            logger.error(f"Error downloading {file_name} from {bucket}: {e}")
            raise

    def upload_dataframe_as_csv(self, df: pd.DataFrame, file_name: str, bucket: str = None) -> str:
        """
        Upload a DataFrame as CSV to MinIO.

        """
        bucket = bucket or self.PROCESSED_BUCKET

        try:
            csv_buffer = BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)

            size = len(csv_buffer.getvalue())

            self.client.put_object(
                bucket, file_name, csv_buffer, length=size, content_type="text/csv"
            )

            logger.info(f"Uploaded {file_name} to {bucket}: {size} bytes")
            return file_name

        except Exception as e:
            logger.error(f"Error uploading {file_name} to {bucket}: {e}")
            raise

    def archive_file(
        self, file_name: str, source_bucket: str = None, delete_source: bool = True
    ) -> str:
        """
        Archive a file by moving it to the archive bucket.

        """
        source_bucket = source_bucket or self.RAW_BUCKET

        try:
            # Create archive path with date prefix
            archive_name = f"{datetime.utcnow().strftime('%Y/%m/%d')}/{file_name}"

            # Copy to archive
            self.client.copy_object(
                self.ARCHIVE_BUCKET,
                archive_name,
                CopySource(source_bucket, file_name),
            )

            logger.info(f"Copied {file_name} to archive/{archive_name}")

            # Remove from source bucket
            if delete_source:
                self.client.remove_object(source_bucket, file_name)
                logger.info(f"Removed {file_name} from {source_bucket}")

            return archive_name

        except Exception as e:
            logger.error(f"Error archiving {file_name}: {e}")
            raise

    def archive_files(
        self, file_names: List[str], source_bucket: str = None, delete_source: bool = True
    ) -> List[str]:
        """
        Archive multiple files.

        """
        archived = []

        for file_name in file_names:
            try:
                archive_path = self.archive_file(file_name, source_bucket, delete_source)
                archived.append(archive_path)
            except Exception as e:
                logger.error(f"Failed to archive {file_name}: {e}")
                # Continue with other files

        logger.info(f"Archived {len(archived)} of {len(file_names)} files")
        return archived

    def get_file_info(self, file_name: str, bucket: str = None) -> Optional[MinIOFile]:
        """
        Get information about a specific file.

        """
        bucket = bucket or self.RAW_BUCKET

        try:
            stat = self.client.stat_object(bucket, file_name)
            return MinIOFile(
                object_name=file_name,
                bucket=bucket,
                size=stat.size,
                last_modified=stat.last_modified,
                etag=stat.etag or "",
            )
        except Exception:
            return None

    def file_exists(self, file_name: str, bucket: str = None) -> bool:
        """Check if a file exists in a bucket."""
        return self.get_file_info(file_name, bucket) is not None
