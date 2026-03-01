"""
Connection Utilities Module

Centralized connection management for MinIO and PostgreSQL.
Implements connection pooling and proper error handling.
"""

import os
from contextlib import contextmanager
from typing import Generator

from .logging_config import get_logger

logger = get_logger("connections")


class ConnectionConfig:
    """Configuration dataclass for connection settings."""

    # MinIO settings
    MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

    # PostgreSQL settings
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "analytics-db")
    POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB = os.getenv("POSTGRES_DB", "analytics")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "analytics")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "analytics")


def get_minio_client():
    """
    Initialize and return MinIO client.

    Returns:
        Minio: Configured MinIO client instance

    Raises:
        ImportError: If minio package is not installed
        Exception: If connection fails
    """
    from minio import Minio

    try:
        client = Minio(
            ConnectionConfig.MINIO_ENDPOINT,
            access_key=ConnectionConfig.MINIO_ACCESS_KEY,
            secret_key=ConnectionConfig.MINIO_SECRET_KEY,
            secure=ConnectionConfig.MINIO_SECURE,
        )
        logger.debug(f"MinIO client created for endpoint: {ConnectionConfig.MINIO_ENDPOINT}")
        return client
    except Exception as e:
        logger.error(f"Failed to create MinIO client: {e}")
        raise


def get_postgres_connection():
    """
    Create PostgreSQL connection using psycopg2.

    Returns:
        connection: psycopg2 connection object

    Raises:
        ImportError: If psycopg2 package is not installed
        Exception: If connection fails
    """
    import psycopg2

    try:
        conn = psycopg2.connect(
            host=ConnectionConfig.POSTGRES_HOST,
            port=ConnectionConfig.POSTGRES_PORT,
            database=ConnectionConfig.POSTGRES_DB,
            user=ConnectionConfig.POSTGRES_USER,
            password=ConnectionConfig.POSTGRES_PASSWORD,
        )
        logger.debug(
            f"PostgreSQL connection established to "
            f"{ConnectionConfig.POSTGRES_HOST}:{ConnectionConfig.POSTGRES_PORT}/{ConnectionConfig.POSTGRES_DB}"
        )
        return conn
    except Exception as e:
        logger.error(f"Failed to create PostgreSQL connection: {e}")
        raise


@contextmanager
def postgres_connection() -> Generator:
    """
    Context manager for PostgreSQL connections.
    Automatically handles commit/rollback and connection cleanup.

    Usage:
        with postgres_connection() as (conn, cursor):
            cursor.execute("SELECT * FROM sales")

    Yields:
        tuple: (connection, cursor) objects
    """
    conn = None
    cursor = None
    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()
        yield conn, cursor
        conn.commit()
        logger.debug("PostgreSQL transaction committed")
    except Exception as e:
        if conn:
            conn.rollback()
            logger.error(f"PostgreSQL transaction rolled back: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            logger.debug("PostgreSQL connection closed")


@contextmanager
def minio_client() -> Generator:
    """
    Context manager for MinIO client.

    Usage:
        with minio_client() as client:
            objects = client.list_objects("bucket")

    Yields:
        Minio: MinIO client instance
    """
    client = get_minio_client()
    try:
        yield client
    finally:
        # MinIO client doesn't need explicit cleanup, but this provides consistency
        logger.debug("MinIO client context closed")


def check_minio_health() -> bool:
    """
    Check if MinIO service is accessible.

    Returns:
        bool: True if MinIO is accessible
    """
    try:
        client = get_minio_client()
        client.list_buckets()
        logger.info("MinIO health check passed")
        return True
    except Exception as e:
        logger.error(f"MinIO health check failed: {e}")
        return False


def check_postgres_health() -> bool:
    """
    Check if PostgreSQL service is accessible.

    Returns:
        bool: True if PostgreSQL is accessible
    """
    try:
        with postgres_connection() as (conn, cursor):
            cursor.execute("SELECT 1")
            logger.info("PostgreSQL health check passed")
            return True
    except Exception as e:
        logger.error(f"PostgreSQL health check failed: {e}")
        return False
