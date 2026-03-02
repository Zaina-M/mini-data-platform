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
    @classmethod
    def _minio_endpoint(cls):
        return os.environ["MINIO_ENDPOINT"]

    @classmethod
    def _minio_access_key(cls):
        return os.environ["MINIO_ACCESS_KEY"]

    @classmethod
    def _minio_secret_key(cls):
        return os.environ["MINIO_SECRET_KEY"]

    @classmethod
    def _minio_secure(cls):
        return os.getenv("MINIO_SECURE", "false").lower() == "true"

    # PostgreSQL settings
    @classmethod
    def _postgres_host(cls):
        return os.environ["POSTGRES_HOST"]

    @classmethod
    def _postgres_port(cls):
        return int(os.environ["POSTGRES_PORT"])

    @classmethod
    def _postgres_db(cls):
        return os.environ["POSTGRES_DB"]

    @classmethod
    def _postgres_user(cls):
        return os.environ["POSTGRES_USER"]

    @classmethod
    def _postgres_password(cls):
        return os.environ["POSTGRES_PASSWORD"]


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
            ConnectionConfig._minio_endpoint(),
            access_key=ConnectionConfig._minio_access_key(),
            secret_key=ConnectionConfig._minio_secret_key(),
            secure=ConnectionConfig._minio_secure(),
        )
        logger.debug(f"MinIO client created for endpoint: {ConnectionConfig._minio_endpoint()}")
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
            host=ConnectionConfig._postgres_host(),
            port=ConnectionConfig._postgres_port(),
            database=ConnectionConfig._postgres_db(),
            user=ConnectionConfig._postgres_user(),
            password=ConnectionConfig._postgres_password(),
        )
        logger.debug(
            f"PostgreSQL connection established to "
            f"{ConnectionConfig._postgres_host()}:{ConnectionConfig._postgres_port()}/{ConnectionConfig._postgres_db()}"
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
