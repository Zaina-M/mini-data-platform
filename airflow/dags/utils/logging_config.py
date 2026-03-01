"""
Logging Configuration Module

Configures logging to write to files instead of console.
Supports structured logging with proper formatting.
"""

import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(
    log_dir: str = "/opt/airflow/logs/pipeline",
    log_level: int = logging.INFO,
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
) -> None:
    """
    Setup application-wide logging configuration.

    Args:
        log_dir: Directory to store log files
        log_level: Logging level (default: INFO)
        max_bytes: Max size per log file before rotation
        backup_count: Number of backup files to keep
    """
    # Create log directory if it doesn't exist
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    # Define log format
    log_format = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s:%(lineno)d | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Create main pipeline log file handler
    main_handler = RotatingFileHandler(
        filename=log_path / "sales_pipeline.log",
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    main_handler.setFormatter(log_format)
    main_handler.setLevel(log_level)

    # Create error-only log file handler
    error_handler = RotatingFileHandler(
        filename=log_path / "sales_pipeline_errors.log",
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    error_handler.setFormatter(log_format)
    error_handler.setLevel(logging.ERROR)

    # Create daily log file handler
    today = datetime.now().strftime("%Y-%m-%d")
    daily_handler = RotatingFileHandler(
        filename=log_path / f"sales_pipeline_{today}.log",
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    daily_handler.setFormatter(log_format)
    daily_handler.setLevel(log_level)

    # Configure root logger for the pipeline
    root_logger = logging.getLogger("sales_pipeline")
    root_logger.setLevel(log_level)

    # Remove existing handlers to avoid duplicates
    root_logger.handlers = []

    # Add handlers
    root_logger.addHandler(main_handler)
    root_logger.addHandler(error_handler)
    root_logger.addHandler(daily_handler)

    # Prevent propagation to avoid duplicate logs in Airflow
    root_logger.propagate = False

    return root_logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for the specified module.

    Args:
        name: Name of the module/component

    Returns:
        Logger instance configured for file output
    """
    # Ensure logging is setup
    parent_logger = logging.getLogger("sales_pipeline")

    if not parent_logger.handlers:
        setup_logging()

    # Create child logger
    logger = logging.getLogger(f"sales_pipeline.{name}")
    logger.setLevel(parent_logger.level)

    return logger


class PipelineLogger:
    """
    Context manager for pipeline logging with automatic entry/exit tracking.

    Usage:
        with PipelineLogger("task_name") as logger:
            logger.info("Processing...")
    """

    def __init__(self, task_name: str, **context):
        self.task_name = task_name
        self.context = context
        self.logger = get_logger(task_name)
        self.start_time = None

    def __enter__(self):
        self.start_time = datetime.now()
        run_id = self.context.get("run_id", "unknown")
        self.logger.info(f"Starting task '{self.task_name}' | run_id={run_id}")
        return self.logger

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = (datetime.now() - self.start_time).total_seconds()

        if exc_type:
            self.logger.error(
                f"Task '{self.task_name}' failed after {duration:.2f}s | "
                f"error={exc_type.__name__}: {exc_val}"
            )
        else:
            self.logger.info(f"Task '{self.task_name}' completed successfully in {duration:.2f}s")

        return False  # Don't suppress exceptions
