"""
Common utility functions used across the ETL pipeline.
Provides shared functionality to reduce code duplication.
"""
import os
import logging
from typing import Optional
from datetime import datetime

from main.config import DEFAULT_TEMP_DIR

logger = logging.getLogger(__name__)


def ensure_temp_directory(temp_dir: str = DEFAULT_TEMP_DIR) -> str:
    """
    Ensure the temporary directory exists.

    Args:
        temp_dir: Path to temporary directory

    Returns:
        str: Path to the temporary directory

    Raises:
        OSError: If directory cannot be created
    """
    try:
        os.makedirs(temp_dir, exist_ok=True)
        logger.debug(f"Temporary directory ensured: {temp_dir}")
        return temp_dir
    except OSError as e:
        logger.error(f"Failed to create temporary directory {temp_dir}: {e}")
        raise


def generate_timestamped_filename(base_name: str, run_id: str, extension: str = ".parquet") -> str:
    """
    Generate a timestamped filename for data files.

    Args:
        base_name: Base name for the file
        run_id: Unique run identifier
        extension: File extension (default: .parquet)

    Returns:
        str: Timestamped filename
    """
    timestamp = datetime.now().isoformat().replace(":", "")
    return f"{base_name}_{run_id}_{timestamp}{extension}"


def safe_file_operation(operation_name: str, operation_func, *args, **kwargs):
    """
    Execute a file operation with error handling and logging.

    Args:
        operation_name: Human-readable name of the operation
        operation_func: Function to execute
        *args: Positional arguments for the function
        **kwargs: Keyword arguments for the function

    Returns:
        Result of the operation_func

    Raises:
        Exception: Re-raises any exception from operation_func with context
    """
    try:
        logger.debug(f"Starting {operation_name}")
        result = operation_func(*args, **kwargs)
        logger.debug(f"Completed {operation_name}")
        return result
    except Exception as e:
        logger.error(f"Failed {operation_name}: {e}")
        raise Exception(f"{operation_name} failed: {e}") from e


def log_dataframe_info(df, data_label: str) -> None:
    """
    Log basic information about a DataFrame.

    Args:
        df: pandas DataFrame to inspect
        data_label: Human-readable label for the data
    """
    if df is not None and hasattr(df, 'shape'):
        logger.info(
            f"{data_label} DataFrame: {df.shape[0]} rows, {df.shape[1]} columns")
        if df.empty:
            logger.warning(f"{data_label} DataFrame is empty")
    else:
        logger.warning(f"{data_label} DataFrame is None or invalid")
