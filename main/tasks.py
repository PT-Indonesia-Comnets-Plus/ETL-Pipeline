"""
Airflow task callables for the ETL pipeline.
These functions are used as Python callables in Airflow PythonOperator tasks.
"""
from typing import Dict, Any, Optional
import logging
from main.main_pipeline import pipeline as etl_pipeline

logger = logging.getLogger(__name__)


def _handle_task_execution(task_name: str, task_func, **kwargs) -> None:
    """
    Generic task execution handler with consistent error handling and logging.

    Args:
        task_name: Name of the task for logging purposes
        task_func: The task function to execute
        **kwargs: Keyword arguments passed by Airflow

    Raises:
        Exception: Re-raises any exception from the task function
    """
    try:
        logger.info(f"Starting {task_name}")
        result = task_func(context=kwargs)
        if result:
            logger.info(f"{task_name} completed successfully")
        else:
            raise Exception(f"{task_name} returned False indicating failure")
    except Exception as e:
        logger.error(f"{task_name} failed: {e}")
        raise


def run_extractor(**kwargs) -> None:
    """
    Executes the data extraction process.

    Args:
        **kwargs: Keyword arguments passed by Airflow

    Raises:
        Exception: If extraction fails
    """
    _handle_task_execution("Data extraction", etl_pipeline.extract, **kwargs)


def run_asset_transformer(**kwargs) -> None:
    """
    Executes the asset data transformation process.

    Args:
        **kwargs: Keyword arguments passed by Airflow

    Raises:
        Exception: If asset transformation fails
    """
    _handle_task_execution("Asset data transformation",
                           etl_pipeline.transform_asset_data, **kwargs)


def run_user_transformer(**kwargs) -> None:
    """
    Executes the user data transformation process.

    Args:
        **kwargs: Keyword arguments passed by Airflow

    Raises:
        Exception: If user transformation fails
    """
    _handle_task_execution("User data transformation",
                           etl_pipeline.transform_user_data, **kwargs)


def run_validator(**kwargs) -> None:
    """
    Executes the data validation process.

    Args:
        **kwargs: Keyword arguments passed by Airflow

    Raises:
        Exception: If validation fails
    """
    _handle_task_execution(
        "Data validation", etl_pipeline.validate_data, **kwargs)


def run_loader(**kwargs) -> None:
    """
    Executes the data loading process.

    Args:
        **kwargs: Keyword arguments passed by Airflow

    Raises:
        Exception: If loading fails
    """
    _handle_task_execution("Data loading", etl_pipeline.load_data, **kwargs)


def run_complete_pipeline(**kwargs) -> None:
    """
    Executes the complete ETL pipeline end-to-end.
    This can be used as a single task in a simple DAG.

    Args:
        **kwargs: Keyword arguments passed by Airflow

    Raises:
        Exception: If pipeline execution fails
    """
    _handle_task_execution("Complete ETL pipeline",
                           etl_pipeline.run_pipeline, **kwargs)
