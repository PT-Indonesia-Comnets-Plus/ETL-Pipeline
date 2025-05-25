"""
Main ETL pipeline module that integrates all ETL components into a single workflow.
Provides a higher-level abstraction that can be used outside of Airflow if needed.
"""
import logging
from typing import Dict, Any, Optional

from main.extract import Extractor
from main.transform_asset import AssetTransformer
from main.transform_user import UserTransformer
from main.validate import DataValidator
from main.load import Loader
from main.utils.create_database import ensure_database_schema

logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    Integrated ETL pipeline that orchestrates the entire data processing workflow.

    This class provides methods to run the complete pipeline or individual steps,
    with consistent error handling and logging throughout the process.

    Attributes:
        extractor: Data extraction component
        asset_transformer: Asset data transformation component
        user_transformer: User data transformation component
        validator: Data validation component
        loader: Data loading component
    """

    def __init__(self) -> None:
        """Initialize ETL pipeline components."""
        self.extractor = Extractor()
        self.asset_transformer = AssetTransformer()
        self.user_transformer = UserTransformer()
        self.validator = DataValidator()
        self.loader = Loader()

    def _execute_step(self, step_name: str, step_func, context: Optional[Dict[str, Any]] = None) -> bool:
        """
        Execute a pipeline step with consistent error handling.

        Args:
            step_name: Human-readable name of the step
            step_func: Function to execute
            context: Optional execution context for Airflow integration

        Returns:
            bool: True if step was successful, False otherwise
        """
        try:
            logger.info(f"Executing {step_name}")
            if context and 'ti' in context:
                step_func(ti=context['ti'])
            else:
                step_func()
            logger.info(f"{step_name} completed successfully")
            return True
        except Exception as e:
            logger.error(f"{step_name} failed: {e}")
            return False

    def setup_database(self) -> bool:
        """
        Set up the database schema required for the ETL process.        Returns:
            bool: True if setup was successful, False otherwise
        """
        try:
            logger.info("Setting up database schema")
            ensure_database_schema()
            logger.info("Database schema setup completed successfully")
            return True
        except Exception as e:
            logger.error(f"Database setup failed: {e}")
            return False

    def extract(self, context: Optional[Dict[str, Any]] = None) -> bool:
        """
        Execute the extraction step of the ETL pipeline.

        Args:
            context: Optional execution context for Airflow integration

        Returns:
            bool: True if extraction was successful, False otherwise
        """
        return self._execute_step("data extraction", self.extractor.run, context)

    def transform_asset_data(self, context: Optional[Dict[str, Any]] = None) -> bool:
        """
        Transform asset data extracted in the previous step.

        Args:
            context: Optional execution context for Airflow integration

        Returns:
            bool: True if transformation was successful, False otherwise
        """
        return self._execute_step("asset data transformation", self.asset_transformer.run, context)

    def transform_user_data(self, context: Optional[Dict[str, Any]] = None) -> bool:
        """
        Transform user data extracted in the previous step.

        Args:
            context: Optional execution context for Airflow integration

        Returns:
            bool: True if transformation was successful, False otherwise
        """
        return self._execute_step("user data transformation", self.user_transformer.run, context)

    def validate_data(self, context: Optional[Dict[str, Any]] = None) -> bool:
        """
        Validate and split the transformed data.

        Args:
            context: Optional execution context for Airflow integration

        Returns:
            bool: True if validation was successful, False otherwise
        """
        return self._execute_step("data validation", self.validator.run, context)

    def load_data(self, context: Optional[Dict[str, Any]] = None) -> bool:
        """
        Load the validated data to destination systems.

        Args:
            context: Optional execution context for Airflow integration

        Returns:
            bool: True if loading was successful, False otherwise
        """
        return self._execute_step("data loading", self.loader.run, context)

    def run_pipeline(self, context: Optional[Dict[str, Any]] = None) -> bool:
        """
        Run the complete ETL pipeline end-to-end.

        Args:
            context: Optional execution context for Airflow integration

        Returns:
            bool: True if the pipeline completed successfully, False otherwise
        """
        logger.info("Starting ETL pipeline execution")

        # Pipeline steps in order
        pipeline_steps = [
            ("Database setup", self.setup_database),
            ("Data extraction", lambda: self.extract(context)),
            ("Asset data transformation", lambda: self.transform_asset_data(context)),
            ("User data transformation", lambda: self.transform_user_data(context)),
            ("Data validation", lambda: self.validate_data(context)),
            ("Data loading", lambda: self.load_data(context)),
        ]

        for step_name, step_func in pipeline_steps:
            if not step_func():
                logger.error(
                    f"Pipeline aborted due to {step_name.lower()} failure")
                return False

        logger.info("ETL pipeline completed successfully")
        return True


# Singleton instance that can be imported and used elsewhere
pipeline = ETLPipeline()
