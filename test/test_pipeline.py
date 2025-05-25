"""
Integration test for the complete ETL pipeline.
Tests the ETLPipeline class and ensures all components work together correctly.
"""
import unittest
from unittest.mock import MagicMock, patch
import sys
import os
import logging
from main.main_pipeline import ETLPipeline

# Configure logging for tests
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class TestETLPipeline(unittest.TestCase):
    """Test cases for the ETLPipeline class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a mock task instance for Airflow context
        self.mock_ti = MagicMock()
        self.mock_ti.xcom_pull.return_value = {}
        self.mock_ti.xcom_push = MagicMock()

        # Create context with mock task instance
        self.context = {'ti': self.mock_ti}

        # Initialize pipeline with dependencies mocked
        self.pipeline = ETLPipeline()

        # Mock the component methods to avoid actual execution
        self.pipeline.extractor.run = MagicMock()
        self.pipeline.asset_transformer.run = MagicMock()
        self.pipeline.user_transformer.run = MagicMock()
        self.pipeline.validator.run = MagicMock()
        self.pipeline.loader.run = MagicMock()

    def test_extract(self):
        """Test the extract method."""
        result = self.pipeline.extract(context=self.context)
        self.assertTrue(result)
        self.pipeline.extractor.run.assert_called_once_with(ti=self.mock_ti)

    def test_transform_asset_data(self):
        """Test the transform_asset_data method."""
        result = self.pipeline.transform_asset_data(context=self.context)
        self.assertTrue(result)
        self.pipeline.asset_transformer.run.assert_called_once_with(
            ti=self.mock_ti)

    def test_transform_user_data(self):
        """Test the transform_user_data method."""
        result = self.pipeline.transform_user_data(context=self.context)
        self.assertTrue(result)
        self.pipeline.user_transformer.run.assert_called_once_with(
            ti=self.mock_ti)

    def test_validate_data(self):
        """Test the validate_data method."""
        result = self.pipeline.validate_data(context=self.context)
        self.assertTrue(result)
        self.pipeline.validator.run.assert_called_once_with(ti=self.mock_ti)

    def test_load_data(self):
        """Test the load_data method."""
        result = self.pipeline.load_data(context=self.context)
        self.assertTrue(result)
        self.pipeline.loader.run.assert_called_once_with(ti=self.mock_ti)

    def test_run_pipeline(self):
        """Test the complete pipeline execution."""
        # Mock all the individual pipeline steps
        self.pipeline.setup_database = MagicMock(return_value=True)
        self.pipeline.extract = MagicMock(return_value=True)
        self.pipeline.transform_asset_data = MagicMock(return_value=True)
        self.pipeline.transform_user_data = MagicMock(return_value=True)
        self.pipeline.validate_data = MagicMock(return_value=True)
        self.pipeline.load_data = MagicMock(return_value=True)

        # Run the pipeline
        result = self.pipeline.run_pipeline(context=self.context)

        # Verify the result and that all methods were called
        self.assertTrue(result)
        self.pipeline.setup_database.assert_called_once()
        self.pipeline.extract.assert_called_once_with(context=self.context)
        self.pipeline.transform_asset_data.assert_called_once_with(
            context=self.context)
        self.pipeline.transform_user_data.assert_called_once_with(
            context=self.context)
        self.pipeline.validate_data.assert_called_once_with(
            context=self.context)
        self.pipeline.load_data.assert_called_once_with(context=self.context)

    def test_pipeline_failure_handling(self):
        """Test that the pipeline handles failures correctly."""
        # Make one of the steps fail
        self.pipeline.setup_database = MagicMock(return_value=True)
        self.pipeline.extract = MagicMock(return_value=True)
        self.pipeline.transform_asset_data = MagicMock(
            return_value=False)  # This step fails

        # Run the pipeline
        result = self.pipeline.run_pipeline(context=self.context)

        # Verify the result and that methods before the failure were called
        self.assertFalse(result)
        self.pipeline.setup_database.assert_called_once()
        self.pipeline.extract.assert_called_once_with(context=self.context)
        self.pipeline.transform_asset_data.assert_called_once_with(
            context=self.context)

        # Verify that methods after the failure were not called
        self.pipeline.transform_user_data.assert_not_called()
        self.pipeline.validate_data.assert_not_called()
        self.pipeline.load_data.assert_not_called()


if __name__ == '__main__':
    unittest.main()
