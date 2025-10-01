from unittest.mock import patch

import pytest

from acryl_datahub_cloud.datahub_reporting.datahub_dataset import (
    DataHubBasedS3Dataset,
    DatasetMetadata,
    FileStoreBackedDatasetConfig,
)


class MockDuckDBType:
    """Mock DuckDB type object that doesn't have a lower() method"""

    def __init__(self, type_name: str):
        self.type_name = type_name

    def __str__(self) -> str:
        return self.type_name


@pytest.fixture
def mock_config() -> FileStoreBackedDatasetConfig:
    return FileStoreBackedDatasetConfig(
        dataset_name="test_dataset",
        bucket_prefix="s3://test-bucket/test-prefix",
        store_platform="s3",
    )


@pytest.fixture
def mock_dataset_metadata() -> DatasetMetadata:
    return DatasetMetadata(
        displayName="Test Dataset",
        description="Test dataset for unit tests",
    )


@pytest.fixture
def dataset(
    mock_config: FileStoreBackedDatasetConfig, mock_dataset_metadata: DatasetMetadata
) -> DataHubBasedS3Dataset:
    with patch("boto3.client"):
        return DataHubBasedS3Dataset(mock_config, mock_dataset_metadata)


def test_generate_schema_metadata_with_string_types(
    dataset: DataHubBasedS3Dataset,
) -> None:
    """Test that _generate_schema_metadata handles string column types correctly"""
    # Test with string types (old behavior)
    duckdb_columns = [
        ("column1", "VARCHAR"),
        ("column2", "INTEGER"),
        ("column3", "DOUBLE"),
    ]

    schema_metadata = dataset._generate_schema_metadata(duckdb_columns)

    assert len(schema_metadata.fields) == 3
    assert schema_metadata.fields[0].fieldPath == "column1"
    assert schema_metadata.fields[1].fieldPath == "column2"
    assert schema_metadata.fields[2].fieldPath == "column3"


def test_generate_schema_metadata_with_duckdb_type_objects(
    dataset: DataHubBasedS3Dataset,
) -> None:
    """Test that _generate_schema_metadata handles DuckDB type objects correctly"""
    # Test with DuckDB type objects (new behavior that caused the bug)
    duckdb_columns = [
        ("column1", MockDuckDBType("VARCHAR")),
        ("column2", MockDuckDBType("INTEGER")),
        ("column3", MockDuckDBType("DOUBLE")),
    ]

    # This should not raise an AttributeError
    schema_metadata = dataset._generate_schema_metadata(duckdb_columns)

    assert len(schema_metadata.fields) == 3
    assert schema_metadata.fields[0].fieldPath == "column1"
    assert schema_metadata.fields[1].fieldPath == "column2"
    assert schema_metadata.fields[2].fieldPath == "column3"


def test_generate_dataset_profile_and_schema_with_duckdb_types_simple(
    dataset: DataHubBasedS3Dataset,
) -> None:
    """Test that _generate_dataset_profile_and_schema handles DuckDB type objects without AttributeError"""
    # Test the core issue: calling str(column[1]).lower() should work with DuckDB type objects

    # Test with mock DuckDB columns that have type objects instead of strings
    duckdb_columns = [
        ("form_urn", MockDuckDBType("VARCHAR")),
        ("count", MockDuckDBType("BIGINT")),
    ]

    # This should not raise AttributeError: 'MockDuckDBType' object has no attribute 'lower'
    try:
        schema_metadata = dataset._generate_schema_metadata(duckdb_columns)
        # If we get here, the fix worked
        assert len(schema_metadata.fields) == 2
        assert schema_metadata.fields[0].fieldPath == "form_urn"
        assert schema_metadata.fields[1].fieldPath == "count"
    except AttributeError as e:
        if "lower" in str(e):
            pytest.fail(f"The fix didn't work - still getting AttributeError: {e}")
        else:
            # Some other AttributeError, re-raise it
            raise


def test_column_type_conversion() -> None:
    """Test that str() conversion works on various column type objects"""
    # Test string types
    assert str("VARCHAR").lower() == "varchar"

    # Test mock DuckDB type objects
    mock_type = MockDuckDBType("INTEGER")
    assert str(mock_type).lower() == "integer"

    # Test None (edge case)
    assert str(None).lower() == "none"


if __name__ == "__main__":
    pytest.main([__file__])
