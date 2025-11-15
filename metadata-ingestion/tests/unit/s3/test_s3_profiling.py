"""Unit tests for S3 profiling functionality."""

import pytest

# Check if profiling dependencies are available
try:
    import pydeequ  # noqa: F401
    import pyspark  # noqa: F401

    _PROFILING_ENABLED = True
except ImportError:
    _PROFILING_ENABLED = False

from datahub.ingestion.source.profiling.common import Cardinality
from datahub.ingestion.source.s3.profiling import (
    _SingleColumnSpec,
    null_str,
)
from datahub.metadata.schema_classes import DatasetFieldProfileClass


class TestNullStr:
    """Tests for the null_str utility function."""

    def test_null_str_with_string(self):
        """Test null_str with a regular string."""
        assert null_str("test") == "test"

    def test_null_str_with_int(self):
        """Test null_str with an integer."""
        assert null_str(42) == "42"

    def test_null_str_with_float(self):
        """Test null_str with a float."""
        assert null_str(3.14) == "3.14"

    def test_null_str_with_none(self):
        """Test null_str with None returns None."""
        assert null_str(None) is None

    def test_null_str_with_zero(self):
        """Test null_str with zero."""
        assert null_str(0) == "0"

    def test_null_str_with_empty_string(self):
        """Test null_str with empty string."""
        assert null_str("") == ""

    def test_null_str_with_bool(self):
        """Test null_str with boolean."""
        assert null_str(True) == "True"
        assert null_str(False) == "False"


class TestSingleColumnSpec:
    """Tests for the _SingleColumnSpec dataclass."""

    def test_single_column_spec_creation(self):
        """Test creating a _SingleColumnSpec instance."""
        column_profile = DatasetFieldProfileClass(fieldPath="test_column")
        spec = _SingleColumnSpec(
            column="test_column",
            column_profile=column_profile,
        )

        assert spec.column == "test_column"
        assert spec.column_profile == column_profile
        assert spec.histogram_distinct is None
        assert spec.unique_count is None
        assert spec.non_null_count is None
        assert spec.cardinality is None

    def test_single_column_spec_with_all_fields(self):
        """Test creating a _SingleColumnSpec with all fields populated."""
        column_profile = DatasetFieldProfileClass(fieldPath="test_column")
        spec = _SingleColumnSpec(
            column="test_column",
            column_profile=column_profile,
            histogram_distinct=True,
            unique_count=100,
            non_null_count=95,
            cardinality=Cardinality.MANY,
        )

        assert spec.column == "test_column"
        assert spec.histogram_distinct is True
        assert spec.unique_count == 100
        assert spec.non_null_count == 95
        assert spec.cardinality == Cardinality.MANY


@pytest.mark.skipif(
    not _PROFILING_ENABLED,
    reason="PySpark not available, skipping profiling tests",
)
class TestSingleTableProfiler:
    """Tests for the _SingleTableProfiler class."""

    def _create_mock_dataframe(self, columns, row_count=10, column_types=None):
        """Helper to create a mock DataFrame."""
