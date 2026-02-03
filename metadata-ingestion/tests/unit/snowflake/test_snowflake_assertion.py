from datetime import datetime
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.snowflake.snowflake_assertion import (
    DataQualityMonitoringResult,
    SnowflakeAssertionsHandler,
)
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.metadata.schema_classes import (
    AssertionSourceTypeClass,
    AssertionTypeClass,
)


class TestDataQualityMonitoringResultModel:
    """Test the Pydantic model for DMF results."""

    def test_parses_argument_names_as_list(self):
        """Model should parse ARGUMENT_NAMES when already a list."""
        row = {
            "MEASUREMENT_TIME": datetime.now(),
            "METRIC_NAME": "null_check",
            "TABLE_NAME": "orders",
            "TABLE_SCHEMA": "public",
            "TABLE_DATABASE": "my_db",
            "VALUE": 1,
            "REFERENCE_ID": "ref_abc123",
            "ARGUMENT_NAMES": ["amount", "quantity"],
        }
        result = DataQualityMonitoringResult.model_validate(row)
        assert result.REFERENCE_ID == "ref_abc123"
        assert result.ARGUMENT_NAMES == ["amount", "quantity"]

    def test_parses_argument_names_from_json_string(self):
        """Model should parse ARGUMENT_NAMES from JSON string (Snowflake format)."""
        row = {
            "MEASUREMENT_TIME": datetime.now(),
            "METRIC_NAME": "null_check",
            "TABLE_NAME": "orders",
            "TABLE_SCHEMA": "public",
            "TABLE_DATABASE": "my_db",
            "VALUE": 1,
            "REFERENCE_ID": "ref_abc123",
            "ARGUMENT_NAMES": '["EMAIL", "ID"]',
        }
        result = DataQualityMonitoringResult.model_validate(row)
        assert result.ARGUMENT_NAMES == ["EMAIL", "ID"]

    def test_parses_empty_argument_names(self):
        """Model should return empty list for empty JSON array."""
        row = {
            "MEASUREMENT_TIME": datetime.now(),
            "METRIC_NAME": "table_level_check",
            "TABLE_NAME": "orders",
            "TABLE_SCHEMA": "public",
            "TABLE_DATABASE": "my_db",
            "VALUE": 1,
            "REFERENCE_ID": "ref_abc123",
            "ARGUMENT_NAMES": "[]",
        }
        result = DataQualityMonitoringResult.model_validate(row)
        assert result.ARGUMENT_NAMES == []


class TestDmfAssertionResultsQuery:
    """Test dmf_assertion_results query generation."""

    def test_query_filters_datahub_prefix_by_default(self):
        """Default query should filter for datahub__* DMFs only."""
        query = SnowflakeQuery.dmf_assertion_results(
            start_time_millis=1000,
            end_time_millis=2000,
            include_external=False,
        )
        assert "datahub" in query and "%" in query
        assert "METRIC_NAME ilike" in query
        assert "REFERENCE_ID" in query
        assert "ARGUMENT_NAMES" in query

    def test_query_includes_all_dmfs_when_external_enabled(self):
        """With include_external=True, no pattern filter."""
        query = SnowflakeQuery.dmf_assertion_results(
            start_time_millis=1000,
            end_time_millis=2000,
            include_external=True,
        )
        assert "ilike" not in query
        assert "REFERENCE_ID" in query
        assert "ARGUMENT_NAMES" in query


class TestExternalDmfGuidGeneration:
    """Test GUID generation for external DMFs using REFERENCE_ID."""

    @pytest.fixture
    def handler(self):
        """Create a handler with mocked dependencies."""
        config = MagicMock()
        config.platform_instance = None
        config.include_external_dmf_assertions = True
        report = MagicMock()
        connection = MagicMock()
        identifiers = MagicMock()
        identifiers.platform = "snowflake"
        return SnowflakeAssertionsHandler(config, report, connection, identifiers)

    def test_guid_is_deterministic(self, handler):
        """Same REFERENCE_ID should always produce same GUID."""
        result = DataQualityMonitoringResult(
            MEASUREMENT_TIME=datetime.now(),
            METRIC_NAME="null_check",
            TABLE_NAME="orders",
            TABLE_SCHEMA="public",
            TABLE_DATABASE="my_db",
            VALUE=1,
            REFERENCE_ID="ref_abc123",
            ARGUMENT_NAMES=[],
        )
        guid1 = handler._generate_external_dmf_guid(result)
        guid2 = handler._generate_external_dmf_guid(result)
        assert guid1 == guid2

    def test_guid_differs_for_different_reference_ids(self, handler):
        """Different REFERENCE_IDs should produce different URNs."""
        result1 = DataQualityMonitoringResult(
            MEASUREMENT_TIME=datetime.now(),
            METRIC_NAME="null_check",
            TABLE_NAME="orders",
            TABLE_SCHEMA="public",
            TABLE_DATABASE="my_db",
            VALUE=1,
            REFERENCE_ID="ref_123",
            ARGUMENT_NAMES=[],
        )
        result2 = DataQualityMonitoringResult(
            MEASUREMENT_TIME=datetime.now(),
            METRIC_NAME="null_check",
            TABLE_NAME="orders",
            TABLE_SCHEMA="public",
            TABLE_DATABASE="my_db",
            VALUE=1,
            REFERENCE_ID="ref_456",
            ARGUMENT_NAMES=[],
        )
        guid1 = handler._generate_external_dmf_guid(result1)
        guid2 = handler._generate_external_dmf_guid(result2)
        assert guid1 != guid2

    def test_guid_includes_platform_instance(self):
        """Platform instance should affect GUID when configured."""
        config_with_instance = MagicMock()
        config_with_instance.platform_instance = "prod"
        config_with_instance.include_external_dmf_assertions = True

        config_without_instance = MagicMock()
        config_without_instance.platform_instance = None
        config_without_instance.include_external_dmf_assertions = True

        report = MagicMock()
        connection = MagicMock()
        identifiers = MagicMock()
        identifiers.platform = "snowflake"

        handler_with = SnowflakeAssertionsHandler(
            config_with_instance, report, connection, identifiers
        )
        handler_without = SnowflakeAssertionsHandler(
            config_without_instance, report, connection, identifiers
        )

        result = DataQualityMonitoringResult(
            MEASUREMENT_TIME=datetime.now(),
            METRIC_NAME="null_check",
            TABLE_NAME="orders",
            TABLE_SCHEMA="public",
            TABLE_DATABASE="my_db",
            VALUE=1,
            REFERENCE_ID="ref_abc123",
            ARGUMENT_NAMES=[],
        )

        guid_with = handler_with._generate_external_dmf_guid(result)
        guid_without = handler_without._generate_external_dmf_guid(result)
        assert guid_with != guid_without


class TestAssertionInfoCreation:
    """Test AssertionInfo aspect creation for external DMFs."""

    @pytest.fixture
    def handler(self):
        """Create a handler with mocked dependencies."""
        config = MagicMock()
        config.platform_instance = None
        config.include_external_dmf_assertions = True
        report = MagicMock()
        connection = MagicMock()
        identifiers = MagicMock()
        identifiers.platform = "snowflake"
        return SnowflakeAssertionsHandler(config, report, connection, identifiers)

    def test_assertion_info_has_correct_type_and_source(self, handler):
        """External DMFs should use CUSTOM type and EXTERNAL source."""
        wu = handler._create_assertion_info_workunit(
            assertion_urn="urn:li:assertion:test123",
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.orders,PROD)",
            dmf_name="null_check",
            argument_names=[],
        )
        assertion_info = wu.metadata.aspect
        assert assertion_info.type == AssertionTypeClass.CUSTOM
        assert assertion_info.source.type == AssertionSourceTypeClass.EXTERNAL
        assert assertion_info.customProperties["snowflake_dmf_name"] == "null_check"

    def test_field_urn_set_for_single_column(self, handler):
        """Field URN should be set when DMF operates on single column."""
        wu = handler._create_assertion_info_workunit(
            assertion_urn="urn:li:assertion:test123",
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.orders,PROD)",
            dmf_name="null_check",
            argument_names=["amount"],
        )
        assertion_info = wu.metadata.aspect
        assert assertion_info.customAssertion.field is not None
        assert "amount" in assertion_info.customAssertion.field

    def test_field_urn_none_for_multi_column(self, handler):
        """Field URN should be None when DMF operates on multiple columns."""
        wu = handler._create_assertion_info_workunit(
            assertion_urn="urn:li:assertion:test123",
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.orders,PROD)",
            dmf_name="compare_columns",
            argument_names=["col1", "col2"],
        )
        assertion_info = wu.metadata.aspect
        assert assertion_info.customAssertion.field is None
        assert assertion_info.customProperties["snowflake_dmf_columns"] == "col1,col2"


class TestMixedDmfProcessing:
    """Test processing both DataHub and external DMFs together."""

    @pytest.fixture
    def handler(self):
        """Create a handler with mocked dependencies."""
        config = MagicMock()
        config.platform_instance = None
        config.include_external_dmf_assertions = True
        report = MagicMock()
        connection = MagicMock()
        identifiers = MagicMock()
        identifiers.platform = "snowflake"
        identifiers.get_dataset_identifier.return_value = "my_db.public.orders"
        identifiers.gen_dataset_urn.return_value = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.orders,PROD)"
        )
        return SnowflakeAssertionsHandler(config, report, connection, identifiers)

    def test_datahub_dmf_extracts_guid_from_name(self, handler):
        """DataHub DMFs (datahub__*) should extract GUID from name and not emit AssertionInfo."""
        row = {
            "MEASUREMENT_TIME": datetime.now(),
            "METRIC_NAME": "datahub__abc123",
            "TABLE_NAME": "orders",
            "TABLE_SCHEMA": "public",
            "TABLE_DATABASE": "my_db",
            "VALUE": 1,
            "REFERENCE_ID": "ref_xyz",
            "ARGUMENT_NAMES": '["col1"]',
        }
        discovered = ["my_db.public.orders"]
        workunits = handler._process_result_row(row, discovered)

        # Should have AssertionRunEvent and DataPlatformInstance (no AssertionInfo)
        assert len(workunits) == 2
        aspect_names = [wu.metadata.aspectName for wu in workunits]
        assert "assertionInfo" not in aspect_names
        assert "abc123" in workunits[0].metadata.entityUrn

    def test_external_dmf_emits_assertion_info(self, handler):
        """External DMFs should emit AssertionInfo."""
        row = {
            "MEASUREMENT_TIME": datetime.now(),
            "METRIC_NAME": "my_custom_check",
            "TABLE_NAME": "orders",
            "TABLE_SCHEMA": "public",
            "TABLE_DATABASE": "my_db",
            "VALUE": 1,
            "REFERENCE_ID": "ref_abc123",
            "ARGUMENT_NAMES": '["amount"]',
        }
        discovered = ["my_db.public.orders"]
        workunits = handler._process_result_row(row, discovered)

        # Should have AssertionInfo, AssertionRunEvent, and DataPlatformInstance
        assert len(workunits) == 3
        aspect_names = [wu.metadata.aspectName for wu in workunits]
        assert "assertionInfo" in aspect_names
