from datetime import datetime
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.source.snowflake.snowflake_assertion import (
    DataQualityMonitoringResult,
    SnowflakeAssertionsHandler,
)
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.metadata.com.linkedin.pegasus2avro.assertion import AssertionResultType
from datahub.metadata.com.linkedin.pegasus2avro.common import DataPlatformInstance
from datahub.metadata.schema_classes import (
    AssertionSourceTypeClass,
    AssertionTypeClass,
)


class TestDataQualityMonitoringResultModel:
    """Test the Pydantic model for DMF results."""

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
            "ARGUMENT_NAMES": '["amount", "quantity"]',
        }
        result = DataQualityMonitoringResult.model_validate(row)
        assert result.REFERENCE_ID == "ref_abc123"
        assert result.ARGUMENT_NAMES == ["amount", "quantity"]

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
        config.include_externally_managed_dmfs = True
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
            ARGUMENT_NAMES="[]",
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
            ARGUMENT_NAMES="[]",
        )
        result2 = DataQualityMonitoringResult(
            MEASUREMENT_TIME=datetime.now(),
            METRIC_NAME="null_check",
            TABLE_NAME="orders",
            TABLE_SCHEMA="public",
            TABLE_DATABASE="my_db",
            VALUE=1,
            REFERENCE_ID="ref_456",
            ARGUMENT_NAMES="[]",
        )
        guid1 = handler._generate_external_dmf_guid(result1)
        guid2 = handler._generate_external_dmf_guid(result2)
        assert guid1 != guid2

    def test_guid_includes_platform_instance(self):
        """Platform instance should affect GUID when configured."""
        config_with_instance = MagicMock()
        config_with_instance.platform_instance = "prod"
        config_with_instance.include_externally_managed_dmfs = True

        config_without_instance = MagicMock()
        config_without_instance.platform_instance = None
        config_without_instance.include_externally_managed_dmfs = True

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
            ARGUMENT_NAMES="[]",
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
        config.include_externally_managed_dmfs = True
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
            reference_id="ref_abc123",
        )
        assertion_info = wu.metadata.aspect
        assert assertion_info.type == AssertionTypeClass.CUSTOM
        assert assertion_info.source.type == AssertionSourceTypeClass.EXTERNAL
        assert assertion_info.customProperties["snowflake_dmf_name"] == "null_check"
        assert assertion_info.customProperties["snowflake_reference_id"] == "ref_abc123"

    def test_field_urn_set_for_single_column(self, handler):
        """Field URN should be set when DMF operates on single column."""
        wu = handler._create_assertion_info_workunit(
            assertion_urn="urn:li:assertion:test123",
            dataset_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.orders,PROD)",
            dmf_name="null_check",
            argument_names=["amount"],
            reference_id="ref_abc123",
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
            reference_id="ref_abc123",
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
        config.include_externally_managed_dmfs = True
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


class TestDataPlatformInstance:
    """Test DataPlatformInstance aspect generation."""

    def test_data_platform_instance_emitted_for_external_dmf(self):
        """External DMFs should emit DataPlatformInstance aspect."""
        config = MagicMock()
        config.platform_instance = "my_instance"
        config.include_externally_managed_dmfs = True
        report = MagicMock()
        connection = MagicMock()
        identifiers = MagicMock()
        identifiers.platform = "snowflake"
        identifiers.get_dataset_identifier.return_value = "my_db.public.orders"
        identifiers.gen_dataset_urn.return_value = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.orders,PROD)"
        )
        handler = SnowflakeAssertionsHandler(config, report, connection, identifiers)

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

        # Find DataPlatformInstance workunit using type-safe filtering
        platform_instance_wus = [
            wu for wu in workunits if wu.get_aspect_of_type(DataPlatformInstance)
        ]
        assert len(platform_instance_wus) == 1

        aspect = platform_instance_wus[0].get_aspect_of_type(DataPlatformInstance)
        assert aspect is not None
        assert aspect.platform == "urn:li:dataPlatform:snowflake"
        assert (
            aspect.instance
            == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:snowflake,my_instance)"
        )

    def test_data_platform_instance_emitted_for_datahub_dmf(self):
        """DataHub DMFs should also emit DataPlatformInstance aspect."""
        config = MagicMock()
        config.platform_instance = "prod"
        config.include_externally_managed_dmfs = True
        report = MagicMock()
        connection = MagicMock()
        identifiers = MagicMock()
        identifiers.platform = "snowflake"
        identifiers.get_dataset_identifier.return_value = "my_db.public.orders"
        identifiers.gen_dataset_urn.return_value = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.orders,PROD)"
        )
        handler = SnowflakeAssertionsHandler(config, report, connection, identifiers)

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

        # Find DataPlatformInstance workunit using type-safe filtering
        platform_instance_wus = [
            wu for wu in workunits if wu.get_aspect_of_type(DataPlatformInstance)
        ]
        assert len(platform_instance_wus) == 1

        aspect = platform_instance_wus[0].get_aspect_of_type(DataPlatformInstance)
        assert aspect is not None
        assert aspect.platform == "urn:li:dataPlatform:snowflake"
        assert (
            aspect.instance
            == "urn:li:dataPlatformInstance:(urn:li:dataPlatform:snowflake,prod)"
        )

    def test_data_platform_instance_without_instance_configured(self):
        """DataPlatformInstance should have None instance when not configured."""
        config = MagicMock()
        config.platform_instance = None
        config.include_externally_managed_dmfs = True
        report = MagicMock()
        connection = MagicMock()
        identifiers = MagicMock()
        identifiers.platform = "snowflake"
        identifiers.get_dataset_identifier.return_value = "my_db.public.orders"
        identifiers.gen_dataset_urn.return_value = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.orders,PROD)"
        )
        handler = SnowflakeAssertionsHandler(config, report, connection, identifiers)

        row = {
            "MEASUREMENT_TIME": datetime.now(),
            "METRIC_NAME": "my_check",
            "TABLE_NAME": "orders",
            "TABLE_SCHEMA": "public",
            "TABLE_DATABASE": "my_db",
            "VALUE": 1,
            "REFERENCE_ID": "ref_123",
            "ARGUMENT_NAMES": "[]",
        }
        discovered = ["my_db.public.orders"]
        workunits = handler._process_result_row(row, discovered)

        # Find DataPlatformInstance workunit using type-safe filtering
        platform_instance_wus = [
            wu for wu in workunits if wu.get_aspect_of_type(DataPlatformInstance)
        ]
        assert len(platform_instance_wus) == 1

        aspect = platform_instance_wus[0].get_aspect_of_type(DataPlatformInstance)
        assert aspect is not None
        assert aspect.platform == "urn:li:dataPlatform:snowflake"
        assert aspect.instance is None

    def test_data_platform_instance_emitted_once_per_assertion(self):
        """DataPlatformInstance should only be emitted once per unique assertion."""
        config = MagicMock()
        config.platform_instance = "my_instance"
        config.include_externally_managed_dmfs = True
        report = MagicMock()
        connection = MagicMock()
        identifiers = MagicMock()
        identifiers.platform = "snowflake"
        identifiers.get_dataset_identifier.return_value = "my_db.public.orders"
        identifiers.gen_dataset_urn.return_value = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.orders,PROD)"
        )
        handler = SnowflakeAssertionsHandler(config, report, connection, identifiers)

        # Process same DMF twice (simulating multiple results for same assertion)
        row = {
            "MEASUREMENT_TIME": datetime.now(),
            "METRIC_NAME": "my_check",
            "TABLE_NAME": "orders",
            "TABLE_SCHEMA": "public",
            "TABLE_DATABASE": "my_db",
            "VALUE": 1,
            "REFERENCE_ID": "ref_123",
            "ARGUMENT_NAMES": "[]",
        }
        discovered = ["my_db.public.orders"]

        # First call
        workunits1 = handler._process_result_row(row, discovered)
        platform_instance_wus1 = [
            wu for wu in workunits1 if wu.get_aspect_of_type(DataPlatformInstance)
        ]
        assert len(platform_instance_wus1) == 1

        # Second call with same assertion
        workunits2 = handler._process_result_row(row, discovered)
        platform_instance_wus2 = [
            wu for wu in workunits2 if wu.get_aspect_of_type(DataPlatformInstance)
        ]
        # Should not emit DataPlatformInstance again
        assert len(platform_instance_wus2) == 0


class TestAssertionResultTypes:
    """Test assertion result type mapping based on VALUE."""

    @pytest.fixture
    def handler(self):
        """Create a handler with mocked dependencies."""
        config = MagicMock()
        config.platform_instance = None
        config.include_externally_managed_dmfs = True
        report = MagicMock()
        connection = MagicMock()
        identifiers = MagicMock()
        identifiers.platform = "snowflake"
        identifiers.get_dataset_identifier.return_value = "my_db.public.orders"
        identifiers.gen_dataset_urn.return_value = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.orders,PROD)"
        )
        return SnowflakeAssertionsHandler(config, report, connection, identifiers)

    def test_value_1_is_success(self, handler):
        """VALUE=1 should result in SUCCESS."""
        row = {
            "MEASUREMENT_TIME": datetime.now(),
            "METRIC_NAME": "my_check",
            "TABLE_NAME": "orders",
            "TABLE_SCHEMA": "public",
            "TABLE_DATABASE": "my_db",
            "VALUE": 1,
            "REFERENCE_ID": "ref_123",
            "ARGUMENT_NAMES": "[]",
        }
        discovered = ["my_db.public.orders"]
        workunits = handler._process_result_row(row, discovered)

        run_event_wu = [
            wu for wu in workunits if wu.metadata.aspectName == "assertionRunEvent"
        ][0]
        assert run_event_wu.metadata.aspect.result.type == AssertionResultType.SUCCESS

    def test_value_0_is_failure(self, handler):
        """VALUE=0 should result in FAILURE."""
        row = {
            "MEASUREMENT_TIME": datetime.now(),
            "METRIC_NAME": "my_check",
            "TABLE_NAME": "orders",
            "TABLE_SCHEMA": "public",
            "TABLE_DATABASE": "my_db",
            "VALUE": 0,
            "REFERENCE_ID": "ref_123",
            "ARGUMENT_NAMES": "[]",
        }
        discovered = ["my_db.public.orders"]
        workunits = handler._process_result_row(row, discovered)

        run_event_wu = [
            wu for wu in workunits if wu.metadata.aspectName == "assertionRunEvent"
        ][0]
        assert run_event_wu.metadata.aspect.result.type == AssertionResultType.FAILURE

    def test_other_values_are_error(self, handler):
        """VALUES other than 0 or 1 should result in ERROR."""
        for invalid_value in [5, 100, -1, 999]:
            # Reset handler state for each iteration
            handler._urns_processed = []

            row = {
                "MEASUREMENT_TIME": datetime.now(),
                "METRIC_NAME": f"my_check_{invalid_value}",
                "TABLE_NAME": "orders",
                "TABLE_SCHEMA": "public",
                "TABLE_DATABASE": "my_db",
                "VALUE": invalid_value,
                "REFERENCE_ID": f"ref_{invalid_value}",
                "ARGUMENT_NAMES": "[]",
            }
            discovered = ["my_db.public.orders"]
            workunits = handler._process_result_row(row, discovered)

            run_event_wu = [
                wu for wu in workunits if wu.metadata.aspectName == "assertionRunEvent"
            ][0]
            assert run_event_wu.metadata.aspect.result.type == AssertionResultType.ERROR
