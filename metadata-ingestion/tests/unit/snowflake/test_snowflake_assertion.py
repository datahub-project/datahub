from datetime import datetime
from typing import Dict, List
from unittest.mock import MagicMock

import pytest

from datahub.emitter.mce_builder import SYSTEM_ACTOR, make_assertion_urn
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_assertion import (
    DataQualityMonitoringResult,
    SnowflakeAssertionsHandler,
)
from datahub.ingestion.source.snowflake.snowflake_query import SnowflakeQuery
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    auto_stale_entity_removal,
)
from datahub.metadata.com.linkedin.pegasus2avro.assertion import AssertionResultType
from datahub.metadata.com.linkedin.pegasus2avro.common import DataPlatformInstance
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionSourceTypeClass,
    AssertionTypeClass,
    StatusClass,
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
        guid1 = handler._generate_external_dmf_guid(result.REFERENCE_ID)
        guid2 = handler._generate_external_dmf_guid(result.REFERENCE_ID)
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
        guid1 = handler._generate_external_dmf_guid(result1.REFERENCE_ID)
        guid2 = handler._generate_external_dmf_guid(result2.REFERENCE_ID)
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

        guid_with = handler_with._generate_external_dmf_guid(result.REFERENCE_ID)
        guid_without = handler_without._generate_external_dmf_guid(result.REFERENCE_ID)
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
        """External DMFs should use CUSTOM type and EXTERNAL source with created timestamp."""
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
        assert assertion_info.source.created is not None
        assert assertion_info.source.created.actor == SYSTEM_ACTOR
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


class TestExternalDmfStatefulIngestion:
    """External DMF definitions come from the association listing and are
    connector-owned (primary), so stateful ingestion can soft-delete DMFs that
    were removed in Snowflake without touching DMFs that simply did not run."""

    DATASET_URN = (
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,my_db.public.orders,PROD)"
    )

    def _handler(self, references, results, stale_removal_enabled=True):
        config = MagicMock()
        config.platform_instance = None
        config.include_externally_managed_dmfs = True
        config.stateful_ingestion.enabled = stale_removal_enabled
        config.stateful_ingestion.remove_stale_metadata = stale_removal_enabled
        report = MagicMock()
        connection = MagicMock()

        def query(sql):
            if "DATA_METRIC_FUNCTION_REFERENCES" in sql:
                if isinstance(references, Exception):
                    raise references
                return iter(references)
            return iter(results)

        connection.query.side_effect = query
        identifiers = MagicMock()
        identifiers.platform = "snowflake"
        identifiers.get_dataset_identifier.return_value = "my_db.public.orders"
        identifiers.gen_dataset_urn.return_value = self.DATASET_URN
        return SnowflakeAssertionsHandler(config, report, connection, identifiers)

    @staticmethod
    def _reference(
        metric_name, ref_id, args='[{"domain": "COLUMN", "name": "amount"}]'
    ):
        return {
            "METRIC_NAME": metric_name,
            "REF_DATABASE_NAME": "my_db",
            "REF_SCHEMA_NAME": "public",
            "REF_ENTITY_NAME": "orders",
            "REF_ID": ref_id,
            "REF_ARGUMENTS": args,
        }

    @staticmethod
    def _result(metric_name, ref_id):
        return {
            "MEASUREMENT_TIME": datetime.now(),
            "METRIC_NAME": metric_name,
            "TABLE_NAME": "orders",
            "TABLE_SCHEMA": "public",
            "TABLE_DATABASE": "my_db",
            "VALUE": 1,
            "REFERENCE_ID": ref_id,
            "ARGUMENT_NAMES": '["amount"]',
        }

    def test_external_dmf_definition_is_primary_and_run_event_is_not(self):
        handler = self._handler(
            references=[self._reference("my_check", "ref_1")],
            results=[self._result("my_check", "ref_1")],
        )
        wus = list(handler.get_assertion_workunits(["my_db.public.orders"]))

        by_aspect: Dict[str, List[MetadataWorkUnit]] = {}
        for wu in wus:
            by_aspect.setdefault(wu.metadata.aspectName, []).append(wu)
        # Definition is emitted once (from the listing), not again from results.
        assert len(by_aspect["assertionInfo"]) == 1
        assert by_aspect["assertionInfo"][0].is_primary_source
        assert by_aspect["dataPlatformInstance"][0].is_primary_source
        assert by_aspect["status"][0].is_primary_source
        status = by_aspect["status"][0].get_aspect_of_type(StatusClass)
        assert status is not None and status.removed is False
        assert not by_aspect["assertionRunEvent"][0].is_primary_source
        info = by_aspect["assertionInfo"][0].get_aspect_of_type(AssertionInfoClass)
        assert info is not None and info.customAssertion is not None
        assert info.customAssertion.field is not None
        assert "amount" in info.customAssertion.field
        # Listing and results resolve to the same assertion URN.
        assert (
            by_aspect["assertionInfo"][0].get_urn()
            == by_aspect["assertionRunEvent"][0].get_urn()
        )

    def test_dmf_without_results_in_window_stays_in_state(self):
        """A DMF that still exists but did not run in the window must not be
        considered stale."""
        handler = self._handler(
            references=[self._reference("daily_check", "ref_daily")],
            results=[],
        )
        stale_handler = MagicMock()
        list(
            auto_stale_entity_removal(
                stale_handler,
                handler.get_assertion_workunits(["my_db.public.orders"]),
            )
        )
        state_urns = {
            c.args[1] for c in stale_handler.add_entity_to_state.call_args_list
        }
        assert state_urns == {
            make_assertion_urn(handler._generate_external_dmf_guid("ref_daily"))
        }
        stale_handler.add_urn_to_skip.assert_not_called()

    def test_datahub_compiled_dmfs_are_never_primary(self):
        handler = self._handler(
            references=[self._reference("datahub__abc123", "ref_dh")],
            results=[self._result("datahub__abc123", "ref_dh")],
        )
        wus = list(handler.get_assertion_workunits(["my_db.public.orders"]))
        assert wus
        assert all(not wu.is_primary_source for wu in wus)
        assert "assertionInfo" not in {wu.metadata.aspectName for wu in wus}

    def test_listing_skips_undiscovered_tables(self):
        handler = self._handler(
            references=[self._reference("my_check", "ref_1")], results=[]
        )
        assert list(handler.get_assertion_workunits(["other_db.public.t"])) == []

    def test_listing_failure_reports_failure_when_stale_removal_enabled(self):
        """A failed listing must block stale removal (via a reported failure);
        otherwise every tracked DMF would look deleted."""
        handler = self._handler(
            references=Exception("insufficient privileges"),
            results=[self._result("my_check", "ref_1")],
        )
        wus = list(handler.get_assertion_workunits(["my_db.public.orders"]))
        handler.report.failure.assert_called_once()
        # Falls back to result-derived, non-primary definitions.
        assert "assertionInfo" in {wu.metadata.aspectName for wu in wus}
        assert all(not wu.is_primary_source for wu in wus)

    def test_listing_failure_only_warns_without_stale_removal(self):
        handler = self._handler(
            references=Exception("insufficient privileges"),
            results=[],
            stale_removal_enabled=False,
        )
        list(handler.get_assertion_workunits(["my_db.public.orders"]))
        handler.report.failure.assert_not_called()
        handler.report.warning.assert_called_once()

    def test_references_not_queried_without_external_dmfs(self):
        handler = self._handler(references=[], results=[])
        handler.config.include_externally_managed_dmfs = False
        list(handler.get_assertion_workunits(["my_db.public.orders"]))
        queries = [c.args[0] for c in handler.connection.query.call_args_list]
        assert not any("DATA_METRIC_FUNCTION_REFERENCES" in q for q in queries)
