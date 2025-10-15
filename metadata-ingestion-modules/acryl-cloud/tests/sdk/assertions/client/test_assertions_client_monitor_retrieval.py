"""Tests for monitor retrieval logic in AssertionsClient.

This test suite focuses on the _retrieve_assertion_and_monitor method
to ensure it correctly finds monitor entities associated with assertions.
"""

from typing import Iterable, Optional

from acryl_datahub_cloud.sdk.assertions_client import AssertionsClient
from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.entities.monitor import Monitor
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import AssertionUrn, DatasetUrn, MonitorUrn, Urn
from tests.sdk.assertions.conftest import StubDataHubClient, StubEntityClient


class MockSearchClient:
    """Mock search client that returns predefined monitor URNs."""

    def __init__(self, monitor_urns: Optional[list[MonitorUrn]] = None):
        self.monitor_urns = monitor_urns or []
        self.last_filter = None

    def get_urns(self, filter=None, **kwargs) -> Iterable[Urn]:
        """Return the predefined monitor URNs."""
        self.last_filter = filter
        return iter(self.monitor_urns)


class TestMonitorRetrieval:
    """Test cases for monitor retrieval logic."""

    def test_retrieve_monitor_via_search_with_custom_urn(self):
        """Test that monitor is found via search when it has a custom URN structure."""
        # Setup
        dataset_urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)"
        )
        assertion_urn = AssertionUrn.from_string("urn:li:assertion:test_assertion")

        # Create a monitor with a custom URN (not dataset_urn + assertion_urn)
        custom_monitor_id = "custom_monitor_12345"
        monitor_urn = MonitorUrn(entity=dataset_urn, id=custom_monitor_id)

        monitor_entity = Monitor(
            id=monitor_urn,
            info=models.MonitorInfoClass(
                type=models.MonitorTypeClass.ASSERTION,
                status=models.MonitorStatusClass(
                    mode=models.MonitorModeClass.ACTIVE,
                ),
                assertionMonitor=models.AssertionMonitorClass(
                    assertions=[
                        models.AssertionEvaluationSpecClass(
                            assertion=str(assertion_urn),
                            schedule=models.CronScheduleClass(
                                cron="0 */6 * * *",
                                timezone="UTC",
                            ),
                            parameters=models.AssertionEvaluationParametersClass(
                                type=models.AssertionEvaluationParametersTypeClass.DATASET_FRESHNESS,
                            ),
                        )
                    ],
                ),
            ),
        )

        assertion_entity = Assertion(
            id=assertion_urn,
            info=models.FreshnessAssertionInfoClass(
                type=models.FreshnessAssertionTypeClass.DATASET_CHANGE,
                entity=str(dataset_urn),
            ),
        )

        # Create entity client that returns our entities
        entity_client = StubEntityClient(
            monitor_entity=monitor_entity,
            assertion_entity=assertion_entity,
        )

        # Create mock search client that returns the custom monitor URN
        mock_search = MockSearchClient(monitor_urns=[monitor_urn])

        # Create client with mocked components
        client = StubDataHubClient(entity_client=entity_client)
        client.search = mock_search  # type: ignore

        assertions_client = AssertionsClient(client)  # type: ignore

        # Execute
        assertion_input = {
            "dataset_urn": dataset_urn,
            "urn": assertion_urn,
        }

        result_assertion, result_monitor_urn, result_monitor = (
            assertions_client._retrieve_assertion_and_monitor(assertion_input)  # type: ignore
        )

        # Assert
        assert result_assertion is not None
        assert result_assertion.urn == assertion_urn

        assert result_monitor_urn == monitor_urn
        assert result_monitor is not None
        assert result_monitor.urn == monitor_urn

        # Verify search was called with correct filter
        assert mock_search.last_filter is not None

    def test_retrieve_monitor_fallback_when_search_returns_empty(self):
        """Test that we fall back to old logic when search returns no results."""
        # Setup
        dataset_urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)"
        )
        assertion_urn = AssertionUrn.from_string("urn:li:assertion:test_assertion")

        assertion_entity = Assertion(
            id=assertion_urn,
            info=models.FreshnessAssertionInfoClass(
                type=models.FreshnessAssertionTypeClass.DATASET_CHANGE,
                entity=str(dataset_urn),
            ),
        )

        # Create entity client with only assertion (no monitor)
        entity_client = StubEntityClient(
            monitor_entity=None,
            assertion_entity=assertion_entity,
        )

        # Create mock search client that returns empty results
        mock_search = MockSearchClient(monitor_urns=[])

        # Create client
        client = StubDataHubClient(entity_client=entity_client)
        client.search = mock_search  # type: ignore

        assertions_client = AssertionsClient(client)  # type: ignore

        # Execute
        assertion_input = {
            "dataset_urn": dataset_urn,
            "urn": assertion_urn,
        }

        result_assertion, result_monitor_urn, result_monitor = (
            assertions_client._retrieve_assertion_and_monitor(assertion_input)  # type: ignore
        )

        # Assert
        assert result_assertion is not None
        assert result_assertion.urn == assertion_urn

        # Monitor URN should be constructed using fallback logic
        expected_fallback_urn = Monitor._ensure_id(id=(dataset_urn, assertion_urn))
        assert result_monitor_urn == expected_fallback_urn

        # Monitor entity should be None since it wasn't found
        assert result_monitor is None

    def test_retrieve_monitor_handles_item_not_found_during_get(self):
        """Test that ItemNotFoundError during monitor get is handled gracefully."""
        # Setup
        dataset_urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)"
        )
        assertion_urn = AssertionUrn.from_string("urn:li:assertion:test_assertion")

        # Create a monitor URN that will be found via search but not via get
        monitor_urn = MonitorUrn(entity=dataset_urn, id="custom_monitor")

        assertion_entity = Assertion(
            id=assertion_urn,
            info=models.FreshnessAssertionInfoClass(
                type=models.FreshnessAssertionTypeClass.DATASET_CHANGE,
                entity=str(dataset_urn),
            ),
        )

        # Create entity client that throws ItemNotFoundError for monitor get
        entity_client = StubEntityClient(
            monitor_entity=None,  # Monitor entity will not be found
            assertion_entity=assertion_entity,
        )

        # Create mock search client that returns a monitor URN
        mock_search = MockSearchClient(monitor_urns=[monitor_urn])

        # Create client
        client = StubDataHubClient(entity_client=entity_client)
        client.search = mock_search  # type: ignore

        assertions_client = AssertionsClient(client)  # type: ignore

        # Execute - should not raise exception
        assertion_input = {
            "dataset_urn": dataset_urn,
            "urn": assertion_urn,
        }

        result_assertion, result_monitor_urn, result_monitor = (
            assertions_client._retrieve_assertion_and_monitor(assertion_input)  # type: ignore
        )

        # Assert - monitor URN should be from search, but entity is None
        assert result_assertion is not None
        assert result_monitor_urn == monitor_urn
        assert result_monitor is None  # Entity not found

    def test_retrieve_monitor_with_multiple_search_results_uses_first(self):
        """Test that when search returns multiple monitors, we use the first one."""
        # Setup
        dataset_urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)"
        )
        assertion_urn = AssertionUrn.from_string("urn:li:assertion:test_assertion")

        # Create multiple monitor URNs
        monitor_urn_1 = MonitorUrn(entity=dataset_urn, id="monitor_1")
        monitor_urn_2 = MonitorUrn(entity=dataset_urn, id="monitor_2")

        monitor_entity_1 = Monitor(
            id=monitor_urn_1,
            info=models.MonitorInfoClass(
                type=models.MonitorTypeClass.ASSERTION,
                status=models.MonitorStatusClass(
                    mode=models.MonitorModeClass.ACTIVE,
                ),
            ),
        )

        assertion_entity = Assertion(
            id=assertion_urn,
            info=models.FreshnessAssertionInfoClass(
                type=models.FreshnessAssertionTypeClass.DATASET_CHANGE,
                entity=str(dataset_urn),
            ),
        )

        # Create entity client that returns the first monitor
        entity_client = StubEntityClient(
            monitor_entity=monitor_entity_1,
            assertion_entity=assertion_entity,
        )

        # Create mock search client that returns both monitor URNs
        mock_search = MockSearchClient(monitor_urns=[monitor_urn_1, monitor_urn_2])

        # Create client
        client = StubDataHubClient(entity_client=entity_client)
        client.search = mock_search  # type: ignore

        assertions_client = AssertionsClient(client)  # type: ignore

        # Execute
        assertion_input = {
            "dataset_urn": dataset_urn,
            "urn": assertion_urn,
        }

        result_assertion, result_monitor_urn, result_monitor = (
            assertions_client._retrieve_assertion_and_monitor(assertion_input)  # type: ignore
        )

        # Assert - should use the first monitor
        assert result_monitor_urn == monitor_urn_1
        assert result_monitor is not None
        assert result_monitor.urn == monitor_urn_1

    def test_retrieve_monitor_when_assertion_not_found(self):
        """Test monitor retrieval when assertion entity doesn't exist."""
        # Setup
        dataset_urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)"
        )
        assertion_urn = AssertionUrn.from_string("urn:li:assertion:nonexistent")

        monitor_urn = MonitorUrn(entity=dataset_urn, id="monitor_id")
        monitor_entity = Monitor(
            id=monitor_urn,
            info=models.MonitorInfoClass(
                type=models.MonitorTypeClass.ASSERTION,
                status=models.MonitorStatusClass(
                    mode=models.MonitorModeClass.ACTIVE,
                ),
            ),
        )

        # Create entity client with only monitor (no assertion)
        entity_client = StubEntityClient(
            monitor_entity=monitor_entity,
            assertion_entity=None,  # Assertion not found
        )

        # Create mock search client
        mock_search = MockSearchClient(monitor_urns=[monitor_urn])

        # Create client
        client = StubDataHubClient(entity_client=entity_client)
        client.search = mock_search  # type: ignore

        assertions_client = AssertionsClient(client)  # type: ignore

        # Execute
        assertion_input = {
            "dataset_urn": dataset_urn,
            "urn": assertion_urn,
        }

        result_assertion, result_monitor_urn, result_monitor = (
            assertions_client._retrieve_assertion_and_monitor(assertion_input)  # type: ignore
        )

        # Assert
        assert result_assertion is None  # Assertion not found
        assert result_monitor_urn == monitor_urn  # But monitor was found
        assert result_monitor is not None
        assert result_monitor.urn == monitor_urn

    def test_search_filter_includes_correct_parameters(self):
        """Test that the search filter is constructed with correct parameters."""
        # Setup
        dataset_urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)"
        )
        assertion_urn = AssertionUrn.from_string("urn:li:assertion:test_assertion")

        assertion_entity = Assertion(
            id=assertion_urn,
            info=models.FreshnessAssertionInfoClass(
                type=models.FreshnessAssertionTypeClass.DATASET_CHANGE,
                entity=str(dataset_urn),
            ),
        )

        entity_client = StubEntityClient(assertion_entity=assertion_entity)
        mock_search = MockSearchClient(monitor_urns=[])

        client = StubDataHubClient(entity_client=entity_client)
        client.search = mock_search  # type: ignore

        assertions_client = AssertionsClient(client)  # type: ignore

        # Execute
        assertion_input = {
            "dataset_urn": dataset_urn,
            "urn": assertion_urn,
        }

        assertions_client._retrieve_assertion_and_monitor(assertion_input)  # type: ignore

        # Assert - check that filter was created
        assert mock_search.last_filter is not None

        # We can't easily inspect the filter internals without more complex mocking,
        # but we can verify it was called with a filter object
        # The filter should contain entity_type="monitor" and assertionUrn field

    def test_retrieve_with_urn_objects_not_strings(self):
        """Test that the method works with Urn objects (not just strings)."""
        # Setup
        dataset_urn = DatasetUrn.from_string(
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.table,PROD)"
        )
        assertion_urn = AssertionUrn.from_string("urn:li:assertion:test_assertion")

        monitor_urn = MonitorUrn(entity=dataset_urn, id="custom_monitor")

        monitor_entity = Monitor(
            id=monitor_urn,
            info=models.MonitorInfoClass(
                type=models.MonitorTypeClass.ASSERTION,
                status=models.MonitorStatusClass(
                    mode=models.MonitorModeClass.ACTIVE,
                ),
            ),
        )

        assertion_entity = Assertion(
            id=assertion_urn,
            info=models.FreshnessAssertionInfoClass(
                type=models.FreshnessAssertionTypeClass.DATASET_CHANGE,
                entity=str(dataset_urn),
            ),
        )

        entity_client = StubEntityClient(
            monitor_entity=monitor_entity,
            assertion_entity=assertion_entity,
        )

        mock_search = MockSearchClient(monitor_urns=[monitor_urn])

        client = StubDataHubClient(entity_client=entity_client)
        client.search = mock_search  # type: ignore

        assertions_client = AssertionsClient(client)  # type: ignore

        # Execute - pass URN objects instead of strings
        assertion_input = {
            "dataset_urn": dataset_urn,  # DatasetUrn object
            "urn": assertion_urn,  # AssertionUrn object
        }

        result_assertion, result_monitor_urn, result_monitor = (
            assertions_client._retrieve_assertion_and_monitor(assertion_input)  # type: ignore
        )

        # Assert
        assert result_assertion is not None
        assert result_monitor_urn == monitor_urn
        assert result_monitor is not None
