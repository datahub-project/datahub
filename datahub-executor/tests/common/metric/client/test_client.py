from datetime import datetime, timedelta, timezone
from typing import List
from unittest.mock import MagicMock, patch

import pytest
from datahub.configuration.common import OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import (
    DataHubMetricCubeEventClass,
    DatasetProfileClass,
    OperationClass,
    OperationTypeClass,
    SystemMetadataClass,
)

from datahub_executor.common.metric.client.client import MetricClient
from datahub_executor.common.metric.types import (
    Metric,
    Operation,
)


class TestMetricClient:
    """Tests for the MetricClient class."""

    @pytest.fixture
    def mock_graph(self) -> MagicMock:
        """Create a mock DataHubGraph instance."""
        return MagicMock(spec=DataHubGraph)

    @pytest.fixture
    def metric_client(self, mock_graph: MagicMock) -> MetricClient:
        """Create a MetricClient instance with a mock graph."""
        return MetricClient(mock_graph)

    @pytest.fixture
    def metric_urn(self) -> str:
        """Sample metric URN for testing."""
        return "urn:li:dataHubMetricCube:(urn:li:dataset:(urn:li:dataPlatform:hive,test.table,PROD),rowCount)"

    @pytest.fixture
    def dataset_urn(self) -> str:
        """Sample dataset URN for testing."""
        return "urn:li:dataset:(urn:li:dataPlatform:hive,test.table,PROD)"

    @pytest.fixture
    def sample_metrics(self) -> List[Metric]:
        """Create sample metrics data for testing."""
        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        return [
            Metric(timestamp_ms=now - 3600000, value=100.0),  # 1 hour ago
            Metric(timestamp_ms=now - 7200000, value=200.0),  # 2 hours ago
            Metric(timestamp_ms=now - 10800000, value=300.0),  # 3 hours ago
        ]

    @pytest.fixture
    def sample_operations(self) -> List[Operation]:
        """Create sample operations data for testing."""
        now = int(datetime.now(timezone.utc).timestamp() * 1000)
        return [
            Operation(timestamp_ms=now - 3600000, type="INSERT"),  # 1 hour ago
            Operation(timestamp_ms=now - 7200000, type="UPDATE"),  # 2 hours ago
            Operation(timestamp_ms=now - 10800000, type="DELETE"),  # 3 hours ago
        ]

    def test_init(self, mock_graph: MagicMock) -> None:
        """Test initialization of MetricClient."""
        client = MetricClient(mock_graph)
        assert client.graph == mock_graph

    def test_save_metric_value(
        self, metric_client: MetricClient, metric_urn: str, mock_graph: MagicMock
    ) -> None:
        """Test saving a metric value."""
        # Setup
        test_metric = Metric(timestamp_ms=1640995200000, value=150.0)  # 2022-01-01

        # Test
        with patch("time.time", return_value=1640995200):  # 2022-01-01
            metric_client.save_metric_value(metric_urn, test_metric)

        # Verify that emit_mcp was called once
        mock_graph.emit_mcps.assert_called_once()

        # Verify the MCP structure
        mcp_arg = mock_graph.emit_mcps.call_args[0][0][0]
        assert isinstance(mcp_arg, MetadataChangeProposalWrapper)
        assert mcp_arg.entityUrn == metric_urn

        # Verify aspect is DataHubMetricCubeEventClass with correct values
        assert isinstance(mcp_arg.aspect, DataHubMetricCubeEventClass)
        assert mcp_arg.aspect.timestampMillis == 1640995200000
        assert mcp_arg.aspect.measure == 150.0
        assert mcp_arg.aspect.reportedTimeMillis == 1640995200000

        # Verify system metadata
        assert isinstance(mcp_arg.systemMetadata, SystemMetadataClass)
        assert mcp_arg.systemMetadata.lastObserved == 1640995200000

    def test_save_metric_values(
        self, metric_client: MetricClient, metric_urn: str, mock_graph: MagicMock
    ) -> None:
        """Test saving multiple metric values."""
        # Setup
        test_metrics = [
            Metric(timestamp_ms=1640995200000, value=150.0),  # 2022-01-01
            Metric(timestamp_ms=1641081600000, value=200.0),  # 2022-01-02
            Metric(timestamp_ms=1641168000000, value=250.0),  # 2022-01-03
        ]

        # Test
        with patch("time.time", return_value=1641168000):  # 2022-01-03
            metric_client.save_metric_values(metric_urn, test_metrics)

        # Verify that emit_mcps was called once
        mock_graph.emit_mcps.assert_called_once()

        # Verify the MCPs structure
        mcps = mock_graph.emit_mcps.call_args[0][0]
        assert len(mcps) == 3

        # Verify each MCP
        for i, mcp in enumerate(mcps):
            assert isinstance(mcp, MetadataChangeProposalWrapper)
            assert mcp.entityUrn == metric_urn

            # Verify aspect is DataHubMetricCubeEventClass with correct values
            assert isinstance(mcp.aspect, DataHubMetricCubeEventClass)
            assert mcp.aspect.timestampMillis == test_metrics[i].timestamp_ms
            assert mcp.aspect.measure == test_metrics[i].value
            assert mcp.aspect.reportedTimeMillis == 1641168000000

            # Verify system metadata
            assert isinstance(mcp.systemMetadata, SystemMetadataClass)
            assert mcp.systemMetadata.lastObserved == 1641168000000
            assert (
                mcp.systemMetadata.runId is not None
                and mcp.systemMetadata.runId.startswith("save-metric-")
            )

    def test_fetch_metric_values(
        self,
        metric_client: MetricClient,
        metric_urn: str,
        mock_graph: MagicMock,
        sample_metrics: List[Metric],
    ) -> None:
        """Test fetching metric values with lookback."""
        # Setup mock response
        mock_metric_events = [
            DataHubMetricCubeEventClass(
                timestampMillis=metric.timestamp_ms,
                measure=metric.value,
                reportedTimeMillis=metric.timestamp_ms,
            )
            for metric in sample_metrics
        ]
        mock_graph.get_timeseries_values.return_value = mock_metric_events

        # Test
        with patch("datetime.datetime", wraps=datetime) as mock_datetime:
            # Mock now() to return a fixed time
            fixed_now = datetime(2022, 1, 1, tzinfo=timezone.utc)
            mock_datetime.now.return_value = fixed_now

            result = metric_client.fetch_metric_values(
                metric_urn, lookback=timedelta(days=7)
            )

        # Verify the result
        assert len(result) == len(sample_metrics)
        for i, metric in enumerate(result):
            assert metric.timestamp_ms == sample_metrics[i].timestamp_ms
            assert metric.value == sample_metrics[i].value

        # Verify that get_timeseries_values was called with correct params
        mock_graph.get_timeseries_values.assert_called_once()
        call_args = mock_graph.get_timeseries_values.call_args[1]
        assert call_args["entity_urn"] == metric_urn
        assert call_args["aspect_type"] == DataHubMetricCubeEventClass
        assert call_args["limit"] == 200  # Default limit

    def test_fetch_metric_values_by_time(
        self,
        metric_client: MetricClient,
        metric_urn: str,
        mock_graph: MagicMock,
        sample_metrics: List[Metric],
    ) -> None:
        """Test fetching metric values by specific time range."""
        # Setup mock response
        mock_metric_events = [
            DataHubMetricCubeEventClass(
                timestampMillis=metric.timestamp_ms,
                measure=metric.value,
                reportedTimeMillis=metric.timestamp_ms,
            )
            for metric in sample_metrics
        ]
        mock_graph.get_timeseries_values.return_value = mock_metric_events

        # Test with specific time range
        start_time = datetime(2022, 1, 1, tzinfo=timezone.utc)
        end_time = datetime(2022, 1, 7, tzinfo=timezone.utc)
        result = metric_client.fetch_metric_values_by_time(
            metric_urn, start_time=start_time, end_time=end_time, limit=100
        )

        # Verify the result
        assert len(result) == len(sample_metrics)
        for i, metric in enumerate(result):
            assert metric.timestamp_ms == sample_metrics[i].timestamp_ms
            assert metric.value == sample_metrics[i].value

        # Verify that get_timeseries_values was called with correct params
        mock_graph.get_timeseries_values.assert_called_once()
        call_args = mock_graph.get_timeseries_values.call_args[1]
        assert call_args["entity_urn"] == metric_urn
        assert call_args["aspect_type"] == DataHubMetricCubeEventClass
        assert call_args["limit"] == 100

        # Verify time filter
        time_filter = call_args["filter"]["or"][0]["and"]
        assert time_filter[0]["field"] == "timestampMillis"
        assert time_filter[0]["condition"] == "GREATER_THAN_OR_EQUAL_TO"
        assert time_filter[0]["value"] == str(int(start_time.timestamp() * 1000))
        assert time_filter[1]["field"] == "timestampMillis"
        assert time_filter[1]["condition"] == "LESS_THAN_OR_EQUAL_TO"
        assert time_filter[1]["value"] == str(int(end_time.timestamp() * 1000))

    def test_fetch_row_count_metric_values_from_metric_cube(
        self,
        metric_client: MetricClient,
        metric_urn: str,
        mock_graph: MagicMock,
        sample_metrics: List[Metric],
    ) -> None:
        """Test fetching row count metrics from metric cube."""
        # Setup mock response for metric cube
        mock_metric_events = [
            DataHubMetricCubeEventClass(
                timestampMillis=metric.timestamp_ms,
                measure=metric.value,
                reportedTimeMillis=metric.timestamp_ms,
            )
            for metric in sample_metrics
        ]
        mock_graph.get_timeseries_values.return_value = mock_metric_events

        # Test
        start_time = datetime(2022, 1, 1, tzinfo=timezone.utc)
        end_time = datetime(2022, 1, 7, tzinfo=timezone.utc)
        result = metric_client.fetch_row_count_metric_values(
            metric_urn=metric_urn,
            dataset_urn=None,
            start_time=start_time,
            end_time=end_time,
            limit=100,
            dataset_profiles_fallback=False,
        )

        # Verify the result
        assert len(result) == len(sample_metrics)
        for i, metric in enumerate(result):
            assert metric.timestamp_ms == sample_metrics[i].timestamp_ms
            assert metric.value == sample_metrics[i].value

        # Verify that get_timeseries_values was called once (no fallback needed)
        assert mock_graph.get_timeseries_values.call_count == 1

    def test_fetch_row_count_metric_values_with_fallback(
        self, metric_client: MetricClient, dataset_urn: str, mock_graph: MagicMock
    ) -> None:
        """Test fetching row count metrics with fallback to dataset profiles."""
        # Setup - First call raises error, second call (for dataset profiles) returns data
        mock_graph.get_timeseries_values.side_effect = [
            OperationalError(
                info={"message": "Failed to find entity with name dataHubMetricCube"},
                message="Failed to find entity with name dataHubMetricCube",
            ),
            [
                DatasetProfileClass(timestampMillis=1640995200000, rowCount=100),
                DatasetProfileClass(
                    timestampMillis=1640908800000,  # One day earlier
                    rowCount=90,
                ),
            ],
        ]

        # Test
        start_time = datetime(2022, 1, 1, tzinfo=timezone.utc)
        end_time = datetime(2022, 1, 7, tzinfo=timezone.utc)
        result = metric_client.fetch_row_count_metric_values(
            metric_urn="invalid_metric_urn",
            dataset_urn=dataset_urn,
            start_time=start_time,
            end_time=end_time,
            limit=100,
            dataset_profiles_fallback=True,
        )

        # Verify the result - should have metrics from dataset profiles
        assert len(result) == 2
        assert result[0].timestamp_ms == 1640908800000
        assert result[0].value == 90
        assert result[1].timestamp_ms == 1640995200000
        assert result[1].value == 100

        # Verify get_timeseries_values was called twice (once for cube, once for profiles)
        assert mock_graph.get_timeseries_values.call_count == 2

        # Second call should be for dataset profiles
        second_call_args = mock_graph.get_timeseries_values.call_args_list[1][1]
        assert second_call_args["aspect_type"] == DatasetProfileClass

    def test_fetch_row_count_metric_values_partial_fallback(
        self,
        metric_client: MetricClient,
        metric_urn: str,
        dataset_urn: str,
        mock_graph: MagicMock,
    ) -> None:
        """Test fetching row counts with partial results from metric cube and fallback to profiles."""
        # Setup - First call returns some data, second call (for dataset profiles) returns more
        metric_cube_data = [
            DataHubMetricCubeEventClass(
                timestampMillis=1641081600000,  # 2022-01-02
                measure=110,
                reportedTimeMillis=1641081600000,
            )
        ]

        profile_data = [
            DatasetProfileClass(
                timestampMillis=1640995200000,  # 2022-01-01
                rowCount=100,
            )
        ]

        mock_graph.get_timeseries_values.side_effect = [metric_cube_data, profile_data]

        # Test
        start_time = datetime(2022, 1, 1, tzinfo=timezone.utc)
        end_time = datetime(2022, 1, 3, tzinfo=timezone.utc)
        result = metric_client.fetch_row_count_metric_values(
            metric_urn=metric_urn,
            dataset_urn=dataset_urn,
            start_time=start_time,
            end_time=end_time,
            limit=100,
            dataset_profiles_fallback=True,
        )

        # Verify the result - should combine results from both sources
        assert len(result) == 2
        # Results should be sorted by timestamp
        assert result[0].timestamp_ms == 1640995200000  # From profile
        assert result[0].value == 100
        assert result[1].timestamp_ms == 1641081600000  # From metric cube
        assert result[1].value == 110

        # Verify get_timeseries_values was called twice
        assert mock_graph.get_timeseries_values.call_count == 2

    def test_fetch_row_count_metric_values_operation_error(
        self, metric_client: MetricClient, dataset_urn: str, mock_graph: MagicMock
    ) -> None:
        """Test fetching row counts with an operational error that's not the fallback case."""
        # Setup - Call raises unhandled error
        mock_graph.get_timeseries_values.side_effect = OperationalError(
            info={"message": "Different error not related to dataHubMetricCube"},
            message="Different error not related to dataHubMetricCube",
        )

        # Test - Should raise the error
        start_time = datetime(2022, 1, 1, tzinfo=timezone.utc)
        end_time = datetime(2022, 1, 7, tzinfo=timezone.utc)

        with pytest.raises(OperationalError) as excinfo:
            metric_client.fetch_row_count_metric_values(
                metric_urn="invalid_metric_urn",
                dataset_urn=dataset_urn,
                start_time=start_time,
                end_time=end_time,
                limit=100,
                dataset_profiles_fallback=False,
            )

        # Verify the error message
        assert "Different error not related to dataHubMetricCube" in str(
            excinfo.value.info
        )

    def test_fetch_row_counts_from_dataset_profile(
        self, metric_client: MetricClient, dataset_urn: str, mock_graph: MagicMock
    ) -> None:
        """Test fetching row counts directly from dataset profiles."""
        # Setup
        profile_data = [
            DatasetProfileClass(
                timestampMillis=1640995200000,  # 2022-01-01
                rowCount=100,
            ),
            DatasetProfileClass(
                timestampMillis=1640908800000,  # 2021-12-31
                rowCount=90,
            ),
            DatasetProfileClass(
                timestampMillis=1640822400000,  # 2021-12-30
                # No rowCount field to test filtering
            ),
        ]

        mock_graph.get_timeseries_values.return_value = profile_data

        # Test
        start_time = datetime(2021, 12, 30, tzinfo=timezone.utc)
        end_time = datetime(2022, 1, 1, tzinfo=timezone.utc)
        result = metric_client.fetch_row_counts_from_dataset_profile(
            dataset_urn=dataset_urn,
            start_time=start_time,
            end_time=end_time,
            limit=100,
        )

        # Verify result - should only include profiles with rowCount
        assert len(result) == 2
        assert result[0].timestamp_ms == 1640995200000
        assert result[0].value == 100
        assert result[1].timestamp_ms == 1640908800000
        assert result[1].value == 90

        # Verify get_timeseries_values call
        mock_graph.get_timeseries_values.assert_called_once()
        call_args = mock_graph.get_timeseries_values.call_args[1]
        assert call_args["entity_urn"] == dataset_urn
        assert call_args["aspect_type"] == DatasetProfileClass

    def test_fetch_operations(
        self,
        metric_client: MetricClient,
        dataset_urn: str,
        mock_graph: MagicMock,
        sample_operations: List[Operation],
    ) -> None:
        """Test fetching operations with lookback."""
        # Setup mock response for operations
        mock_operation_aspects = [
            OperationClass(
                timestampMillis=op.timestamp_ms,
                operationType=(
                    OperationTypeClass.CUSTOM
                    if op.type not in ["INSERT", "UPDATE", "DELETE"]
                    else getattr(OperationTypeClass, op.type)
                ),
                customOperationType=(
                    op.type if op.type not in ["INSERT", "UPDATE", "DELETE"] else None
                ),
                lastUpdatedTimestamp=0,
            )
            for op in sample_operations
        ]
        mock_graph.get_timeseries_values.return_value = mock_operation_aspects

        # Test
        with patch("datetime.datetime", wraps=datetime) as mock_datetime:
            # Mock now() to return a fixed time
            fixed_now = datetime(2022, 1, 1, tzinfo=timezone.utc)
            mock_datetime.now.return_value = fixed_now

            result = metric_client.fetch_operations(
                dataset_urn, lookback=timedelta(days=7), ignore_types=["UNKNOWN"]
            )

        # Verify the result
        assert len(result) == len(sample_operations)
        for i, op in enumerate(result):
            assert op.timestamp_ms == sample_operations[i].timestamp_ms
            assert op.type == sample_operations[i].type

        # Verify that get_timeseries_values was called with correct params
        mock_graph.get_timeseries_values.assert_called_once()
        call_args = mock_graph.get_timeseries_values.call_args[1]
        assert call_args["entity_urn"] == dataset_urn
        assert call_args["aspect_type"] == OperationClass

        # Verify filter includes ignore_types
        filter_conditions = call_args["filter"]["or"][0]["and"]
        assert any(
            cond.get("field") == "operationType"
            and cond.get("values") == ["UNKNOWN"]
            and cond.get("negated") is True
            for cond in filter_conditions
        )

    def test_fetch_operations_by_time(
        self,
        metric_client: MetricClient,
        dataset_urn: str,
        mock_graph: MagicMock,
        sample_operations: List[Operation],
    ) -> None:
        """Test fetching operations by specific time range."""
        # Setup mock response
        mock_operation_aspects = [
            OperationClass(
                timestampMillis=op.timestamp_ms,
                operationType=(
                    OperationTypeClass.CUSTOM
                    if op.type not in ["INSERT", "UPDATE", "DELETE"]
                    else getattr(OperationTypeClass, op.type)
                ),
                customOperationType=(
                    op.type if op.type not in ["INSERT", "UPDATE", "DELETE"] else None
                ),
                lastUpdatedTimestamp=0,
            )
            for op in sample_operations
        ]
        mock_graph.get_timeseries_values.return_value = mock_operation_aspects

        # Test
        start_time = datetime(2022, 1, 1, tzinfo=timezone.utc)
        end_time = datetime(2022, 1, 7, tzinfo=timezone.utc)
        result = metric_client.fetch_operations_by_time(
            dataset_urn,
            start_time=start_time,
            end_time=end_time,
            limit=25,
            ignore_types=["MERGE", "TRUNCATE"],
        )

        # Verify the result
        assert len(result) == len(sample_operations)
        for i, op in enumerate(result):
            assert op.timestamp_ms == sample_operations[i].timestamp_ms
            assert op.type == sample_operations[i].type

        # Verify that get_timeseries_values was called with correct params
        mock_graph.get_timeseries_values.assert_called_once()
        call_args = mock_graph.get_timeseries_values.call_args[1]
        assert call_args["entity_urn"] == dataset_urn
        assert call_args["aspect_type"] == OperationClass
        assert call_args["limit"] == 25

        # Verify filter structure
        filter_conditions = call_args["filter"]["or"][0]["and"]

        # Time range filters
        assert any(
            cond.get("field") == "timestampMillis"
            and cond.get("condition") == "GREATER_THAN_OR_EQUAL_TO"
            and cond.get("value") == str(int(start_time.timestamp() * 1000))
            for cond in filter_conditions
        )
        assert any(
            cond.get("field") == "timestampMillis"
            and cond.get("condition") == "LESS_THAN_OR_EQUAL_TO"
            and cond.get("value") == str(int(end_time.timestamp() * 1000))
            for cond in filter_conditions
        )

        # Ignore types filters
        assert any(
            cond.get("field") == "operationType"
            and cond.get("values") == ["MERGE", "TRUNCATE"]
            and cond.get("negated") is True
            for cond in filter_conditions
        )
        assert any(
            cond.get("field") == "customOperationType"
            and cond.get("values") == ["MERGE", "TRUNCATE"]
            and cond.get("negated") is True
            for cond in filter_conditions
        )

    def test_build_timeseries_filter(self) -> None:
        """Test the _build_timeseries_filter method."""
        start_time = datetime(2022, 1, 1, tzinfo=timezone.utc)
        end_time = datetime(2022, 1, 7, tzinfo=timezone.utc)

        filter_conditions = MetricClient._build_timeseries_filter(start_time, end_time)

        # Verify filter structure
        assert len(filter_conditions) == 2
        assert filter_conditions[0]["field"] == "timestampMillis"
        assert filter_conditions[0]["condition"] == "GREATER_THAN_OR_EQUAL_TO"
        assert filter_conditions[0]["value"] == str(int(start_time.timestamp() * 1000))

        assert filter_conditions[1]["field"] == "timestampMillis"
        assert filter_conditions[1]["condition"] == "LESS_THAN_OR_EQUAL_TO"
        assert filter_conditions[1]["value"] == str(int(end_time.timestamp() * 1000))
