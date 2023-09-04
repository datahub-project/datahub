from typing import Any, List, Optional
from unittest.mock import ANY, Mock, patch

from datahub.ingestion.graph.client import DatahubClientConfig

from datahub_monitors.graph import DataHubAssertionGraph


@patch.object(DataHubAssertionGraph, "test_connection", return_value=None)
class TestDataHubAssertionGraph:
    """Unit tests for the DataHubAssertionGraph class"""

    mock_aspects = [
        {
            "aspect": {
                "urn": "urn:li:datasetAspect:test",
                "name": "test",
                "value": '{"test": "test"}',
            }
        },
        {
            "aspect": {
                "urn": "urn:li:datasetAspect:test2",
                "name": "test2",
                "value": '{"test2": "test2"}',
            }
        },
    ]

    @patch.object(DataHubAssertionGraph, "_post_generic", return_value=None)
    def test_get_timeseries_values_no_result(
        self, post_mock: Mock, connection_mock: Mock
    ) -> None:
        """Test that get_timeseries_values() returns None when an Exception is raised"""

        graph = DataHubAssertionGraph(DatahubClientConfig())
        post_mock.side_effect = Exception("Test Exception")

        result: Optional[List[Any]] = graph.get_timeseries_values(
            entity_urn="urn:li:dataset:test",
            aspect_type=Mock(),
            filter={},
        )

        assert result is None

    @patch.object(
        DataHubAssertionGraph, "_post_generic", return_value={"value": {"values": []}}
    )
    def test_get_timeseries_values_empty_result(
        self, post_mock: Mock, connection_mock: Mock
    ) -> None:
        """Test that get_timeseries_values() returns an empty List when no results are found"""

        graph = DataHubAssertionGraph(DatahubClientConfig())
        result: Optional[List[Any]] = graph.get_timeseries_values(
            entity_urn="urn:li:dataset:test",
            aspect_type=Mock(),
            filter={},
        )

        assert result == []

    @patch.object(
        DataHubAssertionGraph,
        "_post_generic",
        return_value={"value": {"values": mock_aspects}},
    )
    def test_get_timeseries_values_result(
        self, post_mock: Mock, connection_mock: Mock
    ) -> None:
        """Test that get_timeseries_values() returns a list of Aspects when results are found"""

        graph = DataHubAssertionGraph(DatahubClientConfig())
        result: Optional[List[Any]] = graph.get_timeseries_values(
            entity_urn="urn:li:dataset:test",
            aspect_type=Mock(),
            filter={},
        )

        assert result is not None
        assert len(result) == 2

    @patch.object(
        DataHubAssertionGraph,
        "_post_generic",
        return_value={"value": {"values": mock_aspects}},
    )
    def test_get_timeseries_values_parameters(
        self, post_mock: Mock, connection_mock: Mock
    ) -> None:
        """Test that parameters are correctly applied to the query"""

        graph = DataHubAssertionGraph(DatahubClientConfig())
        graph.get_timeseries_values(
            entity_urn="urn:li:dataset:test",
            aspect_type=Mock(),
            filter={
                "or": [
                    {"and": [{"field": "test", "operator": "EQUALS", "value": "test"}]}
                ]
            },
        )

        post_mock.assert_called_with(
            "http://localhost:8080/aspects?action=getTimeseriesAspectValues",
            {
                "urn": "urn:li:dataset:test",
                "entity": "dataset",
                "aspect": ANY,
                "limit": 1,
                "filter": {
                    "or": [
                        {
                            "and": [
                                {"field": "test", "operator": "EQUALS", "value": "test"}
                            ]
                        }
                    ]
                },
            },
        )
