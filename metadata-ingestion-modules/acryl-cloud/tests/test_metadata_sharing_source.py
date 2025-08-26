from typing import cast
from unittest.mock import MagicMock, patch

import pytest

from acryl_datahub_cloud.datahub_metadata_sharing.metadata_sharing_source import (
    DataHubMetadataSharingSource,
    DataHubMetadataSharingSourceConfig,
    GraphQLError,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph


@pytest.fixture
def mock_graph() -> MagicMock:
    return MagicMock(spec=DataHubGraph)


@pytest.fixture
def mock_context(mock_graph: MagicMock) -> MagicMock:
    ctx = MagicMock(spec=PipelineContext)
    ctx.require_graph.return_value = mock_graph
    return ctx


@pytest.fixture
def basic_config() -> DataHubMetadataSharingSourceConfig:
    return DataHubMetadataSharingSourceConfig(
        batch_size=2,
        batch_delay_ms=0,  # No delay for tests
    )


@pytest.fixture
def source(
    mock_context: MagicMock, basic_config: DataHubMetadataSharingSourceConfig
) -> DataHubMetadataSharingSource:
    source = DataHubMetadataSharingSource(basic_config, mock_context)
    # Initialize the graph client and ensure it's typed as a MagicMock
    source.graph = cast(DataHubGraph, mock_context.require_graph())
    assert source.graph is not None  # For mypy
    return source


def test_init(
    source: DataHubMetadataSharingSource,
    mock_context: MagicMock,
    basic_config: DataHubMetadataSharingSourceConfig,
) -> None:
    """Test basic initialization of the source"""
    assert source.config == basic_config
    assert source.ctx == mock_context
    assert source.report is not None
    assert source.report.entities_shared == 0
    assert source.report.entities_failed == 0
    assert source.report.implicit_entities_skipped == 0


def test_execute_graphql_with_retry_success(
    source: DataHubMetadataSharingSource,
) -> None:
    """Test successful GraphQL execution"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    expected_response = {"data": {"some": "data"}}
    mock_graph.execute_graphql.return_value = expected_response

    response = source.execute_graphql_with_retry("query {}", {})

    assert response == expected_response
    mock_graph.execute_graphql.assert_called_once_with("query {}", variables={})


def test_execute_graphql_with_retry_error(source: DataHubMetadataSharingSource) -> None:
    """Test GraphQL execution with error"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    mock_graph.execute_graphql.return_value = {"error": "Something went wrong"}

    with pytest.raises(GraphQLError, match="GraphQL error: Something went wrong"):
        source.execute_graphql_with_retry("query {}", {})


def test_scroll_shared_entities(source: DataHubMetadataSharingSource) -> None:
    """Test scrolling through shared entities"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    mock_response = {
        "scrollAcrossEntities": {
            "nextScrollId": "next-page",
            "searchResults": [
                {"entity": {"urn": "urn:1"}},
                {"entity": {"urn": "urn:2"}},
            ],
        }
    }
    mock_graph.execute_graphql.return_value = mock_response

    next_scroll_id, results = source.scroll_shared_entities("scroll-1", 2)

    assert next_scroll_id == "next-page"
    assert len(results) == 2
    assert results[0]["entity"]["urn"] == "urn:1"


def test_process_single_entity_success(source: DataHubMetadataSharingSource) -> None:
    """Test processing a single entity successfully"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    # Set up the successful share response first
    mock_graph.execute_graphql.return_value = {"shareEntity": {"succeeded": True}}

    entity_result = {
        "entity": {
            "urn": "urn:test:1",
            "share": {
                "lastShareResults": [
                    {
                        "destination": {"urn": "dest:1"},
                        "status": "PENDING",
                        "shareConfig": {
                            "enableUpstreamLineage": True,
                            "enableDownstreamLineage": False,
                        },
                    }
                ]
            },
        },
    }

    source._process_single_entity(entity_result)

    assert source.report.entities_shared == 1
    assert source.report.entities_failed == 0


def test_process_single_entity_implicit_share(
    source: DataHubMetadataSharingSource,
) -> None:
    """Test skipping implicitly shared entities"""
    entity_result = {
        "entity": {
            "urn": "urn:test:1",
            "share": {
                "lastShareResults": [
                    {
                        "destination": {"urn": "dest:1"},
                        "implicitShareEntity": {"urn": "urn:implicit:1"},
                        "status": "SUCCESS",
                        "shareConfig": {},
                    }
                ]
            },
        },
    }

    source._process_single_entity(entity_result)

    assert source.report.implicit_entities_skipped == 1
    assert source.report.entities_shared == 0
    assert source.report.entities_failed == 0


def test_process_single_entity_failure(source: DataHubMetadataSharingSource) -> None:
    """Test handling failed entity sharing"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    # Set up the failed share response
    mock_graph.execute_graphql.return_value = {"shareEntity": {"succeeded": False}}

    entity_result = {
        "entity": {
            "urn": "urn:test:1",
            "share": {
                "lastShareResults": [
                    {
                        "destination": {"urn": "dest:1"},
                        "status": "PENDING",
                        "shareConfig": {},
                    }
                ]
            },
        },
    }

    source._process_single_entity(entity_result)

    assert source.report.entities_shared == 0
    assert source.report.entities_failed == 1


def test_determine_lineage_direction() -> None:
    """Test lineage direction determination"""
    mock_context = MagicMock(spec=PipelineContext)
    mock_graph = MagicMock(spec=DataHubGraph)
    mock_context.require_graph.return_value = mock_graph

    source = DataHubMetadataSharingSource(
        DataHubMetadataSharingSourceConfig(), mock_context
    )
    source.graph = cast(DataHubGraph, mock_graph)  # Properly type the mock

    assert (
        source._determine_lineage_direction(
            {"enableUpstreamLineage": True, "enableDownstreamLineage": True}
        )
        == "BOTH"
    )

    assert (
        source._determine_lineage_direction(
            {"enableUpstreamLineage": True, "enableDownstreamLineage": False}
        )
        == "UPSTREAM"
    )

    assert (
        source._determine_lineage_direction(
            {"enableUpstreamLineage": False, "enableDownstreamLineage": True}
        )
        == "DOWNSTREAM"
    )

    assert source._determine_lineage_direction({}) is None


def test_reshare_entities_full_cycle(source: DataHubMetadataSharingSource) -> None:
    """Test full cycle of resharing entities"""
    assert source.graph is not None  # For mypy
    mock_graph = cast(MagicMock, source.graph)

    # Mock responses in sequence
    mock_graph.execute_graphql.side_effect = [
        # First scroll response
        {
            "scrollAcrossEntities": {
                "nextScrollId": "page2",
                "searchResults": [
                    {
                        "entity": {
                            "urn": "urn:1",
                            "share": {
                                "lastShareResults": [
                                    {
                                        "destination": {"urn": "dest:1"},
                                        "status": "SUCCESS",
                                        "shareConfig": {},
                                    }
                                ]
                            },
                        },
                    }
                ],
            }
        },
        # First share response
        {"shareEntity": {"succeeded": True}},
        # Second scroll response
        {
            "scrollAcrossEntities": {
                "nextScrollId": None,
                "searchResults": [
                    {
                        "entity": {
                            "urn": "urn:2",
                            "share": {
                                "lastShareResults": [
                                    {
                                        "destination": {"urn": "dest:1"},
                                        "implicitShareEntity": {"urn": "implicit:1"},
                                        "status": "SUCCESS",
                                        "shareConfig": {},
                                    }
                                ]
                            },
                        },
                    }
                ],
            }
        },
    ]

    source.reshare_entities()

    assert source.report.entities_shared == 1
    assert source.report.implicit_entities_skipped == 1
    assert source.report.batches_processed == 2


def test_get_workunits(source: DataHubMetadataSharingSource) -> None:
    """Test that get_workunits initializes graph and returns empty list"""
    with patch.object(source, "reshare_entities") as mock_reshare:
        workunits = list(source.get_workunits())
        assert len(workunits) == 0
        mock_reshare.assert_called_once()
