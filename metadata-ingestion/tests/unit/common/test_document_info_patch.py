from unittest.mock import MagicMock, PropertyMock

import pytest

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.common.document_info_patch import (
    supports_document_info_patch,
)
from datahub.utilities.server_config_util import RestServiceConfig


def _graph(raw_config: dict) -> DataHubGraph:
    graph = MagicMock(spec=DataHubGraph)
    graph.server_config = RestServiceConfig(raw_config=raw_config)
    return graph


@pytest.mark.parametrize(
    "raw_config, expected",
    [
        ({"documentInfoPatchCapable": True}, True),
        ({"documentInfoPatchCapable": False}, False),
        ({}, False),
    ],
)
def test_follows_the_server_flag(raw_config: dict, expected: bool) -> None:
    assert supports_document_info_patch(_graph(raw_config)) is expected


def test_no_graph_falls_back_to_upsert() -> None:
    assert not supports_document_info_patch(None)


def test_server_config_error_falls_back_to_upsert() -> None:
    graph = MagicMock(spec=DataHubGraph)
    type(graph).server_config = PropertyMock(side_effect=ConnectionError())

    assert not supports_document_info_patch(graph)
