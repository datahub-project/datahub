"""The direction argument of the openapi graph methods accepts strings.

RelationshipDirection and LineageDirection are StrEnums, so "DOWNSTREAM" is as
valid on the wire as LineageDirection.DOWNSTREAM. These methods used to read
`direction.value`, which raised AttributeError for the string spelling.
"""

from typing import Dict, Optional
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.openapi import LineageDirection, RelationshipDirection


def _graph(response: Dict) -> DataHubGraph:
    """A DataHubGraph wired with only the attrs these methods touch."""
    graph = DataHubGraph.__new__(DataHubGraph)
    graph._gms_server = "http://localhost:8080"
    graph._post_generic = MagicMock(return_value=response)  # type: ignore[method-assign]
    graph._get_generic = MagicMock(return_value=response)  # type: ignore[method-assign]
    return graph


def _sent_direction(mock: MagicMock) -> Optional[str]:
    return mock.call_args.kwargs["params"].get("direction")


@pytest.mark.parametrize(
    "direction", [LineageDirection.DOWNSTREAM, "DOWNSTREAM"], ids=["enum", "str"]
)
def test_scroll_lineage_direction(direction) -> None:
    graph = _graph({"results": []})
    graph.scroll_lineage(urns=["urn:li:dataset:(a,b,PROD)"], direction=direction)
    assert _sent_direction(graph._post_generic) == "DOWNSTREAM"


@pytest.mark.parametrize(
    "direction", [RelationshipDirection.OUTGOING, "OUTGOING"], ids=["enum", "str"]
)
def test_scroll_relationships_direction(direction) -> None:
    graph = _graph({"results": []})
    graph.scroll_relationships(direction=direction)
    assert _sent_direction(graph._post_generic) == "OUTGOING"


@pytest.mark.parametrize(
    "direction", [RelationshipDirection.INCOMING, "INCOMING"], ids=["enum", "str"]
)
def test_get_related_entities_direction(direction) -> None:
    graph = _graph({"entities": [], "count": 0})
    list(
        graph.get_related_entities(
            "urn:li:dataset:(a,b,PROD)", ["DownstreamOf"], direction
        )
    )
    assert _sent_direction(graph._get_generic) == "INCOMING"


def test_omitted_direction_is_not_sent() -> None:
    """None must stay absent so the server applies its own default."""
    graph = _graph({"results": []})
    graph.scroll_lineage(urns=["urn:li:dataset:(a,b,PROD)"])
    assert _sent_direction(graph._post_generic) is None
