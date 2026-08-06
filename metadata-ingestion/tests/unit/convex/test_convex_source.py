from typing import Any, Dict, List, Optional

import pytest

from datahub.ingestion.source.convex.client import ConvexStreamingExportClient
from datahub.ingestion.source.convex.source import schema_fields_from_json_schema
from datahub.metadata.schema_classes import (
    NumberTypeClass,
    RecordTypeClass,
    StringTypeClass,
    UnionTypeClass,
)

# Trimmed from a real `/api/json_schemas` response for a `plays` table.
PLAYS_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "_creationTime": {"type": "number"},
        "_id": {"$description": "Id(plays)", "type": "string"},
        "artistRaw": {"type": "string"},
        "canonicalArtistId": {"$description": "Id(artists)", "type": "string"},
        "playedAt": {"type": "number"},
        "raw": {
            "anyOf": [
                {"type": "string"},
                {
                    "type": "object",
                    "properties": {"StreamTitle": {"type": "string"}},
                    "required": ["StreamTitle"],
                },
            ]
        },
        "nested": {"type": "object", "properties": {"x": {"type": "number"}}},
        "_table": {"type": "string"},
        "_ts": {"type": "integer"},
        "_deleted": {"type": "boolean"},
    },
    "required": ["_creationTime", "_id", "artistRaw", "playedAt"],
}


def test_system_fields_excluded() -> None:
    names = {field.fieldPath for field in schema_fields_from_json_schema(PLAYS_SCHEMA)}
    assert names == {
        "_creationTime",
        "_id",
        "artistRaw",
        "canonicalArtistId",
        "playedAt",
        "raw",
        "nested",
    }


def test_type_mapping() -> None:
    fields = {
        field.fieldPath: field for field in schema_fields_from_json_schema(PLAYS_SCHEMA)
    }
    assert isinstance(fields["artistRaw"].type.type, StringTypeClass)
    assert isinstance(fields["playedAt"].type.type, NumberTypeClass)
    assert isinstance(fields["raw"].type.type, UnionTypeClass)
    assert isinstance(fields["nested"].type.type, RecordTypeClass)
    assert fields["raw"].nativeDataType == "anyOf(string, object)"


def test_reference_descriptions_and_nullability() -> None:
    fields = {
        field.fieldPath: field for field in schema_fields_from_json_schema(PLAYS_SCHEMA)
    }
    assert fields["canonicalArtistId"].description == "Id(artists)"
    assert fields["canonicalArtistId"].nullable  # not in the required list
    assert not fields["artistRaw"].nullable


class _FakeClient(ConvexStreamingExportClient):
    """Client whose snapshot pages come from a list instead of the network."""

    def __init__(self, pages: List[Dict[str, Any]]) -> None:
        super().__init__("https://example.convex.cloud", "fake-key")
        self.pages = pages
        self.requests = 0

    def _get(self, path: str, params: Optional[Dict[str, str]] = None) -> Any:
        self.requests += 1
        return self.pages[self.requests - 1]


@pytest.mark.parametrize(
    "pages,max_pages,expected_count,expected_exact",
    [
        # Snapshot exhausted within the page cap.
        ([{"values": [1, 2, 3], "hasMore": False}], 10, 3, True),
        (
            [
                {"values": [1, 2], "hasMore": True, "cursor": "c1"},
                {"values": [3], "hasMore": False},
            ],
            10,
            3,
            True,
        ),
        # Page cap hit first, so the count is only a lower bound.
        (
            [
                {"values": [1, 2], "hasMore": True, "cursor": "c1"},
                {"values": [3, 4], "hasMore": True, "cursor": "c2"},
            ],
            2,
            4,
            False,
        ),
    ],
)
def test_count_rows(
    pages: List[Dict[str, Any]],
    max_pages: int,
    expected_count: int,
    expected_exact: bool,
) -> None:
    client = _FakeClient(pages)
    row_count = client.count_rows("plays", max_pages)
    assert row_count.count == expected_count
    assert row_count.exact is expected_exact
    assert client.requests == len(pages)
