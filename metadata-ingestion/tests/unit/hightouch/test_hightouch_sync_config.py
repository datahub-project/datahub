from typing import Any, Dict

import pytest

from datahub.ingestion.source.hightouch.config import HightouchSourceReport
from datahub.ingestion.source.hightouch.models import HightouchSync
from datahub.ingestion.source.hightouch.sync_config import (
    destination_object,
    field_mappings,
)


def make_sync(configuration: Dict[str, Any]) -> HightouchSync:
    return HightouchSync.model_validate(
        {
            "id": 401,
            "slug": "sync",
            "workspaceId": 1,
            "modelId": 201,
            "destinationId": 301,
            "createdAt": "2024-01-01T00:00:00Z",
            "updatedAt": "2024-01-01T00:00:00Z",
            "configuration": configuration,
        }
    )


@pytest.mark.parametrize(
    "configuration, expected",
    [
        ({"object": "Lead"}, "Lead"),
        ({"tableName": "contacts"}, "contacts"),
        ({"table": {"name": "contacts"}}, "contacts"),
        ({"type": "event", "eventName": "Signed Up"}, "Signed Up"),
        ({"type": "event", "eventName": {"from": "event_col"}}, "event_col"),
        ({"url": "https://example.com/hook"}, None),
        ({"object": ""}, None),
    ],
)
def test_destination_object(configuration, expected):
    assert destination_object(make_sync(configuration)) == expected


def test_field_mappings_include_external_id_and_skip_non_column_mappings():
    report = HightouchSourceReport()
    sync = make_sync(
        {
            "mappings": [
                {"from": "email", "to": "Email", "type": "standard"},
                {"from": None, "to": "Source", "type": "static"},
                {"to": "Name", "type": "template", "template": "{{ name }}"},
            ],
            "externalIdMapping": {"from": "id", "to": "External_Id__c"},
        }
    )
    pairs = [
        (m.source_field, m.destination_field) for m in field_mappings(sync, report)
    ]
    assert pairs == [("email", "Email"), ("id", "External_Id__c")]
    assert report.field_mappings_dropped == 0


@pytest.mark.parametrize(
    "configuration",
    [
        {"fieldMappings": [{"sourceField": "email", "destinationField": "Email"}]},
        {"columnMappings": {"Email": "email"}},
    ],
)
def test_field_mappings_other_shapes(configuration):
    mappings = field_mappings(make_sync(configuration), HightouchSourceReport())
    assert [(m.source_field, m.destination_field) for m in mappings] == [
        ("email", "Email")
    ]


def test_malformed_field_mappings_are_reported():
    report = HightouchSourceReport()
    sync = make_sync({"mappings": [{"from": "email"}, "not-a-mapping"]})
    assert field_mappings(sync, report) == []
    assert report.field_mappings_dropped == 2
    assert report.warnings
