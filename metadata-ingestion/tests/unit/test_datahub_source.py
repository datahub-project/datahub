from typing import Any, Dict

import pytest

from datahub.ingestion.source.datahub.datahub_database_reader import VersionOrderer


@pytest.fixture
def rows():
    return [
        {"createdon": 0, "version": 0, "urn": "one"},
        {"createdon": 0, "version": 1, "urn": "one"},
        {"createdon": 0, "version": 0, "urn": "two"},
        {"createdon": 0, "version": 0, "urn": "three"},
        {"createdon": 0, "version": 1, "urn": "three"},
        {"createdon": 0, "version": 2, "urn": "three"},
        {"createdon": 0, "version": 1, "urn": "two"},
        {"createdon": 0, "version": 4, "urn": "three"},
        {"createdon": 0, "version": 5, "urn": "three"},
        {"createdon": 1, "version": 6, "urn": "three"},
        {"createdon": 1, "version": 0, "urn": "four"},
        {"createdon": 2, "version": 0, "urn": "five"},
        {"createdon": 2, "version": 1, "urn": "six"},
        {"createdon": 2, "version": 0, "urn": "six"},
        {"createdon": 3, "version": 0, "urn": "seven"},
        {"createdon": 3, "version": 0, "urn": "eight"},
    ]


def test_version_orderer(rows):
    orderer = VersionOrderer[Dict[str, Any]](enabled=True)
    ordered_rows = list(orderer(rows))
    assert ordered_rows == sorted(
        ordered_rows, key=lambda x: (x["createdon"], x["version"] == 0)
    )


def test_version_orderer_disabled(rows):
    orderer = VersionOrderer[Dict[str, Any]](enabled=False)
    ordered_rows = list(orderer(rows))
    assert ordered_rows == rows
