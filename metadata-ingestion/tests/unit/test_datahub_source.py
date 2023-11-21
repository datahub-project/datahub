from dataclasses import dataclass

import pytest

from datahub.ingestion.source.datahub.datahub_database_reader import (
    VersionOrderable,
    VersionOrderer,
)


@dataclass
class MockRow(VersionOrderable):
    createdon: int
    version: int
    urn: str


@pytest.fixture
def rows():
    return [
        MockRow(0, 0, "one"),
        MockRow(0, 1, "one"),
        MockRow(0, 0, "two"),
        MockRow(0, 0, "three"),
        MockRow(0, 1, "three"),
        MockRow(0, 2, "three"),
        MockRow(0, 1, "two"),
        MockRow(0, 4, "three"),
        MockRow(0, 5, "three"),
        MockRow(1, 6, "three"),
        MockRow(1, 0, "four"),
        MockRow(2, 0, "five"),
        MockRow(2, 1, "six"),
        MockRow(2, 0, "six"),
        MockRow(3, 0, "seven"),
        MockRow(3, 0, "eight"),
    ]


def test_version_orderer(rows):
    orderer = VersionOrderer[MockRow](enabled=True)
    ordered_rows = list(orderer(rows))
    assert ordered_rows == sorted(
        ordered_rows, key=lambda x: (x.createdon, x.version == 0)
    )


def test_version_orderer_disabled(rows):
    orderer = VersionOrderer[MockRow](enabled=False)
    ordered_rows = list(orderer(rows))
    assert ordered_rows == rows
