from types import SimpleNamespace

import pytest
from pydantic import ConfigDict

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.ingestion.agent.models import ProbeLeafKind
from datahub.ingestion.source.common.subtypes import DatasetSubTypes

# The probe reuses the ThoughtSpot SDK client — skip when its module can't import.
pytest.importorskip("datahub.ingestion.source.thoughtspot.client")

import datahub.ingestion.source.thoughtspot.client as ts_client
from datahub.ingestion.source.thoughtspot.thoughtspot_probe import (
    list_thoughtspot_children,
)


class _FakeClient:
    def __init__(self, tables):
        self._tables = tables
        self.closed = False

    def get_logical_tables(self):
        return self._tables

    def close(self):
        self.closed = True


def _table(name, columns):
    return SimpleNamespace(
        name=name, columns=[SimpleNamespace(name=c) for c in columns]
    )


def _install(monkeypatch, tables):
    client = _FakeClient(tables)
    monkeypatch.setattr(ts_client, "ThoughtSpotClient", lambda connection: client)
    return client


# A real pydantic config (not a plain SimpleNamespace) so resolve_pattern_field can
# introspect model_fields for worksheet_pattern, which the probe now resolves by
# convention rather than declaring explicitly.
class _Config(ConfigModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    worksheet_pattern: AllowDenyPattern = AllowDenyPattern.allow_all()
    connection: object


def test_top_level_lists_worksheets_with_pattern_verdict(monkeypatch):
    client = _install(
        monkeypatch, [_table("Sales", ["id", "amount"]), _table("LegacyLeads", [])]
    )
    config = _Config(
        worksheet_pattern=AllowDenyPattern(allow=[".*"], deny=["^Legacy.*"]),
        connection=object(),
    )
    result = list_thoughtspot_children(config, [], 100)
    by_name = {n.name: n for n in result.nodes}
    assert by_name["Sales"].kind == DatasetSubTypes.THOUGHTSPOT_WORKSHEET
    assert by_name["Sales"].pattern_field == "worksheet_pattern"
    assert by_name["Sales"].included is True
    # Reuses the connector's own worksheet_pattern for the verdict.
    assert by_name["LegacyLeads"].included is False
    assert by_name["LegacyLeads"].excluded_by == "worksheet_pattern"
    assert client.closed


def test_worksheet_children_are_columns(monkeypatch):
    _install(monkeypatch, [_table("Sales", ["id", "amount"])])
    config = _Config(
        worksheet_pattern=AllowDenyPattern.allow_all(), connection=object()
    )
    result = list_thoughtspot_children(config, ["Sales"], 100)
    assert {n.name for n in result.nodes} == {"id", "amount"}
    assert all(n.kind == ProbeLeafKind.COLUMN for n in result.nodes)
