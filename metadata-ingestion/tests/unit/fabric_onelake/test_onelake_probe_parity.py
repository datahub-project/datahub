"""`probe filter` against what Fabric OneLake ingestion really emits: both read
the same fake tenant through the OneLakeClient each constructs."""

from pathlib import Path
from typing import Any, Dict, Iterator, Set
from unittest import mock

import pytest

from datahub.metadata.urns import DatasetUrn
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    FanOut,
    JudgedRecord,
    ParityListing,
    assert_probe_parity,
    by_name,
    pipeline_ingestion,
)
from tests.unit.fabric_onelake.onelake_fakes import LH_ID, WS, FakeOneLakeClient

# The parity harness masks its reports against the process-global registry, as
# the CLI does; a secret an earlier test registered could otherwise redact a
# fixture identifier.
pytestmark = pytest.mark.usefixtures("_isolate_secret_registry")

SOURCE = "fabric-onelake"
_ITEM = {"workspace": WS.name, "item": "lh_main", "item_type": "Lakehouse"}
# No SQL endpoint: views, columns and usage need ODBC, which is out of scope.
_NO_SQL_ENDPOINT: Dict[str, Any] = {
    "sql_endpoint": {"enabled": False},
    "extract_views": False,
    "extract_schema": {"enabled": False},
    "usage": {"include_usage_statistics": False},
    # One workspace, so bare container names identify what was emitted.
    "workspace_pattern": {"deny": ["^hr-ws$"]},
}


def _main_lakehouse_tables(index: EmittedIndex) -> Set[str]:
    prefix = f"{WS.id}.{LH_ID}."
    names = {DatasetUrn.from_string(u).name for u in index.urns("dataset")}
    return {n.removeprefix(prefix) for n in names if n.startswith(prefix)}


def _schema_qualified(record: JudgedRecord) -> str:
    return f"{record.parent_path[-1]}.{record.name}"


_WORKSPACES = ParityListing(
    "workspaces",
    "workspaces",
    lambda index: index.container_names("Fabric Workspace"),
)
_LAKEHOUSES = ParityListing(
    "lakehouses",
    "lakehouses",
    lambda index: index.container_names("Fabric Lakehouse"),
    kwargs={"workspace": WS.name},
    identity=by_name,
)
_TABLES = ParityListing(
    "tables",
    "tables",
    _main_lakehouse_tables,
    kwargs=_ITEM,
    fan_out=FanOut("schemas", "schema", parent_kwargs=_ITEM),
    identity=_schema_qualified,
)


@pytest.fixture
def _fake_tenant() -> Iterator[None]:
    fake = FakeOneLakeClient()
    with (
        mock.patch(
            "datahub.ingestion.source.fabric.onelake.source.OneLakeClient",
            return_value=fake,
        ),
        mock.patch(
            "datahub.ingestion.source.fabric.onelake.onelake_probe.OneLakeClient",
            return_value=fake,
        ),
    ):
        yield


@pytest.mark.usefixtures("_fake_tenant")
@pytest.mark.parametrize(
    "filters, excluded",
    [
        ({}, {"lakehouses": {}, "tables": {}}),
        (
            {"lakehouse_pattern": {"deny": ["^shared_name$"]}},
            {"lakehouses": {"shared_name": "lakehouse_pattern"}, "tables": {}},
        ),
        (
            {"schema_pattern": {"deny": ["^finance$"]}},
            {"lakehouses": {}, "tables": {"finance.refunds": "schema_pattern"}},
        ),
        (
            {"table_pattern": {"deny": ["^staging\\..*"]}},
            {"lakehouses": {}, "tables": {"staging.tmp_load": "table_pattern"}},
        ),
    ],
    ids=["defaults", "lakehouse", "schema", "table"],
)
def test_verdicts_match_ingestion(
    tmp_path: Path, filters: Dict[str, Any], excluded: Dict[str, Dict[str, str]]
) -> None:
    report = assert_probe_parity(
        SOURCE,
        {**_NO_SQL_ENDPOINT, **filters},
        pipeline_ingestion(SOURCE, tmp_path),
        [_WORKSPACES, _LAKEHOUSES, _TABLES],
    )
    assert report.excluded_by("workspaces") == {"hr-ws": "workspace_pattern"}
    assert report.excluded_by("lakehouses") == excluded["lakehouses"]
    assert report.excluded_by("tables") == excluded["tables"]
