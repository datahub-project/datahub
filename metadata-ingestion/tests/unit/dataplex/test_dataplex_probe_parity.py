"""`probe filter` against what Dataplex ingestion really emits: both read the
same fake Universal Catalog, through the one CatalogServiceClient class they
each construct."""

from pathlib import Path
from typing import Any, Dict, Iterator, List, Set
from unittest import mock

import pytest
from google.auth.credentials import AnonymousCredentials
from google.cloud import dataplex_v1

from datahub.metadata.urns import DatasetUrn
from tests.test_helpers.probe_parity import (
    EmittedIndex,
    FanOut,
    JudgedRecord,
    ParityListing,
    assert_probe_parity,
    pipeline_ingestion,
)

# The parity harness masks its reports against the process-global registry, as
# the CLI does; a secret an earlier test registered could otherwise redact a
# fixture identifier.
pytestmark = pytest.mark.usefixtures("_isolate_secret_registry")


@pytest.fixture(autouse=True)
def _no_application_default_credentials() -> Iterator[None]:
    # Ingestion and the probe both fall back to Application Default Credentials;
    # CI has none, and the fake catalog needs none.
    with mock.patch(
        "google.auth.default", return_value=(AnonymousCredentials(), PROJECT)
    ):
        yield


PROJECT = "proj-a"
LOCATION = f"projects/{PROJECT}/locations/us"
SALES = f"{LOCATION}/entryGroups/sales"
STAGING = f"{LOCATION}/entryGroups/staging"
BIGQUERY_TABLE = "projects/655216118709/locations/global/entryTypes/bigquery-table"
# group -> table names; each table is an entry whose FQN is
# bigquery:<PROJECT>.<group>.<table>.
CATALOG: Dict[str, List[str]] = {
    SALES: ["orders", "orders_tmp", "refunds"],
    STAGING: ["raw_load"],
}


def _entry(group: str, table: str) -> dataplex_v1.Entry:
    dataset = group.rsplit("/", 1)[-1]
    return dataplex_v1.Entry(
        name=f"{group}/entries/{table}",
        entry_type=BIGQUERY_TABLE,
        fully_qualified_name=f"bigquery:{PROJECT}.{dataset}.{table}",
    )


class _FakeCatalog:
    """The CatalogServiceClient calls ingestion and the probe make."""

    def __init__(self, credentials: Any = None) -> None:
        self.transport = mock.MagicMock()

    def list_entry_groups(
        self, request: dataplex_v1.ListEntryGroupsRequest
    ) -> Iterator[dataplex_v1.EntryGroup]:
        for group in CATALOG:
            if group.startswith(f"{request.parent}/"):
                yield dataplex_v1.EntryGroup(name=group)

    def list_entries(
        self, request: dataplex_v1.ListEntriesRequest
    ) -> Iterator[dataplex_v1.Entry]:
        for table in CATALOG.get(request.parent, []):
            yield _entry(request.parent, table)

    def get_entry(self, request: dataplex_v1.GetEntryRequest) -> dataplex_v1.Entry:
        group, _, table = request.name.rpartition("/entries/")
        return _entry(group, table)

    def search_entries(self, request: Any) -> List[Any]:
        return []


def _emitted_tables(index: EmittedIndex) -> Set[str]:
    return {DatasetUrn.from_string(u).name for u in index.urns("dataset")}


def _table_of(record: JudgedRecord) -> str:
    return record.attributes["fully_qualified_name"].removeprefix("bigquery:")


_ENTRIES = ParityListing(
    "entries",
    "entries",
    _emitted_tables,
    kwargs={"project": PROJECT},
    fan_out=FanOut("entry_groups", "entry_group", parent_kwargs={"project": PROJECT}),
    identity=_table_of,
)


def _recipe(**filters: Any) -> Dict[str, Any]:
    return {
        "project_ids": [PROJECT],
        "entries_locations": ["us"],
        "include_lineage": False,
        "filter_config": filters,
    }


@pytest.mark.parametrize(
    "filters, excluded",
    [
        (
            {"entries": {"pattern": {"deny": [".*_tmp$"]}}},
            {"proj-a.sales.orders_tmp": "filter_config.entries.pattern"},
        ),
        (
            {"entries": {"fqn_pattern": {"deny": ["bigquery:.*\\.refunds$"]}}},
            {"proj-a.sales.refunds": "filter_config.entries.fqn_pattern"},
        ),
        (
            {"entry_groups": {"pattern": {"deny": [".*/staging$"]}}},
            {"proj-a.staging.raw_load": "filter_config.entry_groups.pattern"},
        ),
    ],
    ids=["entry-name", "entry-fqn", "entry-group"],
)
def test_entry_verdicts_match_ingestion(
    tmp_path: Path, filters: Dict[str, Any], excluded: Dict[str, str]
) -> None:
    with mock.patch.object(dataplex_v1, "CatalogServiceClient", _FakeCatalog):
        report = assert_probe_parity(
            "dataplex",
            _recipe(**filters),
            pipeline_ingestion("dataplex", tmp_path),
            [_ENTRIES],
        )
    assert report.excluded_by("entries") == excluded
