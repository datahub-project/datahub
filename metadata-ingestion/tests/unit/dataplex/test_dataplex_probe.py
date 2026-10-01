"""Dataplex probe listings against fake gRPC clients, and through
run_probe_method so kind, parent_path, truncation and warnings are the
framework's real ones."""

import json
import logging
from typing import Dict, Iterator, List, Optional
from unittest.mock import Mock

import pytest
from google.api_core import exceptions
from google.cloud import dataplex_v1, resourcemanager_v3

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.agent.probe_methods import ProbeMethodResult, run_probe_method
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.source.common.gcp_project_filter import (
    _search_projects_by_labels,
)
from datahub.ingestion.source.dataplex.dataplex_config import (
    DATAPLEX_ASPECT_TYPE_KIND,
    DATAPLEX_ENTRY_FQN_KIND,
    DATAPLEX_ENTRY_GROUP_KIND,
    DATAPLEX_ENTRY_KIND,
    DATAPLEX_PROJECT_KIND,
    DataplexConfig,
)
from datahub.ingestion.source.dataplex.dataplex_probe import DataplexMetadataProbe
from datahub.ingestion.source.dataplex.dataplex_properties import (
    extract_aspects_to_custom_properties,
)

BASE: Dict[str, object] = {
    "project_ids": ["proj-a"],
    "entries_locations": ["us", "eu"],
}
# Dataplex may return the project NUMBER in resource names even when the request
# named the project id; the probe must hand back whatever the API returned.
GROUP_US = "projects/123456/locations/us/entryGroups/sales"
# Server text a GCP error can carry; none of it may reach the caller.
SERVER_DETAIL = "caller sa-name@proj-a.iam lacks dataplex.entryGroups.list"


def _catalog(groups: Dict[str, List[str]]) -> Mock:
    """A CatalogServiceClient whose list_entry_groups answers per parent, and
    raises PermissionDenied for a parent mapped to ['forbidden']."""
    client = Mock(spec=dataplex_v1.CatalogServiceClient)

    def list_entry_groups(
        request: dataplex_v1.ListEntryGroupsRequest,
    ) -> List[dataplex_v1.EntryGroup]:
        names = groups.get(request.parent, [])
        if names == ["forbidden"]:
            raise exceptions.PermissionDenied(SERVER_DETAIL)
        return [
            dataplex_v1.EntryGroup(name=n, display_name=n.rsplit("/", 1)[-1])
            for n in names
        ]

    client.list_entry_groups.side_effect = list_entry_groups
    return client


def _run(
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    kwargs: Dict[str, object],
    config: Optional[Dict[str, object]] = None,
    catalog: Optional[Mock] = None,
    projects: Optional[Mock] = None,
) -> ProbeMethodResult:
    def build(
        cls: type[DataplexMetadataProbe], cfg: DataplexConfig
    ) -> DataplexMetadataProbe:
        return cls(cfg, None, catalog_client=catalog, projects_client=projects)

    monkeypatch.setattr(DataplexMetadataProbe, "for_config", classmethod(build))
    return run_probe_method("dataplex", config or BASE, command, kwargs)


def _records(result: ProbeMethodResult) -> List[Dict[str, object]]:
    rows = result.result
    assert isinstance(rows, list)
    assert all(isinstance(r, dict) for r in rows)
    return rows


def _strings(result: ProbeMethodResult) -> List[str]:
    rows = result.result
    assert isinstance(rows, list)
    assert all(isinstance(r, str) for r in rows)
    return rows


def test_for_config_opens_no_client() -> None:
    probe = DataplexMetadataProbe.for_config(DataplexConfig.model_validate(BASE))
    assert "_catalog" not in probe.__dict__
    assert "_projects" not in probe.__dict__


def test_a_malformed_credential_is_reported_without_its_contents() -> None:
    config = DataplexConfig.model_validate(
        {
            **BASE,
            "credential": {
                "private_key_id": "key-id-value",
                "private_key": "not-a-pem-key",
                "client_email": "sa-name@proj-a.iam.gserviceaccount.com",
                "client_id": "123",
            },
        }
    )
    with pytest.raises(ValueError) as info:
        DataplexMetadataProbe.for_config(config)
    text = str(info.value)
    for secret in ("key-id-value", "not-a-pem-key", "sa-name"):
        assert secret not in text
    assert info.value.__cause__ is None
    assert info.value.__suppress_context__ is True


def test_explicit_project_ids_are_listed_without_resource_manager(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projects = Mock(spec=resourcemanager_v3.ProjectsClient)
    result = _run(monkeypatch, "projects", {}, projects=projects)
    assert result.kind == DATAPLEX_PROJECT_KIND
    assert [r["name"] for r in _records(result)] == ["proj-a"]
    projects.search_projects.assert_not_called()
    assert any("project_ids" in w for w in result.warnings)


def test_discovery_lists_denied_projects_too(monkeypatch: pytest.MonkeyPatch) -> None:
    projects = Mock(spec=resourcemanager_v3.ProjectsClient)
    projects.search_projects.return_value = iter(
        [
            resourcemanager_v3.Project(project_id="prod-a", display_name="Prod A"),
            resourcemanager_v3.Project(project_id="dev-a"),
        ]
    )
    config: Dict[str, object] = {"project_id_pattern": {"allow": ["^prod-"]}}
    result = _run(monkeypatch, "projects", {}, config=config, projects=projects)
    assert [r["name"] for r in _records(result)] == ["prod-a", "dev-a"]


def test_label_discovery_sends_the_query_ingestion_sends(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ingestion_client = Mock(spec=resourcemanager_v3.ProjectsClient)
    ingestion_client.search_projects.return_value = iter([])
    _search_projects_by_labels(frozenset({"env:prod"}), ingestion_client)

    probe_client = Mock(spec=resourcemanager_v3.ProjectsClient)
    probe_client.search_projects.return_value = iter([])
    _run(
        monkeypatch,
        "projects",
        {},
        config={"project_labels": ["env:prod"]},
        projects=probe_client,
    )

    assert (
        probe_client.search_projects.call_args
        == ingestion_client.search_projects.call_args
    )


def test_a_failed_project_search_is_scrubbed(monkeypatch: pytest.MonkeyPatch) -> None:
    projects = Mock(spec=resourcemanager_v3.ProjectsClient)
    projects.search_projects.side_effect = exceptions.PermissionDenied(SERVER_DETAIL)
    config: Dict[str, object] = {"project_id_pattern": {"allow": ["^prod-"]}}
    with pytest.raises(ProbeConnectionError) as info:
        _run(monkeypatch, "projects", {}, config=config, projects=projects)
    assert "search_projects" in str(info.value)
    assert "403" in str(info.value)
    assert SERVER_DETAIL not in str(info.value)


def test_entry_group_names_are_returned_verbatim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = _catalog({"projects/proj-a/locations/us": [GROUP_US]})
    result = _run(
        monkeypatch,
        "entry_groups",
        {"project": "proj-a", "location": "us"},
        catalog=catalog,
    )
    assert result.kind == DATAPLEX_ENTRY_GROUP_KIND
    assert result.parent_path == ["proj-a"]
    assert [r["name"] for r in _records(result)] == [GROUP_US]


def test_omitting_location_sweeps_entries_locations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    eu_group = "projects/proj-a/locations/eu/entryGroups/finance"
    catalog = _catalog(
        {
            "projects/proj-a/locations/us": [GROUP_US],
            "projects/proj-a/locations/eu": [eu_group],
        }
    )
    result = _run(monkeypatch, "entry_groups", {"project": "proj-a"}, catalog=catalog)
    assert [(r["name"], r["location"]) for r in _records(result)] == [
        (GROUP_US, "us"),
        (eu_group, "eu"),
    ]


def test_one_forbidden_location_degrades_to_a_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = _catalog(
        {
            "projects/proj-a/locations/us": [GROUP_US],
            "projects/proj-a/locations/eu": ["forbidden"],
        }
    )
    result = _run(monkeypatch, "entry_groups", {"project": "proj-a"}, catalog=catalog)
    assert [r["name"] for r in _records(result)] == [GROUP_US]
    assert any("'eu'" in w for w in result.warnings)
    assert not any(SERVER_DETAIL in w for w in result.warnings)


def test_every_location_failing_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    catalog = _catalog(
        {
            "projects/proj-a/locations/us": ["forbidden"],
            "projects/proj-a/locations/eu": ["forbidden"],
        }
    )
    with pytest.raises(ProbeConnectionError) as info:
        _run(monkeypatch, "entry_groups", {"project": "proj-a"}, catalog=catalog)
    assert "list_entry_groups" in str(info.value)
    assert SERVER_DETAIL not in str(info.value)


def test_a_named_location_that_is_forbidden_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog = _catalog({"projects/proj-a/locations/eu": ["forbidden"]})
    with pytest.raises(ProbeConnectionError) as info:
        _run(
            monkeypatch,
            "entry_groups",
            {"project": "proj-a", "location": "eu"},
            catalog=catalog,
        )
    assert SERVER_DETAIL not in str(info.value)
    assert info.value.__cause__ is None


def test_the_provider_closes_only_the_clients_it_opened() -> None:
    injected = Mock(spec=dataplex_v1.CatalogServiceClient)
    probe = DataplexMetadataProbe(
        DataplexConfig.model_validate(BASE), None, catalog_client=injected
    )
    created = Mock(spec=resourcemanager_v3.ProjectsClient)
    probe.__dict__["_projects"] = created
    probe._opened.append(created)
    assert probe._catalog is injected
    probe.__exit__(None, None, None)
    created.transport.close.assert_called_once()
    injected.transport.close.assert_not_called()


GROUP = "projects/proj-a/locations/us/entryGroups/sales"
SUPPORTED_TYPE = "projects/dataplex-types/locations/global/entryTypes/bigquery-table"
UNSUPPORTED_TYPE = (
    "projects/dataplex-types/locations/global/entryTypes/not-a-mapped-type"
)


class _CountingEntries:
    """A list_entries pager that counts how far it was read."""

    def __init__(self, entries: List[dataplex_v1.Entry]) -> None:
        self._entries = entries
        self.read = 0

    def __call__(
        self, request: dataplex_v1.ListEntriesRequest
    ) -> Iterator[dataplex_v1.Entry]:
        assert request.parent == GROUP
        for entry in self._entries:
            self.read += 1
            yield entry


def _entry(
    short: str, fqn: str, entry_type: str = SUPPORTED_TYPE
) -> dataplex_v1.Entry:
    return dataplex_v1.Entry(
        name=f"{GROUP}/entries/{short}",
        fully_qualified_name=fqn,
        entry_type=entry_type,
    )


def _catalog_with_entries(pager: _CountingEntries) -> Mock:
    client = Mock(spec=dataplex_v1.CatalogServiceClient)
    client.list_entries.side_effect = pager
    return client


def test_entries_carry_both_filter_targets_and_the_parent_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pager = _CountingEntries([_entry("orders", "bigquery:proj-a.sales.orders")])
    result = _run(
        monkeypatch,
        "entries",
        {"project": "proj-a", "entry_group": GROUP},
        catalog=_catalog_with_entries(pager),
    )
    assert result.kind == DATAPLEX_ENTRY_KIND
    assert result.parent_path == ["proj-a", GROUP]
    assert result.result == [
        {
            "name": f"{GROUP}/entries/orders",
            "fully_qualified_name": "bigquery:proj-a.sales.orders",
            "entry_type": "bigquery-table",
            "supported": True,
        }
    ]


def test_an_unmapped_entry_type_is_listed_as_unsupported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pager = _CountingEntries([_entry("x", "custom:x", entry_type=UNSUPPORTED_TYPE)])
    result = _run(
        monkeypatch,
        "entries",
        {"project": "proj-a", "entry_group": GROUP},
        catalog=_catalog_with_entries(pager),
    )
    assert _records(result)[0]["supported"] is False
    assert any("mapper" in w for w in result.warnings)


def test_an_entry_without_an_fqn_is_listed_and_flagged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pager = _CountingEntries([_entry("orphan", "")])
    result = _run(
        monkeypatch,
        "entries",
        {"project": "proj-a", "entry_group": GROUP},
        catalog=_catalog_with_entries(pager),
    )
    assert str(_records(result)[0]["name"]).endswith("/orphan")
    assert _records(result)[0]["fully_qualified_name"] == ""
    assert any("fully_qualified_name" in w for w in result.warnings)


def test_entry_fqns_skip_entries_without_one(monkeypatch: pytest.MonkeyPatch) -> None:
    pager = _CountingEntries(
        [_entry("orphan", ""), _entry("orders", "bigquery:proj-a.sales.orders")]
    )
    result = _run(
        monkeypatch,
        "entry_fqns",
        {"project": "proj-a", "entry_group": GROUP},
        catalog=_catalog_with_entries(pager),
    )
    assert result.kind == DATAPLEX_ENTRY_FQN_KIND
    assert result.parent_path == ["proj-a", GROUP]
    assert result.result == ["bigquery:proj-a.sales.orders"]


def test_a_limited_listing_stops_reading_the_pager(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pager = _CountingEntries(
        [_entry(f"t{i}", f"bigquery:proj-a.s.t{i}") for i in range(10)]
    )
    result = _run(
        monkeypatch,
        "entries",
        {"project": "proj-a", "entry_group": GROUP, "limit": "2"},
        catalog=_catalog_with_entries(pager),
    )
    assert len(_records(result)) == 2
    assert result.truncated is True
    assert pager.read == 3  # limit + 1, the framework's truncation probe


def test_an_unknown_entry_group_is_a_bad_argument(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = Mock(spec=dataplex_v1.CatalogServiceClient)
    client.list_entries.side_effect = exceptions.NotFound(SERVER_DETAIL)
    with pytest.raises(ValueError) as info:
        _run(
            monkeypatch,
            "entries",
            {"project": "proj-a", "entry_group": GROUP},
            catalog=client,
        )
    assert SERVER_DETAIL not in str(info.value)


def test_a_forbidden_entry_group_is_scrubbed(monkeypatch: pytest.MonkeyPatch) -> None:
    client = Mock(spec=dataplex_v1.CatalogServiceClient)
    client.list_entries.side_effect = exceptions.PermissionDenied(SERVER_DETAIL)
    with pytest.raises(ProbeConnectionError) as info:
        _run(
            monkeypatch,
            "entry_fqns",
            {"project": "proj-a", "entry_group": GROUP},
            catalog=client,
        )
    assert "list_entries" in str(info.value)
    assert SERVER_DETAIL not in str(info.value)


ENTRY = f"{GROUP}/entries/orders"
ASPECT_KEYS = [
    "123456.global.schema",
    "proj-a.us.datahub-tags",
    "projects/proj-a/locations/us/aspectTypes/owners",
]


def _catalog_with_detail() -> Mock:
    client = Mock(spec=dataplex_v1.CatalogServiceClient)
    client.get_entry.return_value = dataplex_v1.Entry(
        name=ENTRY, aspects={key: dataplex_v1.Aspect() for key in ASPECT_KEYS}
    )
    return client


def test_aspect_types_are_the_names_aspect_type_pattern_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = _run(
        monkeypatch,
        "entry_aspect_types",
        {"entry": ENTRY},
        catalog=_catalog_with_detail(),
    )
    assert result.kind == DATAPLEX_ASPECT_TYPE_KIND
    assert result.result == ["datahub-tags", "owners", "schema"]

    # The same names ingestion would test: every type the probe reports as
    # allowed is one ingestion turns into a custom property, and vice versa.
    props: Dict[str, str] = {}
    pattern = AllowDenyPattern(deny=["datahub-.*"])
    extract_aspects_to_custom_properties(
        {key: dataplex_v1.Aspect() for key in ASPECT_KEYS}, props, pattern
    )
    kept = {
        k.removeprefix("dataplex_aspect_")
        for k in props
        if k.startswith("dataplex_aspect_")
    }
    assert kept == {t for t in _strings(result) if pattern.allowed(t)}


def test_aspect_data_never_leaves_the_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _catalog_with_detail()
    result = _run(monkeypatch, "entry_aspect_types", {"entry": ENTRY}, catalog=client)
    assert _strings(result)
    request = client.get_entry.call_args.kwargs["request"]
    assert request.view == dataplex_v1.EntryView.ALL  # what ingestion fetches


def test_an_unknown_entry_is_a_bad_argument(monkeypatch: pytest.MonkeyPatch) -> None:
    client = Mock(spec=dataplex_v1.CatalogServiceClient)
    client.get_entry.side_effect = exceptions.NotFound(SERVER_DETAIL)
    with pytest.raises(ValueError) as info:
        _run(monkeypatch, "entry_aspect_types", {"entry": ENTRY}, catalog=client)
    assert SERVER_DETAIL not in str(info.value)


SENTINEL_VALUE = "sentinel-aspect-value-7f3a"


def test_a_retry_error_is_scrubbed(monkeypatch: pytest.MonkeyPatch) -> None:
    client = Mock(spec=dataplex_v1.CatalogServiceClient)
    client.get_entry.side_effect = exceptions.RetryError(
        f"Timeout of 60s exceeded, last exception: {SERVER_DETAIL}",
        exceptions.ServiceUnavailable(SERVER_DETAIL),
    )
    with pytest.raises(ProbeConnectionError) as info:
        _run(monkeypatch, "entry_aspect_types", {"entry": ENTRY}, catalog=client)
    assert "get_entry" in str(info.value)
    assert SERVER_DETAIL not in str(info.value)
    assert info.value.__cause__ is None


def test_aspect_values_are_neither_returned_nor_logged(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    client = Mock(spec=dataplex_v1.CatalogServiceClient)
    client.get_entry.return_value = dataplex_v1.Entry(
        name=ENTRY,
        aspects={
            "proj-a.us.owners": dataplex_v1.Aspect(data={"owner": SENTINEL_VALUE})
        },
    )
    with caplog.at_level(logging.DEBUG):
        result = _run(
            monkeypatch, "entry_aspect_types", {"entry": ENTRY}, catalog=client
        )
    assert _strings(result) == ["owners"]
    assert SENTINEL_VALUE not in json.dumps(result.to_dict())
    assert SENTINEL_VALUE not in caplog.text
