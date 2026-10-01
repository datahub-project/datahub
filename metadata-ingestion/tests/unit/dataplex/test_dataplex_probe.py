"""Dataplex probe listings against fake gRPC clients, and through
run_probe_method so kind, parent_path, truncation and warnings are the
framework's real ones."""

from typing import Dict, List, Optional
from unittest.mock import Mock

import pytest
from google.api_core import exceptions
from google.cloud import dataplex_v1, resourcemanager_v3

from datahub.ingestion.agent.probe_methods import ProbeMethodResult, run_probe_method
from datahub.ingestion.agent.verdicts import ProbeConnectionError
from datahub.ingestion.source.common.gcp_project_filter import (
    _search_projects_by_labels,
)
from datahub.ingestion.source.dataplex.dataplex_config import (
    DATAPLEX_ENTRY_GROUP_KIND,
    DATAPLEX_PROJECT_KIND,
    DataplexConfig,
)
from datahub.ingestion.source.dataplex.dataplex_probe import DataplexMetadataProbe

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
    assert [r["name"] for r in result.result] == ["proj-a"]
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
    assert [r["name"] for r in result.result] == ["prod-a", "dev-a"]


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
    assert [r["name"] for r in result.result] == [GROUP_US]


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
    assert [(r["name"], r["location"]) for r in result.result] == [
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
    assert [r["name"] for r in result.result] == [GROUP_US]
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
