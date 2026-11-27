import json
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, Union
from unittest import mock

import pytest
import requests
from looker_sdk.error import SDKError
from looker_sdk.rtl.transport import TransportOptions
from looker_sdk.sdk.api40.models import LookmlModelExplore, Project

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.looker.looker_common import (
    https_repo_url_from_git_remote,
    lookml_file_path_from_link,
)
from datahub.ingestion.source.looker.looker_source import LookerDashboardSource
from datahub.metadata.schema_classes import (
    AuditStampClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    MetadataChangeProposalClass,
)
from datahub.sdk.dataset import Dataset
from tests.unit.looker.test_looker_source import explore, fake_client

SourceFactory = Callable[..., LookerDashboardSource]

ORDERS_LINK = "/projects/proj/files/views%2Fsales%2Forders.view.lkml?line=12"
REPO = "https://github.com/example-org/lookml"
ORDERS_URL = f"{REPO}/blob/main/views/sales/orders.view.lkml"
OLD_URL = f"{REPO}/blob/main/views/old/orders.view.lkml"
LABEL = "View in Git"
ORDERS_URN = "urn:li:dataset:(urn:li:dataPlatform:looker,sales.explore.orders,PROD)"
INVOICES_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:looker,finance.explore.invoices,PROD)"
)
INGESTION_STAMP = AuditStampClass(time=0, actor="urn:li:corpuser:__ingestion")
USER_STAMP = AuditStampClass(time=1700000000000, actor="urn:li:corpuser:jdoe")


def client_with(
    lookml_link: Optional[str], project: Optional[Project] = None
) -> mock.MagicMock:
    client = fake_client()

    def explore_with_link(
        model: str,
        explore_name: str,
        fields: Optional[str] = None,
        transport_options: Optional[TransportOptions] = None,
    ) -> LookmlModelExplore:
        result = explore(model, explore_name)
        result.lookml_link = lookml_link
        return result

    client.lookml_model_explore.side_effect = explore_with_link
    client.project.return_value = project or Project(
        git_remote_url="git@github.com:example-org/lookml.git",
        git_production_branch_name="main",
    )
    return client


def with_graph(
    source: LookerDashboardSource, *links: InstitutionalMemoryMetadataClass
) -> LookerDashboardSource:
    graph = mock.MagicMock()
    graph.get_aspect.return_value = (
        InstitutionalMemoryClass(elements=list(links)) if links else None
    )
    source.ctx.graph = graph
    return source


def link(
    url: str, stamp: AuditStampClass, label: str = LABEL
) -> InstitutionalMemoryMetadataClass:
    return InstitutionalMemoryMetadataClass(
        url=url, description=label, createStamp=stamp
    )


def link_patches(source: LookerDashboardSource) -> List[Dict[str, Any]]:
    """GenericJsonPatch payloads for institutionalMemory, one per patched explore."""
    patches = []
    for wu in source.get_workunits_internal():
        if not isinstance(wu, MetadataWorkUnit):
            continue
        mcp = wu.metadata
        if (
            isinstance(mcp, MetadataChangeProposalClass)
            and mcp.aspectName == "institutionalMemory"
        ):
            assert mcp.changeType == "PATCH"
            assert mcp.aspect is not None
            patches.append({**json.loads(mcp.aspect.value), "entityUrn": mcp.entityUrn})
    return patches


def ops(patch: Dict[str, Any]) -> List[Tuple[str, str]]:
    assert patch["arrayPrimaryKeys"] == {"elements": ["url", "description"]}
    return [(p["op"], p["path"]) for p in patch["patch"]]


def path_for(url: str) -> str:
    return "/elements/" + url.replace("/", "~1") + "/" + LABEL


@pytest.mark.parametrize(
    "lookml_link, expected",
    [
        (ORDERS_LINK, "views/sales/orders.view.lkml"),
        ("/projects/proj/files/orders.explore.lkml", "orders.explore.lkml"),
        # A project, or a repo folder, named "files" must not be taken for the marker.
        ("/projects/files/files/views%2Fa.view.lkml", "views/a.view.lkml"),
        ("/projects/proj/files/files%2Fa.view.lkml", "files/a.view.lkml"),
        ("/projects/proj/files/", None),
        ("/explore/sales/orders", None),
        (None, None),
    ],
)
def test_lookml_file_path_from_link(
    lookml_link: Optional[str], expected: Optional[str]
) -> None:
    assert lookml_file_path_from_link(lookml_link) == expected


@pytest.mark.parametrize(
    "remote, expected",
    [
        ("git@github.com:example-org/lookml.git", REPO),
        ("org-123@github.com:example-org/lookml.git", REPO),
        ("git@GitHub.com:example-org/lookml.git", REPO),
        ("ssh://git@github.com:22/example-org/lookml.git", REPO),
        ("github.com:example-org/lookml.git", REPO),
        ("ssh://git@ssh.github.com:443/example-org/lookml.git", REPO),
        ("https://www.github.com/example-org/lookml", REPO),
        (
            "git@altssh.gitlab.com:group/lookml.git",
            "https://gitlab.com/group/lookml",
        ),
        ("https://user:secret-token@github.com/example-org/lookml.git", REPO),
        (
            "https://gitlab.com/group/sub/lookml.git",
            "https://gitlab.com/group/sub/lookml",
        ),
        # Malformed: no host, no owner/repo, or a shorthand we must not guess a host for.
        ("example-org/lookml", None),
        ("github.com/example-org/lookml", None),
        ("https://github.com/lookml", None),
        ("https://[broken/example-org/lookml", None),
        ("not a remote", None),
    ],
)
def test_https_repo_url_from_git_remote(remote: str, expected: Optional[str]) -> None:
    assert https_repo_url_from_git_remote(remote) == expected


def test_first_add(looker_source_factory: SourceFactory) -> None:
    source = with_graph(
        looker_source_factory(client_with(ORDERS_LINK), extract_explore_git_links=True)
    )
    patches = link_patches(source)
    assert sorted(p["entityUrn"] for p in patches) == sorted([ORDERS_URN, INVOICES_URN])
    graph = source.ctx.graph
    assert isinstance(graph, mock.MagicMock)
    graph.get_aspect.assert_any_call(ORDERS_URN, InstitutionalMemoryClass)
    assert ops(patches[0]) == [("add", path_for(ORDERS_URL))]
    value = patches[0]["patch"][0]["value"]
    created = value.pop("createStamp")
    assert value == {
        "url": ORDERS_URL,
        "description": LABEL,
        "settings": {"showInAssetPreview": True},
    }
    assert created["actor"] == "urn:li:corpuser:__ingestion"
    # Stamped with the time the link is added, not the epoch.
    assert created["time"] > 1_700_000_000_000


def test_each_project_gets_its_own_repo(looker_source_factory: SourceFactory) -> None:
    client = client_with(ORDERS_LINK)
    by_explore = client.lookml_model_explore.side_effect

    def explore_in_own_project(
        model: str,
        explore_name: str,
        fields: Optional[str] = None,
        transport_options: Optional[TransportOptions] = None,
    ) -> LookmlModelExplore:
        result = by_explore(model, explore_name)
        result.project_name = "proj" if explore_name == "orders" else "other"
        return result

    client.lookml_model_explore.side_effect = explore_in_own_project
    client.project.side_effect = (
        lambda project_id, fields=None, transport_options=None: Project(
            git_remote_url=f"git@github.com:example-org/{project_id}.git",
            git_production_branch_name="main",
        )
    )
    source = with_graph(looker_source_factory(client, extract_explore_git_links=True))

    urls = {p["entityUrn"]: p["patch"][0]["value"]["url"] for p in link_patches(source)}
    assert urls == {
        ORDERS_URN: "https://github.com/example-org/proj/blob/main/views/sales/orders.view.lkml",
        INVOICES_URN: "https://github.com/example-org/other/blob/main/views/sales/orders.view.lkml",
    }
    assert client.project.call_count == 2


def test_same_url_writes_nothing(looker_source_factory: SourceFactory) -> None:
    source = with_graph(
        looker_source_factory(client_with(ORDERS_LINK), extract_explore_git_links=True),
        link(ORDERS_URL, INGESTION_STAMP),
    )
    assert link_patches(source) == []


def test_changed_url_removes_old_and_adds_new_in_one_patch(
    looker_source_factory: SourceFactory,
) -> None:
    # The label alone marks a link as managed, so a hand-made one is replaced too.
    source = with_graph(
        looker_source_factory(client_with(ORDERS_LINK), extract_explore_git_links=True),
        link(OLD_URL, INGESTION_STAMP),
    )
    patch = link_patches(source)[0]
    assert ops(patch) == [
        ("remove", path_for(OLD_URL)),
        ("add", path_for(ORDERS_URL)),
    ]


def test_current_and_stale_ingestion_links_give_remove_only(
    looker_source_factory: SourceFactory,
) -> None:
    source = with_graph(
        looker_source_factory(client_with(ORDERS_LINK), extract_explore_git_links=True),
        link(ORDERS_URL, INGESTION_STAMP),
        link(OLD_URL, INGESTION_STAMP),
    )
    patch = link_patches(source)[0]
    assert ops(patch) == [("remove", path_for(OLD_URL))]


def test_missing_production_branch_falls_back_to_master(
    looker_source_factory: SourceFactory,
) -> None:
    client = client_with(
        ORDERS_LINK, Project(git_remote_url="git@github.com:example-org/lookml.git")
    )
    source = with_graph(looker_source_factory(client, extract_explore_git_links=True))
    patch = link_patches(source)[0]
    assert patch["patch"][0]["value"]["url"] == (
        f"{REPO}/blob/master/views/sales/orders.view.lkml"
    )


@pytest.mark.parametrize(
    "lookml_link, branch, expected_path",
    [
        (
            "/projects/proj/files/views%2Fmy%20orders%23v2.view.lkml",
            "main",
            "main/views/my%20orders%23v2.view.lkml",
        ),
        (ORDERS_LINK, "hotfix#12", "hotfix%2312/views/sales/orders.view.lkml"),
        (ORDERS_LINK, "release/2026", "release/2026/views/sales/orders.view.lkml"),
    ],
    ids=["file-path", "branch", "branch-with-slash"],
)
def test_link_is_url_encoded(
    looker_source_factory: SourceFactory,
    lookml_link: str,
    branch: str,
    expected_path: str,
) -> None:
    client = client_with(
        lookml_link,
        Project(
            git_remote_url="git@github.com:example-org/lookml.git",
            git_production_branch_name=branch,
        ),
    )
    source = with_graph(looker_source_factory(client, extract_explore_git_links=True))
    patch = link_patches(source)[0]
    assert patch["patch"][0]["value"]["url"] == f"{REPO}/blob/{expected_path}"


def test_link_with_another_label_is_left_alone(
    looker_source_factory: SourceFactory,
) -> None:
    source = with_graph(
        looker_source_factory(client_with(ORDERS_LINK), extract_explore_git_links=True),
        link(ORDERS_URL, USER_STAMP, label="Docs"),
    )
    patch = link_patches(source)[0]
    assert ops(patch) == [("add", path_for(ORDERS_URL))]


@pytest.mark.parametrize(
    "error",
    [
        requests.ConnectionError("GMS unreachable"),
        ValueError("aspect could not be decoded"),
    ],
    ids=["connection-error", "unreadable-response"],
)
def test_failed_link_read_skips_only_that_explore(
    looker_source_factory: SourceFactory, error: Exception
) -> None:
    source = with_graph(
        looker_source_factory(client_with(ORDERS_LINK), extract_explore_git_links=True)
    )
    graph = source.ctx.graph
    assert isinstance(graph, mock.MagicMock)

    def get_aspect(urn: str, aspect_type: Type[InstitutionalMemoryClass]) -> None:
        if urn == ORDERS_URN:
            raise error
        return None

    graph.get_aspect.side_effect = get_aspect

    workunits = list(source.get_workunits_internal())

    explore_urns = {str(wu.urn) for wu in workunits if isinstance(wu, Dataset)}
    assert {ORDERS_URN, INVOICES_URN} <= explore_urns
    linked = [
        wu.metadata.entityUrn
        for wu in workunits
        if isinstance(wu, MetadataWorkUnit)
        and isinstance(wu.metadata, MetadataChangeProposalClass)
        and wu.metadata.aspectName == "institutionalMemory"
    ]
    assert linked == [INVOICES_URN]
    assert [w.title for w in source.reporter.warnings].count(
        "Could not read an Explore's existing links"
    ) == 1


def test_no_datahub_connection_adds_only_and_notes_it_once(
    looker_source_factory: SourceFactory,
) -> None:
    source = looker_source_factory(
        client_with(ORDERS_LINK), extract_explore_git_links=True
    )
    assert source.ctx.graph is None
    patches = link_patches(source)
    assert [ops(p) for p in patches] == 2 * [[("add", path_for(ORDERS_URL))]]
    assert [i.title for i in source.reporter.infos].count(
        "Explore Git links are add-only"
    ) == 1


@pytest.mark.parametrize(
    "project_response, warning_title",
    [
        (SDKError("403 Forbidden"), "Failed to fetch Looker project"),
        (Project(git_remote_url=None), "Looker project has no Git remote"),
        (
            Project(git_remote_url="https://user:secret-token@[broken/lookml.git"),
            "Malformed Git remote for Explore links",
        ),
        (
            Project(git_remote_url="example-org/lookml"),
            "Malformed Git remote for Explore links",
        ),
        (
            Project(
                git_remote_url="user:secret-token@github.com/example-org/lookml.git"
            ),
            "Malformed Git remote for Explore links",
        ),
        (
            Project(
                git_remote_url="https://user:secret-token@bitbucket.org/example-org/lookml.git"
            ),
            "Unsupported Git host for Explore links",
        ),
    ],
)
def test_project_without_usable_remote_gets_no_link(
    looker_source_factory: SourceFactory,
    project_response: Union[Project, Exception],
    warning_title: str,
) -> None:
    client = client_with(ORDERS_LINK)
    if isinstance(project_response, Exception):
        client.project.side_effect = project_response
    else:
        client.project.return_value = project_response
    source = with_graph(
        looker_source_factory(client, extract_explore_git_links=True),
        link(OLD_URL, INGESTION_STAMP),
    )

    assert link_patches(source) == []
    assert any(w.title == warning_title for w in source.reporter.warnings)
    assert "secret-token" not in str(source.reporter.as_obj())
    # The failed lookup is cached: two explores, one project call.
    assert client.project.call_count == 1


def test_no_link_when_flag_is_off(looker_source_factory: SourceFactory) -> None:
    client = client_with(ORDERS_LINK)
    source = looker_source_factory(client)
    assert link_patches(source) == []
    client.project.assert_not_called()
    assert not any(
        i.title == "Explore Git links are add-only" for i in source.reporter.infos
    )


@pytest.mark.parametrize(
    "lookml_link",
    ["/projects/proj/files/imported_projects%2Fother%2Forders.view.lkml", None],
    ids=["imported-project", "no-lookml-file"],
)
def test_explore_without_own_lookml_file_keeps_existing_link(
    looker_source_factory: SourceFactory, lookml_link: Optional[str]
) -> None:
    client = client_with(lookml_link)
    source = with_graph(
        looker_source_factory(client, extract_explore_git_links=True),
        link(OLD_URL, INGESTION_STAMP),
    )
    assert link_patches(source) == []
    client.project.assert_not_called()
    graph = source.ctx.graph
    assert isinstance(graph, mock.MagicMock)
    graph.get_aspect.assert_not_called()
    # Imported projects are documented as linkless; a missing link is not.
    reported = any(
        i.title == "Explore has no LookML file link" for i in source.reporter.infos
    )
    assert reported == (lookml_link is None)
