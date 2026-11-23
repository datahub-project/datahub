import json
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest import mock

import pytest
from looker_sdk.error import SDKError
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
from tests.unit.looker.test_looker_source import explore, fake_client

SourceFactory = Callable[..., LookerDashboardSource]

ORDERS_LINK = "/projects/proj/files/views%2Fsales%2Forders.view.lkml?line=12"
REPO = "https://github.com/example-org/lookml"
ORDERS_URL = f"{REPO}/blob/main/views/sales/orders.view.lkml"
OLD_URL = f"{REPO}/blob/main/views/old/orders.view.lkml"
LABEL = "View in Git"
INGESTION_STAMP = AuditStampClass(time=0, actor="urn:li:corpuser:__ingestion")
USER_STAMP = AuditStampClass(time=1700000000000, actor="urn:li:corpuser:jdoe")


def client_with(lookml_link: Optional[str], project: Optional[Project] = None) -> Any:
    client = fake_client()

    def explore_with_link(
        model: str, explore_name: str, fields: Any = None, transport_options: Any = None
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
            patches.append(json.loads(mcp.aspect.value))
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
    # Both explores of the fake model ("orders" and "unused") get the link.
    assert len(patches) == 2
    assert ops(patches[0]) == [("add", path_for(ORDERS_URL))]
    assert patches[0]["patch"][0]["value"] == {
        "url": ORDERS_URL,
        "description": LABEL,
        "createStamp": {"time": 0, "actor": "urn:li:corpuser:__ingestion"},
        "settings": {"showInAssetPreview": True},
    }


def test_same_url_writes_nothing(looker_source_factory: SourceFactory) -> None:
    source = with_graph(
        looker_source_factory(client_with(ORDERS_LINK), extract_explore_git_links=True),
        link(ORDERS_URL, INGESTION_STAMP),
    )
    assert link_patches(source) == []


def test_changed_url_removes_old_and_adds_new_in_one_patch(
    looker_source_factory: SourceFactory,
) -> None:
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


def test_hand_made_link_at_same_url_and_label_is_not_overwritten(
    looker_source_factory: SourceFactory,
) -> None:
    source = with_graph(
        looker_source_factory(client_with(ORDERS_LINK), extract_explore_git_links=True),
        link(ORDERS_URL, USER_STAMP),
    )
    assert link_patches(source) == []


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


def test_file_path_is_url_encoded_in_link(looker_source_factory: SourceFactory) -> None:
    client = client_with("/projects/proj/files/views%2Fmy%20orders%23v2.view.lkml")
    source = with_graph(looker_source_factory(client, extract_explore_git_links=True))
    patch = link_patches(source)[0]
    assert patch["patch"][0]["value"]["url"] == (
        f"{REPO}/blob/main/views/my%20orders%23v2.view.lkml"
    )


def test_any_link_with_the_label_is_managed(
    looker_source_factory: SourceFactory,
) -> None:
    """The label alone marks a link as managed, so a hand-made one at another URL is replaced."""
    source = with_graph(
        looker_source_factory(client_with(ORDERS_LINK), extract_explore_git_links=True),
        link(OLD_URL, USER_STAMP),
    )
    patch = link_patches(source)[0]
    assert ops(patch) == [
        ("remove", path_for(OLD_URL)),
        ("add", path_for(ORDERS_URL)),
    ]


def test_link_with_another_label_is_left_alone(
    looker_source_factory: SourceFactory,
) -> None:
    source = with_graph(
        looker_source_factory(client_with(ORDERS_LINK), extract_explore_git_links=True),
        link(ORDERS_URL, USER_STAMP, label="Docs"),
    )
    patch = link_patches(source)[0]
    assert ops(patch) == [("add", path_for(ORDERS_URL))]


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
                git_remote_url="https://user:secret-token@bitbucket.org/example-org/lookml.git"
            ),
            "Unsupported Git host for Explore links",
        ),
    ],
)
def test_project_without_usable_remote_gets_no_link(
    looker_source_factory: SourceFactory,
    project_response: Any,
    warning_title: str,
) -> None:
    client = client_with(ORDERS_LINK)
    if isinstance(project_response, Exception):
        client.project.side_effect = project_response
    else:
        client.project.return_value = project_response
    source = with_graph(looker_source_factory(client, extract_explore_git_links=True))

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


def test_no_link_for_explore_from_imported_project(
    looker_source_factory: SourceFactory,
) -> None:
    client = client_with(
        "/projects/proj/files/imported_projects%2Fother%2Forders.view.lkml"
    )
    source = looker_source_factory(client, extract_explore_git_links=True)
    assert link_patches(source) == []
    client.project.assert_not_called()
