import json
from typing import Any, Callable, Dict, List, Optional

import pytest
from looker_sdk.error import SDKError
from looker_sdk.sdk.api40.models import LookmlModelExplore, Project

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.looker.looker_common import lookml_file_path_from_link
from datahub.ingestion.source.looker.looker_source import LookerDashboardSource
from datahub.metadata.schema_classes import MetadataChangeProposalClass
from tests.unit.looker.test_looker_source import explore, fake_client

SourceFactory = Callable[..., LookerDashboardSource]

ORDERS_LINK = "/projects/proj/files/views%2Fsales%2Forders.view.lkml?line=12"
ORDERS_GITHUB_URL = (
    "https://github.com/example-org/lookml/blob/main/views/sales/orders.view.lkml"
)


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


def link_patches(source: LookerDashboardSource) -> List[Dict[str, Any]]:
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


def test_explore_gets_label_keyed_link_and_project_is_fetched_once(
    looker_source_factory: SourceFactory,
) -> None:
    client = client_with(ORDERS_LINK)
    source = looker_source_factory(client, emit_explore_git_links=True)

    patches = link_patches(source)

    # Both explores of the fake model ("orders" and "unused") get the link.
    assert patches == 2 * [
        {
            "arrayPrimaryKeys": {"elements": ["description"]},
            "patch": [
                {
                    "op": "add",
                    "path": "/elements/View in GitHub",
                    "value": {
                        "url": ORDERS_GITHUB_URL,
                        "description": "View in GitHub",
                        "createStamp": {"time": 0, "actor": "urn:li:corpuser:datahub"},
                        "settings": {"showInAssetPreview": True},
                    },
                }
            ],
        }
    ]
    # Explores of the same project share one cached project lookup.
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
    source = looker_source_factory(client, emit_explore_git_links=True)
    assert link_patches(source) == []
    client.project.assert_not_called()


@pytest.mark.parametrize(
    "remote, branch, expected_url",
    [
        (
            "https://user:secret-token@github.com/example-org/lookml.git",
            "main",
            ORDERS_GITHUB_URL,
        ),
        (
            "ssh://git@gitlab.com/example-org/lookml.git",
            "prod",
            "https://gitlab.com/example-org/lookml/-/blob/prod/views/sales/orders.view.lkml",
        ),
        (
            "git@github.com:example-org/lookml.git",
            None,
            "https://github.com/example-org/lookml/blob/master/views/sales/orders.view.lkml",
        ),
        ("ssh://git@github.com:22/example-org/lookml.git", "main", ORDERS_GITHUB_URL),
        ("org-123@github.com:example-org/lookml.git", "main", ORDERS_GITHUB_URL),
    ],
)
def test_remote_url_forms(
    looker_source_factory: SourceFactory,
    remote: str,
    branch: Optional[str],
    expected_url: str,
) -> None:
    client = client_with(
        ORDERS_LINK,
        Project(git_remote_url=remote, git_production_branch_name=branch),
    )
    source = looker_source_factory(client, emit_explore_git_links=True)
    patch = link_patches(source)[0]
    assert patch["patch"][0]["value"]["url"] == expected_url
    assert "secret-token" not in json.dumps(patch)


@pytest.mark.parametrize(
    "project_response, warning_title",
    [
        (SDKError("403 Forbidden"), "Failed to fetch Looker project"),
        (Project(git_remote_url=None), "Looker project has no Git remote"),
        (
            Project(
                git_remote_url="https://user:secret-token@bitbucket.org/example-org/lookml.git"
            ),
            "Unsupported Git host for Explore links",
        ),
    ],
)
def test_project_without_usable_remote_is_reported(
    looker_source_factory: SourceFactory,
    project_response: Any,
    warning_title: str,
) -> None:
    client = client_with(ORDERS_LINK)
    if isinstance(project_response, Exception):
        client.project.side_effect = project_response
    else:
        client.project.return_value = project_response
    source = looker_source_factory(client, emit_explore_git_links=True)

    assert link_patches(source) == []
    assert any(w.title == warning_title for w in source.reporter.warnings)
    assert "secret-token" not in str(source.reporter.as_obj())
    # The failed lookup is cached: two explores, one project call.
    assert client.project.call_count == 1
