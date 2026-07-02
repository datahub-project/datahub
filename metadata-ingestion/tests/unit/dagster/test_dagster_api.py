from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.dagster.config import DagsterSourceConfig
from datahub.ingestion.source.dagster.dagster_api import (
    DagsterGraphQLClient,
    DagsterGraphQLError,
)

_ASSET_NODE = {
    "assetKey": {"path": ["my_db", "events"]},
    "groupName": "analytics",
    "opNames": ["events"],
    "jobNames": ["my_job"],
    "description": "Events.",
    "computeKind": "python",
    "kinds": ["python"],
    "dependencyKeys": [{"path": ["my_db", "raw"]}],
    "dependedByKeys": [],
    "owners": [
        {"__typename": "UserAssetOwner", "email": "a@example.com"},
        {"__typename": "TeamAssetOwner", "team": "platform"},
    ],
    "tags": [{"key": "tier", "value": "gold"}, {"key": "pii", "value": ""}],
    "metadataEntries": [
        {"__typename": "MarkdownMetadataEntry", "label": "docs", "mdStr": "# Docs"},
        {
            "__typename": "UrlMetadataEntry",
            "label": "runbook",
            "url": "https://example.com/rb",
        },
        {
            "__typename": "CodeReferencesMetadataEntry",
            "label": "source",
            "codeReferences": [
                {"__typename": "UrlCodeReference", "url": "https://git/x.py"}
            ],
        },
        {
            "__typename": "TableSchemaMetadataEntry",
            "label": "schema",
            "schema": {
                "columns": [
                    {
                        "name": "id",
                        "type": "int",
                        "description": None,
                        "constraints": {"nullable": False},
                    }
                ]
            },
        },
        {"__typename": "IntMetadataEntry", "label": "row_count", "intRepr": "42"},
        {
            "__typename": "TableColumnLineageMetadataEntry",
            "label": "column_lineage",
            "lineage": [
                {
                    "columnName": "id",
                    "columnDeps": [
                        {"assetKey": {"path": ["my_db", "raw"]}, "columnName": "id"}
                    ],
                }
            ],
        },
    ],
}

_REPOS_RESPONSE = {
    "repositoriesOrError": {
        "__typename": "RepositoryConnection",
        "nodes": [
            {
                "name": "my_repo",
                "location": {"name": "my_location"},
                "pipelines": [
                    {
                        "name": "my_job",
                        "description": "A job.",
                        "tags": [{"key": "schedule", "value": "daily"}],
                        "owners": [
                            {
                                "__typename": "UserDefinitionOwner",
                                "email": "a@example.com",
                            }
                        ],
                    }
                ],
            }
        ],
    }
}


def _assets_response(
    nodes: list, cursor: object = None, has_more: bool = False
) -> dict:
    return {
        "repositoryOrError": {
            "__typename": "Repository",
            "assetNodesConnection": {
                "cursor": cursor,
                "hasMore": has_more,
                "nodes": nodes,
            },
        }
    }


def _client() -> DagsterGraphQLClient:
    config = DagsterSourceConfig.parse_obj({"host": "http://localhost:3000"})
    return DagsterGraphQLClient(config)


def test_endpoint_oss_vs_cloud() -> None:
    oss = DagsterSourceConfig.parse_obj({"host": "http://localhost:3000"})
    assert DagsterGraphQLClient._build_endpoint(oss) == "http://localhost:3000/graphql"
    cloud = DagsterSourceConfig.parse_obj(
        {
            "host": "https://org.dagster.cloud",
            "is_cloud": True,
            "deployment": "prod",
            "token": "t",
        }
    )
    assert (
        DagsterGraphQLClient._build_endpoint(cloud)
        == "https://org.dagster.cloud/prod/graphql"
    )


def test_get_repositories_parses_full_payload() -> None:
    client = _client()

    def side_effect(query: str, variables: Any = None) -> dict:
        if "repositoriesOrError" in query:
            return _REPOS_RESPONSE
        return _assets_response([_ASSET_NODE])

    with patch.object(client, "_execute", side_effect=side_effect):
        repos = client.get_repositories()

    assert len(repos) == 1
    repo = repos[0]
    assert repo.name == "my_repo"
    assert repo.location_name == "my_location"
    assert repo.jobs[0].description == "A job."
    assert repo.jobs[0].owners[0].email == "a@example.com"

    asset = repo.assets[0]
    assert asset.key == ["my_db", "events"]
    assert asset.upstream_keys == [["my_db", "raw"]]
    assert {o.email or o.team for o in asset.owners} == {"a@example.com", "platform"}
    # value-less tag is normalized to None
    assert any(t.key == "pii" and t.value is None for t in asset.tags)

    meta = asset.metadata
    assert meta.custom_properties["docs"] == "# Docs"
    assert meta.custom_properties["row_count"] == "42"
    assert {link.url for link in meta.links} == {
        "https://example.com/rb",
        "https://git/x.py",
    }
    assert meta.columns is not None
    assert meta.columns[0].name == "id"
    assert meta.columns[0].nullable is False

    assert len(meta.column_lineage) == 1
    cl = meta.column_lineage[0]
    assert cl.downstream_column == "id"
    assert cl.upstreams[0].asset_key == ["my_db", "raw"]
    assert cl.upstreams[0].column == "id"


def test_asset_pagination_follows_cursor() -> None:
    client = _client()
    page1 = _assets_response([_ASSET_NODE], cursor="next", has_more=True)
    second = {**_ASSET_NODE, "assetKey": {"path": ["my_db", "more"]}}
    page2 = _assets_response([second], cursor=None, has_more=False)
    calls = {"asset": 0}

    def side_effect(query: str, variables: Any = None) -> dict:
        if "repositoriesOrError" in query:
            return _REPOS_RESPONSE
        calls["asset"] += 1
        return page1 if calls["asset"] == 1 else page2

    with patch.object(client, "_execute", side_effect=side_effect):
        repos = client.get_repositories()

    assert calls["asset"] == 2  # followed hasMore=True to a second page
    assert [a.key for a in repos[0].assets] == [["my_db", "events"], ["my_db", "more"]]


def test_python_error_in_repositories_raises() -> None:
    client = _client()
    payload = {"repositoriesOrError": {"__typename": "PythonError", "message": "boom"}}
    with patch.object(client, "_execute", return_value=payload):
        with pytest.raises(DagsterGraphQLError):
            client.get_repositories()


def test_unexpected_repositories_type_raises() -> None:
    client = _client()
    payload = {"repositoriesOrError": {"__typename": "SomeFutureType"}}
    with patch.object(client, "_execute", return_value=payload):
        with pytest.raises(DagsterGraphQLError):
            client.get_repositories()


def test_python_error_in_assets_raises() -> None:
    client = _client()

    def side_effect(query: str, variables: Any = None) -> dict:
        if "repositoriesOrError" in query:
            return _REPOS_RESPONSE
        return {"repositoryOrError": {"__typename": "PythonError", "message": "boom"}}

    with patch.object(client, "_execute", side_effect=side_effect):
        with pytest.raises(DagsterGraphQLError):
            client.get_repositories()


def test_unexpected_assets_type_raises() -> None:
    client = _client()

    def side_effect(query: str, variables: Any = None) -> dict:
        if "repositoriesOrError" in query:
            return _REPOS_RESPONSE
        return {"repositoryOrError": {"__typename": "SomeFutureType"}}

    with patch.object(client, "_execute", side_effect=side_effect):
        with pytest.raises(DagsterGraphQLError):
            client.get_repositories()


def test_execute_raises_on_graphql_errors() -> None:
    client = _client()
    fake_response = MagicMock()
    fake_response.json.return_value = {"errors": [{"message": "bad field"}]}
    fake_response.raise_for_status.return_value = None
    with patch.object(client.session, "post", return_value=fake_response):
        with pytest.raises(DagsterGraphQLError):
            client._execute("query { version }")


def test_connection_success_and_failure() -> None:
    client = _client()
    with patch.object(client, "_execute", return_value={"version": "1.0"}):
        client.test_connection()  # should not raise

    with patch.object(client, "_execute", side_effect=DagsterGraphQLError("bad token")):
        with pytest.raises(DagsterGraphQLError):
            client.test_connection()
