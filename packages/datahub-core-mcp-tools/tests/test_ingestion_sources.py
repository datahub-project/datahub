"""list_ingestion_sources, create_ingestion_source and update_ingestion_source.

Reads go through a test ReadBackend; writes reach a mocked graph.
"""

import json
from typing import Iterator, Optional, cast
from unittest.mock import MagicMock

import pytest
import requests
from datahub.configuration.common import GraphError
from fastmcp.exceptions import ToolError
from pydantic import JsonValue

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.read_backend import ReadBackend, ReadBackendBinding
from datahub_core_mcp_tools.tool_context import ToolContext
from datahub_core_mcp_tools.tools.ingestion_sources import (
    create_ingestion_source,
    list_ingestion_sources,
    update_ingestion_source,
)

JsonObject = dict[str, JsonValue]
SOURCE_URN = "urn:li:dataHubIngestionSource:my-source"
EXECUTION_URN = "urn:li:dataHubExecutionRequest:abc-123"
RECIPE = """
source:
  type: postgres
  config:
    host_port: db:5432
    username: reader
    password: ${PG_PASSWORD}
"""


class _Sources:
    def __init__(self) -> None:
        self.sources: dict[str, JsonObject] = {}
        self.listed: list[JsonObject] = []
        self.list_inputs: list[JsonObject] = []

    def ingestion_source(self, urn: str) -> Optional[JsonObject]:
        return self.sources.get(urn)

    def ingestion_sources(self, variables: JsonObject) -> list[JsonObject]:
        self.list_inputs.append(variables)
        return self.listed


@pytest.fixture
def backend() -> _Sources:
    return _Sources()


@pytest.fixture
def graph(backend: _Sources) -> Iterator[MagicMock]:
    client = MagicMock()
    context = ToolContext([ReadBackendBinding(cast(ReadBackend, backend))])
    with with_datahub_client(client, context):
        yield client._graph


def _stored(recipe: JsonObject, **fields: JsonValue) -> JsonObject:
    return {
        "urn": SOURCE_URN,
        "name": "pg",
        "type": "postgres",
        "config": {"recipe": json.dumps(recipe), "executorId": "default"},
        **fields,
    }


def _write(graph: MagicMock, operation: str) -> dict:
    assert graph.execute_graphql.call_count == 1
    kwargs = graph.execute_graphql.call_args.kwargs
    assert kwargs["operation_name"] == operation
    return kwargs["variables"]


def test_create_saves_the_recipe_as_json(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {"createIngestionSource": SOURCE_URN}

    result = create_ingestion_source(name=" pg ", recipe=RECIPE, schedule="0 * * * *")

    assert result == {"urn": SOURCE_URN, "name": "pg", "type": "postgres"}
    source_input = _write(graph, "createIngestionSource")["input"]
    assert (
        json.loads(source_input["config"]["recipe"])["source"]["config"]["password"]
        == "${PG_PASSWORD}"
    )
    assert source_input["config"]["executorId"] == "default"
    assert source_input["schedule"] == {"interval": "0 * * * *", "timezone": "UTC"}


@pytest.mark.parametrize(
    ("recipe", "error"),
    [
        ("source:\n  type: postgres\n  config:\n    password: pw\n", "literal"),
        ("source:\n  config: {}\n", "source.type"),
    ],
)
def test_create_refuses_a_bad_recipe(graph: MagicMock, recipe: str, error: str) -> None:
    with pytest.raises(ToolError, match=error):
        create_ingestion_source(name="pg", recipe=recipe)
    graph.execute_graphql.assert_not_called()


def test_create_never_saves_run_options(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {"createIngestionSource": SOURCE_URN}

    result = create_ingestion_source(
        name="pg", recipe=RECIPE + "__run_options__:\n  dry_run: true\n"
    )

    recipe = _write(graph, "createIngestionSource")["input"]["config"]["recipe"]
    assert "__run_options__" not in json.loads(recipe)
    assert result["dropped_keys"] == ["__run_options__"]


def test_create_with_no_reply_says_how_to_check(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = requests.ReadTimeout("read timed out")

    with pytest.raises(ToolError, match="may or may not") as e:
        create_ingestion_source(name="pg", recipe=RECIPE)
    assert 'list_ingestion_sources(query="pg")' in str(e.value)


def test_list_returns_each_source_with_its_latest_run(
    graph: MagicMock, backend: _Sources
) -> None:
    backend.listed = [
        {
            "urn": SOURCE_URN,
            "name": "pg",
            "type": "postgres",
            "schedule": None,
            "executions": {
                "executionRequests": [
                    {"urn": EXECUTION_URN, "result": {"status": "SUCCESS"}}
                ]
            },
        },
        {"urn": "urn:li:dataHubIngestionSource:new", "name": "new", "type": "file"},
    ]

    result = list_ingestion_sources(query="pg", count=1000)

    assert backend.list_inputs == [{"start": 0, "count": 100, "query": "pg"}]
    assert result["count"] == 2
    assert result["sources"][0]["latest_run"] == {
        "execution_urn": EXECUTION_URN,
        "status": "SUCCESS",
    }
    assert result["sources"][1]["latest_run"] is None


def test_one_source_has_its_recipe_parsed_and_literal_secrets_masked(
    graph: MagicMock, backend: _Sources
) -> None:
    backend.sources[SOURCE_URN] = _stored(
        {"source": {"type": "postgres", "config": {"password": "pw", "user": "u"}}}
    )

    result = list_ingestion_sources(source_urn=SOURCE_URN)

    assert result["config"]["recipe"] == {
        "source": {"type": "postgres", "config": {"password": "********", "user": "u"}}
    }
    with pytest.raises(ToolError, match="No ingestion source exists"):
        list_ingestion_sources(source_urn="urn:li:dataHubIngestionSource:gone")
    with pytest.raises(ToolError, match="not an ingestion source URN"):
        list_ingestion_sources(source_urn="urn:li:dataset:x")


def test_update_merges_into_the_stored_recipe_and_keeps_its_settings(
    graph: MagicMock, backend: _Sources
) -> None:
    backend.sources[SOURCE_URN] = _stored(
        {
            "source": {
                "type": "postgres",
                "config": {
                    "password": "${PG_PASSWORD}",
                    "profiling": {"enabled": False},
                    "include_views": True,
                },
            }
        },
        source={"type": "MANUAL"},
        schedule={"interval": "0 * * * *", "timezone": "Europe/Paris"},
    )
    stored_config = backend.sources[SOURCE_URN]["config"]
    assert isinstance(stored_config, dict)
    stored_config.update(
        {
            "executorId": "remote",
            "version": "1.6.0",
            "debugMode": True,
            "extraArgs": [{"key": "extra_pip_requirements", "value": '["x"]'}],
        }
    )
    graph.execute_graphql.return_value = {"updateIngestionSource": SOURCE_URN}

    result = update_ingestion_source(
        SOURCE_URN,
        config_updates={"profiling.enabled": True},
        remove_fields=["include_views", "typo"],
        schedule="0 6 * * *",
    )

    assert result == {
        "urn": SOURCE_URN,
        "applied": {"profiling.enabled": True},
        "removed": ["include_views"],
        "not_found": ["typo"],
    }
    variables = _write(graph, "updateIngestionSource")
    source_input = variables["input"]
    assert json.loads(source_input["config"].pop("recipe")) == {
        "source": {
            "type": "postgres",
            "config": {"password": "${PG_PASSWORD}", "profiling": {"enabled": True}},
        }
    }
    assert source_input == {
        "name": "pg",
        "type": "postgres",
        "config": {
            "executorId": "remote",
            "version": "1.6.0",
            "debugMode": True,
            "extraArgs": [{"key": "extra_pip_requirements", "value": '["x"]'}],
        },
        "source": {"type": "MANUAL"},
        "schedule": {"interval": "0 6 * * *", "timezone": "Europe/Paris"},
    }


def test_update_refuses_literal_credentials_and_unknown_sources(
    graph: MagicMock, backend: _Sources
) -> None:
    with pytest.raises(ToolError, match="literal credentials"):
        update_ingestion_source(SOURCE_URN, config_updates={"password": "pw"})
    with pytest.raises(ToolError, match="No ingestion source exists"):
        update_ingestion_source(SOURCE_URN, name="renamed")
    with pytest.raises(ToolError, match="Nothing to change"):
        update_ingestion_source(SOURCE_URN)
    graph.execute_graphql.assert_not_called()


def test_a_refused_update_reports_gms_reason(
    graph: MagicMock, backend: _Sources
) -> None:
    backend.sources[SOURCE_URN] = _stored({"source": {"type": "postgres"}})
    graph.execute_graphql.side_effect = GraphError(
        [{"message": "Unauthorized to perform this action."}]
    )

    with pytest.raises(ToolError, match="rejected updateIngestionSource: Unauth"):
        update_ingestion_source(SOURCE_URN, name="renamed")
