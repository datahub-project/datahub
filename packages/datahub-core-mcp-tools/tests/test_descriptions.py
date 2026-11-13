"""update_description."""

from typing import Iterator, cast
from unittest.mock import MagicMock

import pytest
import requests
from datahub.configuration.common import GraphError
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.mcp_server import register_tools
from datahub_core_mcp_tools.read_backend import ReadBackend, ReadBackendBinding
from datahub_core_mcp_tools.tool_context import ToolContext
from datahub_core_mcp_tools.tool_groups import groups_for_tags
from datahub_core_mcp_tools.tools.descriptions import update_description

USERS = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
CONTAINER = "urn:li:container:12345"
CHART = "urn:li:chart:(looker,sales_chart)"
TERM = "urn:li:glossaryTerm:Revenue"
DBT = "urn:li:dataset:(urn:li:dataPlatform:dbt,db.schema.users,PROD)"
V2_EMAIL = "[version=2.0].[type=string].email"


@pytest.fixture
def graph() -> Iterator[MagicMock]:
    client = MagicMock()
    with with_datahub_client(client):
        yield client._graph


def _write(graph: MagicMock) -> dict:
    return graph.execute_graphql.call_args_list[-1].kwargs


def _sources(entity: dict | None) -> dict:
    return {"entity": entity}


def test_replace_an_asset_description(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {"updateDescription": True}

    result = update_description(
        entity_urn=CONTAINER, description="Production warehouse"
    )

    assert result == {
        "success": True,
        "urn": CONTAINER,
        "column_path": None,
        "message": "Description updated successfully",
    }
    assert graph.execute_graphql.call_count == 1
    assert _write(graph)["variables"] == {
        "input": {"description": "Production warehouse", "resourceUrn": CONTAINER}
    }


def _dataset_sources(*paths: str, siblings: dict | None = None) -> dict:
    """A dataset as getDescriptionSources reads it."""
    return {
        "type": "DATASET",
        "siblings": siblings,
        "schemaMetadata": {
            "fields": [{"fieldPath": path, "description": None} for path in paths]
        },
    }


def _aspects(*paths: str) -> dict:
    """Raw aspects, as GraphQLReadBackend.aspects reads a column write's schema."""
    return {"schemaMetadata": {"fields": [{"fieldPath": path} for path in paths]}}


def test_replace_a_column_description(graph: MagicMock) -> None:
    graph.get_entity_semityped.return_value = _aspects("id", "email")
    graph.execute_graphql.return_value = {"updateDescription": True}

    result = update_description(
        entity_urn=USERS, description="Primary email", column_path=" email "
    )

    assert result["column_path"] == "email"
    assert graph.execute_graphql.call_count == 1
    assert _write(graph)["variables"]["input"] == {
        "description": "Primary email",
        "resourceUrn": USERS,
        "subResource": "email",
        "subResourceType": "DATASET_FIELD",
    }


@pytest.mark.parametrize("operation", ["replace", "remove"])
def test_a_column_name_is_written_under_its_v2_field_path(
    graph: MagicMock, operation: str
) -> None:
    graph.get_entity_semityped.return_value = _aspects(V2_EMAIL)
    graph.execute_graphql.return_value = {"updateDescription": True}

    result = update_description(
        entity_urn=USERS,
        operation=operation,  # type: ignore[arg-type]
        description="Primary email",
        column_path="email",
    )

    assert result["column_path"] == V2_EMAIL
    assert _write(graph)["variables"]["input"]["subResource"] == V2_EMAIL


def test_a_column_not_in_the_schema_writes_nothing(graph: MagicMock) -> None:
    graph.get_entity_semityped.return_value = _aspects("id")

    with pytest.raises(ToolError, match="not in the schema.*list_schema_fields"):
        update_description(entity_urn=USERS, description="x", column_path="email")

    graph.execute_graphql.assert_not_called()


def test_remove_writes_an_empty_description(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {"updateDescription": True}

    result = update_description(
        entity_urn=USERS, operation="remove", description="ignored"
    )

    assert result["message"] == "Description removed successfully"
    assert _write(graph)["variables"]["input"]["description"] == ""


def test_types_updatedescription_refuses_use_their_own_mutation(
    graph: MagicMock,
) -> None:
    graph.execute_graphql.return_value = {"updateChart": {"urn": CHART}}

    update_description(entity_urn=CHART, description="Weekly sales")

    assert _write(graph)["operation_name"] == "updateChartDescription"
    assert _write(graph)["variables"] == {"urn": CHART, "description": "Weekly sales"}


@pytest.mark.parametrize(
    ("entity", "expected"),
    [
        # The description edited in DataHub is the one shown.
        (
            {
                "type": "DATASET",
                "editableProperties": {"description": "Edited."},
                "properties": {"description": "From dbt."},
            },
            "Edited. (PII)",
        ),
        # Without one, the source system's is shown and is what is extended.
        (
            {
                "type": "DATASET",
                "editableProperties": None,
                "properties": {"description": "From dbt."},
            },
            "From dbt. (PII)",
        ),
        (
            {"type": "GLOSSARY_TERM", "properties": {"description": "Money in."}},
            "Money in. (PII)",
        ),
        # GraphQL answers an unset aspect with null (legacy OSS e0507ea).
        ({"type": "DATASET", "editableProperties": None, "properties": None}, " (PII)"),
    ],
)
def test_append_extends_the_description_datahub_shows(
    graph: MagicMock, entity: dict, expected: str
) -> None:
    graph.execute_graphql.side_effect = [_sources(entity), {"updateDescription": True}]

    update_description(entity_urn=USERS, operation="append", description=" (PII)")

    assert graph.execute_graphql.call_args_list[0].kwargs["operation_name"] == (
        "getDescriptionSources"
    )
    assert _write(graph)["variables"]["input"]["description"] == expected


@pytest.mark.parametrize(
    ("edited", "expected"),
    [
        (
            [{"fieldPath": "email", "description": "Edited email."}],
            "Edited email. (PII)",
        ),
        (None, "Email from source. (PII)"),
        ([{"fieldPath": "email", "description": None}], "Email from source. (PII)"),
    ],
)
def test_append_to_a_column(
    graph: MagicMock, edited: list | None, expected: str
) -> None:
    entity = {
        "type": "DATASET",
        "editableProperties": None,
        "properties": None,
        "schemaMetadata": {
            "fields": [
                {"fieldPath": "id", "description": "Key."},
                {"fieldPath": "email", "description": "Email from source."},
            ]
        },
        "editableSchemaMetadata": (
            None if edited is None else {"editableSchemaFieldInfo": edited}
        ),
    }
    graph.execute_graphql.side_effect = [_sources(entity), {"updateDescription": True}]

    update_description(
        entity_urn=USERS, operation="append", description=" (PII)", column_path="email"
    )

    assert _write(graph)["variables"]["input"]["description"] == expected


def test_append_to_a_column_by_name_in_a_v2_dataset_sources(graph: MagicMock) -> None:
    entity = _dataset_sources("[version=2.0].[type=long].id", V2_EMAIL)
    entity["editableSchemaMetadata"] = {
        "editableSchemaFieldInfo": [{"fieldPath": V2_EMAIL, "description": "Email."}]
    }
    graph.execute_graphql.side_effect = [_sources(entity), {"updateDescription": True}]

    update_description(
        entity_urn=USERS, operation="append", description=" (PII)", column_path="email"
    )

    assert _write(graph)["variables"]["input"]["subResource"] == V2_EMAIL
    assert _write(graph)["variables"]["input"]["description"] == "Email. (PII)"


def _sibling_search(*siblings: dict) -> dict:
    return {
        "entity": {
            "urn": USERS,
            "siblingsSearch": {"searchResults": [{"entity": s} for s in siblings]},
        }
    }


def _dbt_model() -> dict:
    return {
        "urn": DBT,
        "siblings": {"isPrimary": True},
        "properties": {"description": "Users, from dbt."},
        "editableProperties": None,
        "schemaMetadata": {
            "fields": [{"fieldPath": "email", "description": "Email, from dbt."}]
        },
        "editableSchemaMetadata": None,
    }


@pytest.mark.parametrize(
    ("column", "expected"),
    [(None, "Users, from dbt. (PII)"), ("email", "Email, from dbt. (PII)")],
)
def test_append_extends_the_description_a_sibling_shows(
    graph: MagicMock, column: str | None, expected: str
) -> None:
    # The warehouse table has no docs of its own; DataHub shows its dbt model's.
    # Appending to the empty local text would write just the suffix, which, as
    # the edited description, would then hide the dbt text.
    warehouse = _dataset_sources(V2_EMAIL, siblings={"isPrimary": False})
    warehouse.update(editableProperties=None, properties=None)
    graph.execute_graphql.side_effect = [
        _sources(warehouse),
        _sibling_search(_dbt_model()),
        {"updateDescription": True},
    ]

    update_description(
        entity_urn=USERS, operation="append", description=" (PII)", column_path=column
    )

    operations = [
        c.kwargs["operation_name"] for c in graph.execute_graphql.call_args_list
    ]
    assert operations == [
        "getDescriptionSources",
        "GetEntitySiblings",
        "updateDescription",
    ]
    assert _write(graph)["variables"]["input"]["description"] == expected


def test_append_without_sibling_merge_reads_the_entity_alone(
    graph: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SIBLING_MERGE_ENABLED", "false")
    warehouse = _dataset_sources(siblings={"isPrimary": False})
    warehouse.update(editableProperties=None, properties={"description": "Own."})
    graph.execute_graphql.side_effect = [
        _sources(warehouse),
        {"updateDescription": True},
    ]

    update_description(entity_urn=USERS, operation="append", description=" (PII)")

    assert graph.execute_graphql.call_count == 2
    assert _write(graph)["variables"]["input"]["description"] == "Own. (PII)"


def test_a_failed_sibling_read_never_becomes_a_partial_append(
    graph: MagicMock,
) -> None:
    graph.execute_graphql.side_effect = [
        _sources(_dataset_sources(siblings={"isPrimary": False})),
        requests.ConnectionError("reset"),
    ]

    with pytest.raises(ToolError, match="could not read .* nothing was appended"):
        update_description(entity_urn=USERS, operation="append", description=" (PII)")

    assert graph.execute_graphql.call_count == 2


@pytest.mark.parametrize(
    ("entity", "error"),
    [
        (None, "does not exist"),
        ({"type": "NOTEBOOK"}, "appending is not supported for NOTEBOOK"),
        # A dataset with no schema (null schemaMetadata) has no such column.
        (
            {"type": "DATASET", "editableProperties": None, "schemaMetadata": None},
            "not in the schema",
        ),
    ],
)
def test_append_that_cannot_read_the_description_writes_nothing(
    graph: MagicMock, entity: dict | None, error: str
) -> None:
    graph.execute_graphql.return_value = _sources(entity)

    with pytest.raises(ToolError, match=error):
        update_description(
            entity_urn=USERS,
            operation="append",
            description=" (PII)",
            column_path="email" if entity and entity["type"] == "DATASET" else None,
        )

    assert graph.execute_graphql.call_count == 1


def test_a_failed_read_never_becomes_a_replace(graph: MagicMock) -> None:
    # Treating an unreadable description as empty would replace it with the
    # appended text alone.
    graph.execute_graphql.side_effect = requests.ConnectionError("reset")

    with pytest.raises(ToolError, match="could not read .* nothing was appended"):
        update_description(entity_urn=USERS, operation="append", description=" (PII)")

    assert graph.execute_graphql.call_count == 1


def test_append_reads_through_the_bound_backend() -> None:
    client = MagicMock()
    client._graph.execute_graphql.return_value = {"updateDescription": True}
    backend = MagicMock(spec=["description_sources"])
    backend.description_sources.return_value = {
        "type": "TAG",
        "properties": {"description": "Personal data."},
    }

    with with_datahub_client(
        client,
        tool_context=ToolContext([ReadBackendBinding(cast(ReadBackend, backend))]),
    ):
        update_description(
            entity_urn="urn:li:tag:PII", operation="append", description=" Restricted."
        )

    backend.description_sources.assert_called_once_with("urn:li:tag:PII")
    assert _write(client._graph)["variables"]["input"]["description"] == (
        "Personal data. Restricted."
    )


@pytest.mark.parametrize(
    ("urn", "operation", "description", "column", "error"),
    [
        ("users", "replace", "x", None, "must be a DataHub URN"),
        (USERS, "replace", None, None, "`description` is required"),
        (USERS, "append", "", None, "`description` is required"),
        (TERM, "replace", "x", "email", "only for datasets"),
    ],
)
def test_invalid_arguments_write_nothing(
    graph: MagicMock,
    urn: str,
    operation: str,
    description: str | None,
    column: str | None,
    error: str,
) -> None:
    with pytest.raises(ToolError, match=error):
        update_description(
            entity_urn=urn,
            operation=operation,  # type: ignore[arg-type]
            description=description,
            column_path=column,
        )

    graph.execute_graphql.assert_not_called()


def test_a_write_gms_did_not_apply_is_an_error(graph: MagicMock) -> None:
    graph.execute_graphql.return_value = {"updateDescription": False}

    with pytest.raises(ToolError, match="did not change the description"):
        update_description(entity_urn=USERS, description="x")


def test_a_refused_write_reports_gms_reason(graph: MagicMock) -> None:
    graph.execute_graphql.side_effect = GraphError(
        [{"message": "Unauthorized to perform this action.", "extensions": {}}]
    )

    with pytest.raises(ToolError, match="rejected updateDescription: Unauthorized"):
        update_description(entity_urn=USERS, description="x")


def test_an_append_without_a_reply_warns_against_appending_again(
    graph: MagicMock,
) -> None:
    graph.execute_graphql.side_effect = [
        _sources({"type": "DATASET", "editableProperties": None, "properties": None}),
        requests.ReadTimeout("read timed out"),
    ]

    with pytest.raises(ToolError) as raised:
        update_description(entity_urn=USERS, operation="append", description=" x")

    assert "may or may not have been applied" in str(raised.value)
    assert "appending twice" in str(raised.value)


async def test_registered_as_a_destructive_write() -> None:
    mcp = FastMCP("t")
    register_tools(mcp, is_oss=True)

    async with Client(mcp) as client:
        tool = {t.name: t for t in await client.list_tools()}["update_description"]

    assert tool.annotations is not None
    assert tool.annotations.destructive_hint is True
    # Appending again appends again.
    assert tool.annotations.idempotent_hint is False
    assert groups_for_tags(tool.meta["fastmcp"]["tags"]) == ["write_metadata"]
