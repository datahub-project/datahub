"""Sibling metadata reaches get_entities the way the UI shows it."""

import copy
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Optional
from unittest.mock import Mock, patch

import pytest
from datahub.configuration.common import GraphError

from datahub_core_mcp_tools.graphql_helpers import with_datahub_client
from datahub_core_mcp_tools.sibling_merge import merge_siblings
from datahub_core_mcp_tools.tools.entities import get_entities

WAREHOUSE_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.analytics.events,PROD)"
)
DBT_URN = "urn:li:dataset:(urn:li:dataPlatform:dbt,db.analytics.events,PROD)"


def _tag(name: str) -> dict:
    return {"tag": {"urn": f"urn:li:tag:{name}", "properties": {"name": name}}}


def _term(name: str) -> dict:
    return {
        "term": {"urn": f"urn:li:glossaryTerm:{name}", "properties": {"name": name}}
    }


def _warehouse(*, primary: bool = False) -> dict:
    """A GetEntity result for a warehouse table its own ingestion left undocumented."""
    return {
        "urn": WAREHOUSE_URN,
        "siblings": {"isPrimary": primary},
        "properties": {"name": "events", "description": None},
        "tags": {"tags": [_tag("Raw")]},
        "schemaMetadata": {
            "fields": [
                {"fieldPath": "EVENT_SOURCE", "nativeDataType": "VARCHAR"},
                {"fieldPath": "project_name", "nativeDataType": "VARCHAR"},
                {"fieldPath": "LOADED_AT", "nativeDataType": "TIMESTAMP"},
            ]
        },
        "editableSchemaMetadata": None,
    }


def _dbt(*, primary: bool = True) -> dict:
    """The dbt model that builds the table, carrying the docs from its schema.yml."""
    return {
        "urn": DBT_URN,
        "siblings": {"isPrimary": primary},
        "properties": {"description": "One row per product event."},
        "tags": {"tags": [_tag("Raw"), _tag("Tier1")]},
        "schemaMetadata": {
            "fields": [
                {
                    "fieldPath": "event_source",
                    "description": "Where the event fired: web, ios or android.",
                    "glossaryTerms": {"terms": [_term("EventSource")]},
                    "schemaFieldEntity": {
                        "documentation": {
                            "documentations": [{"documentation": "Propagated note."}]
                        }
                    },
                },
                # Same column in the v2 field-path form some sources emit.
                {
                    "fieldPath": "[version=2.0].[type=string].project_name",
                    "description": "The product area that owns the event.",
                },
                {
                    "fieldPath": "dbt_only_column",
                    "description": "Not in the warehouse.",
                },
            ]
        },
        "editableSchemaMetadata": {
            "editableSchemaFieldInfo": [
                {"fieldPath": "event_source", "description": "Edited in the UI."}
            ]
        },
    }


def _with_siblings(entity: dict, *siblings: dict) -> dict:
    """What GraphQLReadBackend.entity returns: GetEntity plus GetEntitySiblings."""
    entity["siblingsSearch"] = {"searchResults": [{"entity": s} for s in siblings]}
    return entity


def _fields(entity: dict) -> dict[str, dict]:
    return {f["fieldPath"]: f for f in entity["schemaMetadata"]["fields"]}


def test_undocumented_table_gets_its_dbt_siblings_docs() -> None:
    entity = _with_siblings(_warehouse(), _dbt())
    merge_siblings(entity)

    assert entity["properties"]["description"] == "One row per product event."
    # Unioned, not duplicated.
    assert [t["tag"]["urn"] for t in entity["tags"]["tags"]] == [
        "urn:li:tag:Raw",
        "urn:li:tag:Tier1",
    ]
    fields = _fields(entity)
    # Matched ignoring case and v2 tokens; the warehouse's own spelling is kept.
    source = fields["EVENT_SOURCE"]
    assert source["description"] == "Where the event fired: web, ios or android."
    assert source["glossaryTerms"]["terms"][0]["term"]["urn"] == (
        "urn:li:glossaryTerm:EventSource"
    )
    assert source["schemaFieldEntity"]["documentation"]["documentations"] == [
        {"documentation": "Propagated note."}
    ]
    assert fields["project_name"]["description"] == (
        "The product area that owns the event."
    )
    # The schema describes the requested table: a sibling-only column is not added.
    assert "dbt_only_column" not in fields
    # Edits are re-keyed to the warehouse fieldPath, which is what cleaning looks up.
    assert entity["editableSchemaMetadata"]["editableSchemaFieldInfo"] == [
        {"fieldPath": "EVENT_SOURCE", "description": "Edited in the UI."}
    ]
    assert entity["siblings"] == [DBT_URN]
    assert "siblingsSearch" not in entity


def test_primary_sibling_wins_a_conflict_as_in_the_ui() -> None:
    warehouse = _warehouse()
    warehouse["properties"]["description"] = "Warehouse comment."
    warehouse["schemaMetadata"]["fields"][0]["description"] = "Column comment."

    as_secondary = _with_siblings(copy.deepcopy(warehouse), _dbt(primary=True))
    merge_siblings(as_secondary)
    assert as_secondary["properties"]["description"] == "One row per product event."
    assert _fields(as_secondary)["EVENT_SOURCE"]["description"].startswith("Where")

    # With no sibling marked primary, the requested entity is treated as primary.
    unmarked = _with_siblings(copy.deepcopy(warehouse), _dbt(primary=False))
    merge_siblings(unmarked)
    assert unmarked["properties"]["description"] == "Warehouse comment."
    assert _fields(unmarked)["EVENT_SOURCE"]["description"] == "Column comment."


def test_own_columns_sharing_a_key_keep_their_own_metadata() -> None:
    warehouse = _warehouse()
    warehouse["schemaMetadata"]["fields"] = [
        {"fieldPath": "ID", "description": "Upper-case id."},
        {"fieldPath": "id"},
    ]
    warehouse["editableSchemaMetadata"] = {
        "editableSchemaFieldInfo": [{"fieldPath": "ID", "description": "Edited ID."}]
    }
    dbt = _dbt(primary=False)
    dbt["schemaMetadata"]["fields"] = [{"fieldPath": "id", "description": "dbt id."}]
    dbt["editableSchemaMetadata"] = None
    entity = _with_siblings(warehouse, dbt)

    merge_siblings(entity)

    fields = _fields(entity)
    assert fields["ID"]["description"] == "Upper-case id."
    # The sibling fills `id`, not its upper-case namesake.
    assert fields["id"]["description"] == "dbt id."
    assert entity["editableSchemaMetadata"]["editableSchemaFieldInfo"] == [
        {"fieldPath": "ID", "description": "Edited ID."}
    ]


def test_entity_without_siblings_is_unchanged() -> None:
    entity = _warehouse()
    entity["siblings"] = None
    expected = {k: v for k, v in copy.deepcopy(entity).items() if k != "siblings"}

    merge_siblings(entity)

    assert entity == expected


@pytest.fixture
def mock_client() -> Mock:
    client = Mock()
    client._graph = Mock()
    client._graph.exists.return_value = True
    return client


def _graphql(
    entity: dict, *siblings: dict, siblings_error: Optional[Exception] = None
) -> tuple[Callable[..., dict], list[Optional[str]]]:
    """execute_graphql stand-in answering GetEntity and GetEntitySiblings."""
    sent: list[Optional[str]] = []

    def execute(
        graph: Any, *, operation_name: Optional[str] = None, **kwargs: Any
    ) -> dict:
        sent.append(operation_name)
        if operation_name == "GetEntitySiblings":
            if siblings_error is not None:
                raise siblings_error
            return {
                "entity": {
                    "urn": entity["urn"],
                    "siblingsSearch": {
                        "searchResults": [{"entity": s} for s in siblings]
                    },
                }
            }
        return {"entity": copy.deepcopy(entity)}

    return execute, sent


@contextmanager
def _serving(client: Mock, execute: Callable[..., dict]) -> Iterator[None]:
    with (
        with_datahub_client(client),
        patch("datahub_core_mcp_tools.graphql_helpers.execute_graphql", execute),
    ):
        yield


def _assert_own_metadata_only(result: dict) -> None:
    """The response the tools gave before the merge existed."""
    assert "siblings" not in result
    assert "description" not in result
    assert "description" not in _fields(result)["EVENT_SOURCE"]


def test_get_entities_returns_merged_docs_and_names_the_sibling(
    mock_client: Mock,
) -> None:
    execute, sent = _graphql(_warehouse(), _dbt())
    with _serving(mock_client, execute):
        result = get_entities(WAREHOUSE_URN, fields=["description", "schemaMetadata"])

    assert sent == ["GetEntity", "GetEntitySiblings"]
    assert result["description"] == "One row per product event."
    assert result["siblings"] == [DBT_URN]
    column = _fields(result)["EVENT_SOURCE"]
    assert column["description"] == "Where the event fired: web, ios or android."
    assert column["editedDescription"] == "Edited in the UI."


@pytest.mark.parametrize(
    "dbt_primary, event_source_edit",
    [
        # dbt's edit wins the column both edited, as on the UI's dataset page.
        (True, "Edited in the UI."),
        (False, "Warehouse edit."),
    ],
)
def test_own_column_edits_survive_the_merge(
    mock_client: Mock, dbt_primary: bool, event_source_edit: str
) -> None:
    warehouse = _warehouse()
    warehouse["editableSchemaMetadata"] = {
        "editableSchemaFieldInfo": [
            {"fieldPath": "EVENT_SOURCE", "description": "Warehouse edit."},
            {"fieldPath": "LOADED_AT", "description": "When the row landed."},
        ]
    }
    execute, _ = _graphql(warehouse, _dbt(primary=dbt_primary))
    with _serving(mock_client, execute):
        result = get_entities(WAREHOUSE_URN, fields=["schemaMetadata"])

    fields = _fields(result)
    assert fields["EVENT_SOURCE"]["editedDescription"] == event_source_edit
    assert fields["LOADED_AT"]["editedDescription"] == "When the row landed."


def test_get_entities_skips_the_sibling_read_without_siblings(
    mock_client: Mock,
) -> None:
    entity = _warehouse()
    entity["siblings"] = None
    execute, sent = _graphql(entity)
    with _serving(mock_client, execute):
        result = get_entities(WAREHOUSE_URN)

    assert sent == ["GetEntity"]
    assert "siblings" not in result


def test_switched_off_returns_the_entity_alone(
    mock_client: Mock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SIBLING_MERGE_ENABLED", "false")
    execute, sent = _graphql(_warehouse(), _dbt())
    with _serving(mock_client, execute):
        result = get_entities(WAREHOUSE_URN, fields=["description", "schemaMetadata"])

    assert sent == ["GetEntity"]
    _assert_own_metadata_only(result)


def test_failed_sibling_read_returns_the_entity_alone(mock_client: Mock) -> None:
    execute, sent = _graphql(
        _warehouse(), _dbt(), siblings_error=GraphError("search timed out")
    )
    with _serving(mock_client, execute):
        result = get_entities(WAREHOUSE_URN, fields=["description", "schemaMetadata"])

    assert sent == ["GetEntity", "GetEntitySiblings"]
    _assert_own_metadata_only(result)
