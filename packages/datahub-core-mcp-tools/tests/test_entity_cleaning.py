"""entity_cleaning: what an entity read looks like once it is bounded for an LLM."""

from unittest.mock import patch

from datahub_core_mcp_tools import entity_cleaning
from datahub_core_mcp_tools.entity_cleaning import (
    clean_get_entities_response,
    documentation_entries,
    sanitize_and_truncate_description,
    truncate_descriptions,
)

DATASET = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sales.orders,PROD)"


def test_embedded_images_become_alt_text_and_sql_survives() -> None:
    text = (
        'Revenue <img src="data:image/png;base64,AAAA" alt="chart"> where a <> b '
        "and ![diagram](data:image/png;base64,BBBB) &amp; more"
    )

    assert sanitize_and_truncate_description(text, 500) == (
        "Revenue chart where a <> b and diagram & more"
    )


def test_a_cut_description_says_how_much_was_dropped() -> None:
    cut = sanitize_and_truncate_description(
        "alpha beta gamma delta", 12, dropped_count=True
    )

    assert cut == "alpha beta… [+12 chars]"


def test_long_custom_property_values_are_marked_as_truncated() -> None:
    entity = {"properties": {"customProperties": [{"key": "sql", "value": "x" * 6000}]}}

    truncate_descriptions(entity)

    value = entity["properties"]["customProperties"][0]["value"]
    assert value.startswith("x" * 5000)
    assert value.endswith("[value truncated: showing 5000 of 6000 chars]")


def test_documentation_entries_carry_their_origin() -> None:
    documentation = {
        "documentations": [
            {
                "documentation": "Copied from upstream.",
                "attribution": {
                    "sourceDetail": [{"key": "propagated", "value": "true"}]
                },
            },
            {
                "documentation": "Model output.",
                "attribution": {"sourceDetail": [{"key": "inferred", "value": "true"}]},
            },
            {"documentation": "Ingested."},
        ]
    }

    assert [
        e["origin"] for e in documentation_entries(documentation, max_length=100)
    ] == [
        "propagated",
        "ai_generated",
        "unknown",
    ]


def _schema_entity(fields: list[dict], edits: list[dict] | None = None) -> dict:
    return {
        "urn": DATASET,
        "schemaMetadata": {"fields": fields, "platformSchema": {"schema": ""}},
        "editableSchemaMetadata": {"editableSchemaFieldInfo": edits or []},
    }


def _tags(*names: str) -> dict:
    return {"tags": [{"tag": {"properties": {"name": n}}} for n in names]}


def test_columns_are_ordered_by_usefulness_and_ui_edits_are_merged() -> None:
    entity = _schema_entity(
        [
            {"fieldPath": "z_plain", "nativeDataType": "INT"},
            {"fieldPath": "a_tagged", "tags": _tags("pii")},
            {"fieldPath": "m_described", "description": "Order total."},
            {"fieldPath": "id", "isPartOfKey": True, "nullable": False},
        ],
        edits=[
            {"fieldPath": "a_tagged", "description": "Edited.", "tags": _tags("pii")},
            {"fieldPath": "m_described", "description": "Order total."},
        ],
    )

    schema = clean_get_entities_response(entity)["schemaMetadata"]

    assert [f["fieldPath"] for f in schema["fields"]] == [
        "id",
        "m_described",
        "a_tagged",
        "z_plain",
    ]
    tagged = schema["fields"][2]
    assert tagged["tags"] == ["pii"]
    assert tagged["editedDescription"] == "Edited."
    # Same as what ingestion wrote: not repeated.
    assert "editedTags" not in tagged
    assert "editedDescription" not in schema["fields"][1]
    assert "platformSchema" not in schema


def test_column_text_is_a_preview_unless_the_column_was_targeted() -> None:
    long_text = "word " * 100
    entity = _schema_entity([{"fieldPath": "notes", "description": long_text}])

    preview = clean_get_entities_response(entity)
    full = clean_get_entities_response(entity, full_text_columns=["notes"])

    assert preview["schemaMetadata"]["fields"][0]["description"].endswith("chars]")
    assert len(preview["schemaMetadata"]["fields"][0]["description"]) < 150
    assert full["schemaMetadata"]["fields"][0]["description"] == long_text


def test_the_schema_budget_keeps_at_least_one_column_and_says_what_it_cut() -> None:
    entity = _schema_entity([{"fieldPath": f"col_{i}"} for i in range(50)])

    schema = clean_get_entities_response(entity, schema_token_budget=1)[
        "schemaMetadata"
    ]

    assert len(schema["fields"]) == 1
    assert schema["schemaFieldsTruncated"] == {
        "totalFields": 50,
        "includedFields": 1,
        "offset": 0,
    }


def test_offset_and_limit_page_through_the_ordered_columns() -> None:
    entity = _schema_entity([{"fieldPath": f"col_{i:02d}"} for i in range(10)])

    schema = clean_get_entities_response(entity, offset=4, limit=3)["schemaMetadata"]

    assert [f["fieldPath"] for f in schema["fields"]] == ["col_04", "col_05", "col_06"]


def test_long_documents_and_view_sql_are_capped() -> None:
    with patch.object(entity_cleaning, "DOCUMENT_CONTENT_CHAR_LIMIT", 10):
        document = clean_get_entities_response(
            {"urn": "urn:li:document:d", "info": {"contents": {"text": "x" * 25}}}
        )
    view = clean_get_entities_response(
        {"urn": DATASET, "viewProperties": {"logic": "SELECT " + "a, " * 3000}}
    )

    contents = document["info"]["contents"]
    assert contents["text"].startswith("x" * 10 + "\n\n[Content truncated.")
    assert contents["_originalLengthChars"] == 25
    assert view["viewProperties"]["logic"].endswith("... [truncated]")


def test_last_modified_stamps_become_dates() -> None:
    cleaned = clean_get_entities_response(
        {"urn": DATASET, "properties": {"lastModified": {"time": 1779284700000}}}
    )

    assert cleaned["properties"]["lastModified"] == "2026-05-20"
