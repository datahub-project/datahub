"""Minimal Dagster definitions for live integration testing of the pull source.

Run with:  dagster dev -f definitions.py -h 0.0.0.0 -p 3000

Declares a small asset graph (raw_events -> events) plus owners, a group, tags, a
markdown description, a documentation URL, and a table schema, so the connector's
ownership / tags / documentation / schema extraction are all exercised. All names
are generic placeholders (no customer identifiers).
"""

from dagster import (  # type: ignore[import-not-found]
    Definitions,
    MetadataValue,
    TableColumn,
    TableSchema,
    asset,
)


@asset(
    key_prefix=["my_db", "my_schema"],
    group_name="analytics",
    owners=["data_eng@example.com"],
    tags={"tier": "bronze"},
    description="Raw events landing asset.",
)
def raw_events() -> list:
    return [{"event_id": 1}, {"event_id": 2}]


@asset(
    key_prefix=["my_db", "my_schema"],
    group_name="analytics",
    owners=["data_eng@example.com", "team:platform"],
    tags={"tier": "gold", "pii": ""},
    description="# Events\nCleaned and deduplicated events.",
    metadata={
        "runbook": MetadataValue.url("https://example.com/runbook"),
        "schema": MetadataValue.table_schema(
            TableSchema(
                columns=[
                    TableColumn("event_id", "int", description="Primary key"),
                    TableColumn("event_ts", "timestamp", description="Event time"),
                ]
            )
        ),
    },
)
def events(raw_events: list) -> int:
    return len(raw_events)


defs = Definitions(assets=[raw_events, events])
