"""Equivalence test: DB mode and REST mode must emit the same URNs and the
same lineage edges for the same logical Fivetran data.

The two readers fetch from different sources (SQL log tables vs REST API)
but feed the same downstream URN-construction code. This test pins that
contract.

Known acceptable differences (not asserted equal):
- DPI customProperties detail: DB mode parses `end_message_data` JSON
  (richer failure breadcrumbs); REST mode has plain `status` only.
- DataJob's own customProperties: connector_name source differs slightly
  (DB pulls from `connection_name` column, REST uses `listed.schema`),
  but these align in this test fixture by design.
- Aspect emission order: workunit ordering can differ; assertions are
  order-insensitive.
"""

import datetime
import json
from typing import Dict, Iterator, List, Set, Tuple
from unittest import mock
from unittest.mock import MagicMock

import pytest
import time_machine

from datahub.ingestion.run.pipeline import Pipeline
from datahub.ingestion.source.fivetran.fivetran_rest_api import FivetranAPIClient
from datahub.ingestion.source.fivetran.response_models import (
    FivetranColumn,
    FivetranConnectionSchemas,
    FivetranGroup,
    FivetranListedConnection,
    FivetranListedUser,
    FivetranSchema,
    FivetranTable,
)

# Frozen time so DB and REST runs produce the same timestamps in MCEs.
FROZEN_TIME = "2024-06-01 12:00:00"

# ---------------------------------------------------------------------------
# Shared test fixture: one Postgres connector → Snowflake destination,
# `public.employee` (id, name) lineage, two recent sync runs.
# ---------------------------------------------------------------------------

CONNECTOR_ID = "calendar_elected"
DESTINATION_ID = "test_destination_id"
SOURCE_SCHEMA = "public"
SOURCE_TABLE = "employee"
DEST_SCHEMA = "postgres_public"
DEST_TABLE = "employee"
DEST_DATABASE = "test_database"
USER_ID = "test_user_id"
USER_EMAIL = "user_a@example.com"

SYNC_1_START = datetime.datetime(2024, 5, 31, 6, 0, 0, tzinfo=datetime.timezone.utc)
SYNC_1_END = datetime.datetime(2024, 5, 31, 6, 1, 0, tzinfo=datetime.timezone.utc)
SYNC_2_START = datetime.datetime(2024, 5, 31, 18, 0, 0, tzinfo=datetime.timezone.utc)
SYNC_2_END = datetime.datetime(2024, 5, 31, 18, 1, 0, tzinfo=datetime.timezone.utc)


# ---------------------------------------------------------------------------
# DB mode mock — the SQL query handler.
# ---------------------------------------------------------------------------


def _db_query_handler(query: str) -> List[Dict]:
    """Mock query results that mirror the REST mocks below.

    Note: Snowflake config defaults uppercase identifiers, so the runtime
    sends queries against `"TEST_DATABASE"` / `"TEST"`. Build the expected
    query strings the same way.
    """
    from datahub.ingestion.source.fivetran.fivetran_query import FivetranLogQuery

    q = FivetranLogQuery()
    q.set_schema("TEST")

    if query == q.use_database(DEST_DATABASE.upper()):
        return []
    if query == q.get_connectors_query():
        return [
            {
                "connection_id": CONNECTOR_ID,
                "connecting_user_id": USER_ID,
                "connector_type_id": "postgres",
                "connection_name": DEST_SCHEMA,  # matches REST `listed.schema`
                "paused": False,
                "sync_frequency": 1440,
                "destination_id": DESTINATION_ID,
            }
        ]
    if query == q.get_table_lineage_query(connector_ids=[CONNECTOR_ID]):
        return [
            {
                "connection_id": CONNECTOR_ID,
                "source_table_id": "10040",
                "source_table_name": SOURCE_TABLE,
                "source_schema_name": SOURCE_SCHEMA,
                "destination_table_id": "7779",
                "destination_table_name": DEST_TABLE,
                "destination_schema_name": DEST_SCHEMA,
            }
        ]
    if query == q.get_column_lineage_query(connector_ids=[CONNECTOR_ID]):
        return [
            {
                "source_table_id": "10040",
                "destination_table_id": "7779",
                "source_column_name": "id",
                "destination_column_name": "id",
            },
            {
                "source_table_id": "10040",
                "destination_table_id": "7779",
                "source_column_name": "name",
                "destination_column_name": "name",
            },
        ]
    if query == q.get_users_query():
        return [
            {
                "user_id": USER_ID,
                "given_name": "User",
                "family_name": "A",
                "email": USER_EMAIL,
            }
        ]
    if query == q.get_sync_logs_query(syncs_interval=7, connector_ids=[CONNECTOR_ID]):
        return [
            {
                "connection_id": CONNECTOR_ID,
                "sync_id": "sync-1",
                "start_time": SYNC_1_START,
                "end_time": SYNC_1_END,
                "end_message_data": '"{\\"status\\":\\"SUCCESSFUL\\"}"',
            },
            {
                "connection_id": CONNECTOR_ID,
                "sync_id": "sync-2",
                "start_time": SYNC_2_START,
                "end_time": SYNC_2_END,
                "end_message_data": '"{\\"status\\":\\"SUCCESSFUL\\"}"',
            },
        ]
    return []


# ---------------------------------------------------------------------------
# REST mode mocks — equivalent payloads to the SQL fixture above.
# ---------------------------------------------------------------------------


def _fake_list_connections(
    self: object, group_id: str, page_size: int = 500
) -> Iterator[FivetranListedConnection]:
    if group_id != DESTINATION_ID:
        return iter([])
    return iter(
        [
            FivetranListedConnection(
                id=CONNECTOR_ID,
                schema_=DEST_SCHEMA,  # `listed.schema` populates connector_name
                service="postgres",
                paused=False,
                sync_frequency=1440,
                group_id=DESTINATION_ID,
                connected_by=USER_ID,
            )
        ]
    )


def _fake_get_connection_schemas(
    self: object, connection_id: str
) -> FivetranConnectionSchemas:
    if connection_id != CONNECTOR_ID:
        return FivetranConnectionSchemas()
    return FivetranConnectionSchemas(
        schemas={
            SOURCE_SCHEMA: FivetranSchema(
                name_in_destination=DEST_SCHEMA,
                enabled=True,
                tables={
                    SOURCE_TABLE: FivetranTable(
                        name_in_destination=DEST_TABLE,
                        enabled=True,
                        columns={
                            "id": FivetranColumn(
                                name_in_destination="id",
                                enabled=True,
                                is_primary_key=True,
                            ),
                            "name": FivetranColumn(
                                name_in_destination="name", enabled=True
                            ),
                        },
                    )
                },
            )
        }
    )


def _fake_list_users(
    self: object, group_id: str, page_size: int = 500
) -> Iterator[FivetranListedUser]:
    if group_id != DESTINATION_ID:
        return iter([])
    return iter(
        [
            FivetranListedUser(
                id=USER_ID, email=USER_EMAIL, given_name="User", family_name="A"
            )
        ]
    )


def _fake_list_groups(self: object, page_size: int = 500) -> Iterator[FivetranGroup]:
    return iter([FivetranGroup(id=DESTINATION_ID, name="Test Group")])


# ---------------------------------------------------------------------------
# Pipelines
# ---------------------------------------------------------------------------


def _run_db_pipeline(output_path: str) -> None:
    with (
        mock.patch(
            "datahub.ingestion.source.fivetran.fivetran_log_db_reader.create_engine"
        ) as mock_create_engine,
        mock.patch(
            "datahub.ingestion.source.fivetran.fivetran_log_db_reader.create_workspace_client"
        ),
    ):
        connection_magic_mock = MagicMock()
        connection_magic_mock.execute.side_effect = _db_query_handler
        mock_create_engine.return_value = connection_magic_mock

        pipeline = Pipeline.create(
            {
                "run_id": "fivetran-db-mode",
                "source": {
                    "type": "fivetran",
                    "config": {
                        "log_source": "log_database",
                        "fivetran_log_config": {
                            "destination_platform": "snowflake",
                            "snowflake_destination_config": {
                                "account_id": "testid",
                                "warehouse": "test_wh",
                                "username": "test",
                                "password": "test@123",
                                "database": DEST_DATABASE,
                                "role": "testrole",
                                "log_schema": "test",
                            },
                        },
                        # No connector filter — DB mode filters by
                        # `connection_name`, REST by `connection_id`.
                        # Different fields would yield different inclusions
                        # in this fixture; test pipeline-level filtering
                        # behavior in the per-mode tests instead.
                        "destination_patterns": {"allow": [DESTINATION_ID]},
                    },
                },
                "sink": {"type": "file", "config": {"filename": output_path}},
            }
        )
        pipeline.run()
        pipeline.raise_from_status()


def _run_rest_pipeline(output_path: str) -> None:
    with (
        mock.patch.object(
            FivetranAPIClient,
            "list_groups",
            autospec=True,
            side_effect=_fake_list_groups,
        ),
        mock.patch.object(
            FivetranAPIClient,
            "list_connections",
            autospec=True,
            side_effect=_fake_list_connections,
        ),
        mock.patch.object(
            FivetranAPIClient,
            "get_connection_schemas",
            autospec=True,
            side_effect=_fake_get_connection_schemas,
        ),
        mock.patch.object(
            FivetranAPIClient,
            "list_users",
            autospec=True,
            side_effect=_fake_list_users,
        ),
    ):
        pipeline = Pipeline.create(
            {
                "run_id": "fivetran-rest-mode",
                "source": {
                    "type": "fivetran",
                    "config": {
                        "log_source": "rest_api",
                        "api_config": {
                            "api_key": "k",
                            "api_secret": "s",
                        },
                        # Force same destination URN as DB mode by declaring
                        # the destination's platform + database so REST mode
                        # doesn't have to discover it.
                        "destination_to_platform_instance": {
                            DESTINATION_ID: {
                                "platform": "snowflake",
                                "database": DEST_DATABASE,
                                "env": "PROD",
                            }
                        },
                    },
                },
                "sink": {"type": "file", "config": {"filename": output_path}},
            }
        )
        pipeline.run()
        pipeline.raise_from_status()


# ---------------------------------------------------------------------------
# Helpers for comparing emitted MCEs
# ---------------------------------------------------------------------------


def _load_events(path: str) -> List[Dict]:
    with open(path) as f:
        return json.load(f)


def _urns_by_entity_type(events: List[Dict]) -> Dict[str, Set[str]]:
    """Group all distinct entity URNs by entity type."""
    result: Dict[str, Set[str]] = {}
    for ev in events:
        urn = ev.get("entityUrn")
        et = ev.get("entityType")
        if urn and et:
            result.setdefault(et, set()).add(urn)
    return result


def _aspect_signatures(
    events: List[Dict], aspect_name: str, fields: Tuple[str, ...]
) -> Dict[str, Dict]:
    """For each entity emitting `aspect_name`, project the named fields."""
    result: Dict[str, Dict] = {}
    for ev in events:
        if ev.get("aspectName") != aspect_name:
            continue
        urn = ev.get("entityUrn")
        if not urn:
            continue
        value = (ev.get("aspect") or {}).get("value") or {}
        result[urn] = {k: value.get(k) for k in fields}
    return result


def _input_output_lineage(events: List[Dict]) -> Dict[str, Tuple[Set[str], Set[str]]]:
    """For each DataJob emitting dataJobInputOutput, return (inputs, outputs)."""
    out: Dict[str, Tuple[Set[str], Set[str]]] = {}
    for ev in events:
        if ev.get("aspectName") != "dataJobInputOutput":
            continue
        urn = ev.get("entityUrn")
        if not urn:
            continue
        value = (ev.get("aspect") or {}).get("value") or {}
        inputs = set(value.get("inputDatasets", []) or [])
        outputs = set(value.get("outputDatasets", []) or [])
        out[urn] = (inputs, outputs)
    return out


# ---------------------------------------------------------------------------
# The actual test
# ---------------------------------------------------------------------------


@time_machine.travel(FROZEN_TIME, tick=False)
@pytest.mark.integration
def test_db_and_rest_modes_emit_equivalent_lineage(tmp_path):
    db_out = str(tmp_path / "fivetran_db.json")
    rest_out = str(tmp_path / "fivetran_rest.json")

    _run_db_pipeline(db_out)
    _run_rest_pipeline(rest_out)

    db_events = _load_events(db_out)
    rest_events = _load_events(rest_out)

    # 1. Same set of entity URNs across modes (DataFlow, DataJob, Dataset).
    db_urns = _urns_by_entity_type(db_events)
    rest_urns = _urns_by_entity_type(rest_events)
    for entity_type in ("dataFlow", "dataJob", "dataset"):
        assert db_urns.get(entity_type, set()) == rest_urns.get(entity_type, set()), (
            f"{entity_type} URN sets differ\n"
            f"  DB only:   {db_urns.get(entity_type, set()) - rest_urns.get(entity_type, set())}\n"
            f"  REST only: {rest_urns.get(entity_type, set()) - db_urns.get(entity_type, set())}"
        )

    # 2. DataFlow `name` aspect matches between modes — pinning that
    #    `connector_name` is the same regardless of which reader fetched it.
    db_flow_names = _aspect_signatures(db_events, "dataFlowInfo", ("name",))
    rest_flow_names = _aspect_signatures(rest_events, "dataFlowInfo", ("name",))
    assert db_flow_names == rest_flow_names

    # 3. DataJob `name` matches.
    db_job_names = _aspect_signatures(db_events, "dataJobInfo", ("name",))
    rest_job_names = _aspect_signatures(rest_events, "dataJobInfo", ("name",))
    assert db_job_names == rest_job_names

    # 4. Lineage edges (input/output dataset URNs) match per DataJob.
    assert _input_output_lineage(db_events) == _input_output_lineage(rest_events)

    # 5. DPI events: DB mode emits per-run DataProcessInstance events;
    #    REST mode does NOT (Fivetran's REST API has no sync-history
    #    endpoint per connection — there's no equivalent data source).
    db_has_dpi = any(
        "dataProcessInstance" in ev.get("entityUrn", "") for ev in db_events
    )
    rest_has_dpi = any(
        "dataProcessInstance" in ev.get("entityUrn", "") for ev in rest_events
    )
    assert db_has_dpi, "DB mode should emit DPI events"
    assert not rest_has_dpi, (
        "REST mode should not emit DPI events (no sync-history endpoint)"
    )
