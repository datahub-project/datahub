"""Tests for the REST-only log reader.

Each test is driven by sample JSON captured in the prerequisites step;
copy the saved /tmp/fv_*.json contents into the inline fixtures or load
them from `tests/unit/fivetran/fixtures/` if your fixtures get large.
"""

import inspect
import threading
import time
from typing import Iterable, List, Optional
from unittest.mock import MagicMock, patch

import pytest
import requests
import sqlalchemy.exc

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.source import StructuredLogEntry
from datahub.ingestion.source.fivetran.config import (
    FivetranAPIConfig,
    FivetranSourceReport,
)
from datahub.ingestion.source.fivetran.fivetran_log_rest_reader import (
    FivetranLogRestReader,
)
from datahub.ingestion.source.fivetran.fivetran_rest_api import FivetranAPIClient
from datahub.ingestion.source.fivetran.response_models import (
    FivetranColumn,
    FivetranConnectionSchemas,
    FivetranGroup,
    FivetranListConnectionsResponse,
    FivetranListedConnection,
    FivetranListedUser,
    FivetranListUsersResponse,
    FivetranSchema,
    FivetranTable,
)


def _make_client():
    return FivetranAPIClient(FivetranAPIConfig(api_key="k", api_secret="s"))


class TestResponseModelParsing:
    def test_list_connections_paginated(self):
        raw = {
            "code": "Success",
            "data": {
                "items": [
                    {
                        "id": "calendar_elected",
                        "schema": "postgres_public",
                        "service": "postgres",
                        "paused": False,
                        "sync_frequency": 1440,
                        "group_id": "g1",
                        "connected_by": "test_user_id",
                    }
                ],
                "next_cursor": "abc123",
            },
        }
        parsed = FivetranListConnectionsResponse.model_validate(raw["data"])
        assert len(parsed.items) == 1
        assert parsed.items[0].id == "calendar_elected"
        assert parsed.items[0].service == "postgres"
        assert parsed.next_cursor == "abc123"

    def test_connection_schemas_with_columns(self):
        raw = {
            "schemas": {
                "public": {
                    "name_in_destination": "postgres_public",
                    "enabled": True,
                    "tables": {
                        "employee": {
                            "name_in_destination": "employee",
                            "enabled": True,
                            "columns": {
                                "id": {
                                    "name_in_destination": "id",
                                    "enabled": True,
                                    "is_primary_key": True,
                                },
                                "name": {
                                    "name_in_destination": "name",
                                    "enabled": True,
                                    "is_primary_key": False,
                                },
                            },
                        }
                    },
                }
            }
        }
        parsed = FivetranConnectionSchemas.model_validate(raw)
        assert "public" in parsed.schemas
        schema = parsed.schemas["public"]
        assert schema.name_in_destination == "postgres_public"
        assert "employee" in schema.tables
        table = schema.tables["employee"]
        assert table.name_in_destination == "employee"
        assert len(table.columns) == 2
        assert table.columns["id"].is_primary_key is True

    def test_list_users(self):
        raw = {
            "items": [
                {
                    "id": "test_user_id",
                    "email": "user_a@example.com",
                    "given_name": "User",
                    "family_name": "A",
                }
            ],
            "next_cursor": None,
        }
        parsed = FivetranListUsersResponse.model_validate(raw)
        assert parsed.items[0].id == "test_user_id"
        assert parsed.items[0].email == "user_a@example.com"

    def test_unknown_fields_are_tolerated(self):
        # Future-proof against Fivetran adding new fields.
        raw = {
            "items": [
                {
                    "id": "x",
                    "schema": "s",
                    "service": "postgres",
                    "paused": False,
                    "sync_frequency": 1,
                    "group_id": "g",
                    "connected_by": None,
                    "field_we_dont_know_about": [1, 2, 3],
                }
            ]
        }
        FivetranListConnectionsResponse.model_validate(raw)  # must not raise


class TestListConnections:
    def test_single_page(self):
        client = _make_client()
        resp = MagicMock()
        resp.json.return_value = {
            "code": "Success",
            "data": {
                "items": [
                    {
                        "id": "c1",
                        "schema": "s",
                        "service": "postgres",
                        "paused": False,
                        "sync_frequency": 1440,
                        "group_id": "g",
                        "connected_by": None,
                    }
                ],
                "next_cursor": None,
            },
        }
        resp.raise_for_status = MagicMock()
        with patch.object(client._session, "get", return_value=resp):
            result = list(client.list_connections(group_id="g"))
        assert len(result) == 1
        assert result[0].id == "c1"

    def test_paginates_until_cursor_none(self):
        client = _make_client()

        def _page(_url, **kwargs):
            cursor = kwargs.get("params", {}).get("cursor")
            r = MagicMock()
            if cursor is None:
                r.json.return_value = {
                    "code": "Success",
                    "data": {
                        "items": [
                            {
                                "id": "p1",
                                "schema": "s",
                                "service": "postgres",
                                "paused": False,
                                "sync_frequency": 1,
                                "group_id": "g",
                            }
                        ],
                        "next_cursor": "next1",
                    },
                }
            elif cursor == "next1":
                r.json.return_value = {
                    "code": "Success",
                    "data": {
                        "items": [
                            {
                                "id": "p2",
                                "schema": "s",
                                "service": "postgres",
                                "paused": False,
                                "sync_frequency": 1,
                                "group_id": "g",
                            }
                        ],
                        "next_cursor": None,
                    },
                }
            r.raise_for_status = MagicMock()
            return r

        with patch.object(client._session, "get", side_effect=_page) as mocked:
            result = list(client.list_connections(group_id="g"))
        assert [c.id for c in result] == ["p1", "p2"]
        assert mocked.call_count == 2


class TestGetConnectionSchemas:
    def test_returns_parsed_schemas(self):
        client = _make_client()
        resp = MagicMock()
        resp.json.return_value = {
            "code": "Success",
            "data": {
                "schemas": {
                    "public": {
                        "name_in_destination": "postgres_public",
                        "enabled": True,
                        "tables": {
                            "employee": {
                                "name_in_destination": "employee",
                                "enabled": True,
                                "columns": {
                                    "id": {
                                        "name_in_destination": "id",
                                        "enabled": True,
                                        "is_primary_key": True,
                                    }
                                },
                            }
                        },
                    }
                }
            },
        }
        resp.raise_for_status = MagicMock()
        with patch.object(client._session, "get", return_value=resp):
            result = client.get_connection_schemas("conn_x")
        assert "public" in result.schemas
        assert "employee" in result.schemas["public"].tables


class TestListUsers:
    def test_paginates(self):
        client = _make_client()

        def _page(_url, **kwargs):
            cursor = kwargs.get("params", {}).get("cursor")
            r = MagicMock()
            if cursor is None:
                r.json.return_value = {
                    "code": "Success",
                    "data": {
                        "items": [
                            {"id": "u1", "email": "u1@x"},
                        ],
                        "next_cursor": "n",
                    },
                }
            else:
                r.json.return_value = {
                    "code": "Success",
                    "data": {
                        "items": [{"id": "u2", "email": "u2@x"}],
                        "next_cursor": None,
                    },
                }
            r.raise_for_status = MagicMock()
            return r

        with patch.object(client._session, "get", side_effect=_page):
            users = list(client.list_users(group_id="g"))
        assert {u.id for u in users} == {"u1", "u2"}


def _make_reader(
    api_client: object,
    report: Optional[FivetranSourceReport] = None,
    max_workers: int = 1,
    per_connector_timeout_sec: int = 300,
) -> FivetranLogRestReader:
    reader = FivetranLogRestReader.__new__(FivetranLogRestReader)
    reader.api_client = api_client  # type: ignore[assignment]  # tests pass MagicMock
    reader._report = report or FivetranSourceReport()
    reader._max_table_lineage_per_connector = 120
    reader._max_column_lineage_per_connector = 1000
    reader._max_workers = max_workers
    reader._per_connector_timeout_sec = per_connector_timeout_sec
    reader._db_log_reader = None
    reader._db_lineage_reader = None
    reader._db_lineage_unavailable_reported = False
    reader._table_columns_failure_reported = False
    reader._report_lock = threading.Lock()
    reader._user_email_cache = {}
    reader._group_ids = ["g1"]  # populated in __init__ in real construction
    return reader


class TestGetAllowedConnectorsListRest:
    def test_assembles_connector_with_table_lineage_from_schemas(self):
        api = MagicMock()
        api.list_connections.return_value = iter(
            [
                FivetranListedConnection(
                    id="c1",
                    schema_="postgres_public",
                    service="postgres",
                    paused=False,
                    sync_frequency=1440,
                    group_id="g1",
                    connected_by="u1",
                )
            ]
        )
        api.get_connection_schemas.return_value = FivetranConnectionSchemas(
            schemas={
                "public": FivetranSchema(
                    name_in_destination="postgres_public",
                    enabled=True,
                    tables={
                        "employee": FivetranTable(
                            name_in_destination="employee",
                            enabled=True,
                        )
                    },
                )
            }
        )
        # Per-table column endpoint — the bulk schemas response only
        # contains user-modified columns, so this is where the full
        # default column set comes from.
        api.get_table_columns.return_value = {
            "id": FivetranColumn(
                name_in_destination="id",
                enabled=True,
                is_primary_key=True,
            )
        }
        api.get_sync_history.return_value = iter([])

        reader = _make_reader(api)

        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        assert len(connectors) == 1
        c = connectors[0]
        assert c.connector_id == "c1"
        assert c.connector_type == "postgres"
        # Table lineage flattened from schemas:
        assert len(c.lineage) == 1
        tl = c.lineage[0]
        assert tl.source_table == "public.employee"
        assert tl.destination_table == "postgres_public.employee"
        # Column lineage from the per-table endpoint: source name from
        # dict key, destination name from `name_in_destination`.
        assert len(tl.column_lineage) == 1
        cl = tl.column_lineage[0]
        assert cl.source_column == "id"
        assert cl.destination_column == "id"
        # And the per-table endpoint was actually consulted.
        api.get_table_columns.assert_called_with("c1", "public", "employee")

    def test_filters_disabled_schemas_and_tables(self):
        # Disabled schemas/tables/columns must not appear in the lineage.
        api = MagicMock()
        api.list_connections.return_value = iter(
            [
                FivetranListedConnection(
                    id="c1",
                    schema_="x",
                    service="postgres",
                    paused=False,
                    sync_frequency=1,
                    group_id="g1",
                )
            ]
        )
        api.get_connection_schemas.return_value = FivetranConnectionSchemas(
            schemas={
                "public": FivetranSchema(
                    name_in_destination="public",
                    enabled=False,  # disabled
                    tables={"t": FivetranTable(name_in_destination="t")},
                ),
                "ok": FivetranSchema(
                    name_in_destination="ok",
                    enabled=True,
                    tables={
                        "disabled_t": FivetranTable(
                            name_in_destination="disabled_t", enabled=False
                        ),
                        "ok_t": FivetranTable(name_in_destination="ok_t"),
                    },
                ),
            }
        )
        api.get_sync_history.return_value = iter([])

        reader = _make_reader(api)
        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )
        c = connectors[0]
        # Only the enabled schema's enabled table is present
        assert {tl.source_table for tl in c.lineage} == {"ok.ok_t"}

    def test_connector_filter_drops_excluded(self):
        api = MagicMock()
        api.list_connections.return_value = iter(
            [
                FivetranListedConnection(
                    id="keep",
                    schema_="s",
                    service="postgres",
                    paused=False,
                    sync_frequency=1,
                    group_id="g1",
                ),
                FivetranListedConnection(
                    id="drop",
                    schema_="s",
                    service="postgres",
                    paused=False,
                    sync_frequency=1,
                    group_id="g1",
                ),
            ]
        )
        api.get_connection_schemas.return_value = FivetranConnectionSchemas()
        api.get_sync_history.return_value = iter([])
        reader = _make_reader(api)
        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern(allow=["keep"]),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )
        assert {c.connector_id for c in connectors} == {"keep"}


class TestGetUserEmailRest:
    def test_returns_cached_email(self):
        api = MagicMock()
        api.list_users.return_value = iter(
            [
                FivetranListedUser(id="u1", email="u1@x"),
                FivetranListedUser(id="u2", email="u2@x"),
            ]
        )
        api.list_groups.return_value = iter([FivetranGroup(id="g1")])

        reader = _make_reader(api)
        reader._group_ids = None  # force discovery once

        assert reader.get_user_email("u1") == "u1@x"
        # Second call — cached, no extra REST call needed
        assert reader.get_user_email("u2") == "u2@x"
        # `list_users` should only have been called once (cache populated bulk)
        assert api.list_users.call_count == 1

    def test_returns_none_for_missing_user(self):
        api = MagicMock()
        api.list_users.return_value = iter([])
        reader = _make_reader(api)
        assert reader.get_user_email("not_in_account") is None

    def test_returns_none_for_none_input(self):
        reader = _make_reader(MagicMock())
        assert reader.get_user_email(None) is None


def _warning_titles(report: FivetranSourceReport) -> List[Optional[str]]:
    return [entry.title for entry in report.warnings]


def _failure_titles(report: FivetranSourceReport) -> List[Optional[str]]:
    return [entry.title for entry in report.failures]


def _entry_with_title(
    report_entries: Iterable[StructuredLogEntry], title: str
) -> StructuredLogEntry:
    for entry in report_entries:
        if entry.title == title:
            return entry
    raise AssertionError(f"No structured-log entry with title {title!r}")


def _context_contains(entry: StructuredLogEntry, needle: str) -> bool:
    """Structured-log `context` is a LossyList[str]; entries are formatted as
    "<context> <exception_class>: <exception_message>" so substring-match
    rather than equality.
    """
    return any(needle in c for c in entry.context)


class TestRestFailurePaths:
    """Failures during REST calls must surface as report.warning/failure
    entries so operators see them in the ingest summary, not get swallowed.
    """

    def test_schema_fetch_failure_routes_to_report_warning(self):
        api = MagicMock()
        api.list_connections.return_value = iter(
            [
                FivetranListedConnection(
                    id="broken_connector",
                    schema_="s",
                    service="postgres",
                    paused=False,
                    sync_frequency=1,
                    group_id="g1",
                )
            ]
        )
        api.get_connection_schemas.side_effect = ValueError("boom")
        api.get_sync_history.return_value = iter([])

        report = FivetranSourceReport()
        reader = _make_reader(api, report=report)

        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        # Connector still emitted — just without lineage.
        assert len(connectors) == 1
        assert connectors[0].lineage == []
        # Exactly one warning, against the right title; no failures spilled in.
        assert _warning_titles(report) == [
            "Failed to fetch Fivetran connection schemas"
        ]
        assert len(report.failures) == 0
        entry = _entry_with_title(
            report.warnings, "Failed to fetch Fivetran connection schemas"
        )
        assert _context_contains(entry, "broken_connector")

    def test_group_discovery_failure_routes_to_report_failure(self):
        # `list_groups` raises ValueError when API returns non-Success code.
        api = MagicMock()
        api.list_groups.side_effect = ValueError(
            "Fivetran API non-success: 'Forbidden'"
        )

        report = FivetranSourceReport()
        reader = _make_reader(api, report=report)
        reader._group_ids = None  # force discovery

        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        # Empty result, but the reason is reported as a structured failure.
        assert connectors == []
        assert _failure_titles(report) == ["Fivetran group discovery failed"]
        assert len(report.warnings) == 0

    def test_group_discovery_network_error_routes_to_report_failure(self):
        # A transport-level failure (e.g. timeout) must also be caught and
        # surfaced as report.failure rather than crashing the run.
        api = MagicMock()
        api.list_groups.side_effect = requests.ConnectionError("network gone")

        report = FivetranSourceReport()
        reader = _make_reader(api, report=report)
        reader._group_ids = None

        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        assert connectors == []
        assert _failure_titles(report) == ["Fivetran group discovery failed"]

    def test_list_connections_failure_skips_group_and_warns(self):
        # First group's list_connections raises; second group succeeds.
        # The connector from the second group must still be emitted.
        api = MagicMock()
        api.list_connections.side_effect = [
            requests.ConnectionError("g1 down"),
            iter(
                [
                    FivetranListedConnection(
                        id="c_in_g2",
                        schema_="s",
                        service="postgres",
                        paused=False,
                        sync_frequency=1,
                        group_id="g2",
                    )
                ]
            ),
        ]
        api.get_connection_schemas.return_value = FivetranConnectionSchemas()
        api.get_sync_history.return_value = iter([])

        report = FivetranSourceReport()
        reader = _make_reader(api, report=report)
        reader._group_ids = ["g1", "g2"]  # bypass discovery

        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        # g1 skipped, g2 emitted.
        assert [c.connector_id for c in connectors] == ["c_in_g2"]
        assert _warning_titles(report) == ["Failed to list Fivetran connections"]
        entry = _entry_with_title(
            report.warnings, "Failed to list Fivetran connections"
        )
        assert _context_contains(entry, "g1")

    def test_list_users_failure_in_one_group_does_not_break_other_groups(self):
        # `list_users` raises for g1; g2 yields a usable user. Email lookup
        # for the g2 user must still succeed; the g1 failure is reported.
        api = MagicMock()

        def _list_users(group_id, *args, **kwargs):
            if group_id == "g1":
                raise requests.ConnectionError("g1 users down")
            return iter([FivetranListedUser(id="u_in_g2", email="ok@x")])

        api.list_users.side_effect = _list_users

        report = FivetranSourceReport()
        reader = _make_reader(api, report=report)
        reader._group_ids = ["g1", "g2"]  # bypass discovery

        # The lookup for u_in_g2 must succeed even though g1 failed.
        assert reader.get_user_email("u_in_g2") == "ok@x"
        assert _warning_titles(report) == ["Failed to list Fivetran group users"]
        entry = _entry_with_title(
            report.warnings, "Failed to list Fivetran group users"
        )
        assert _context_contains(entry, "g1")

    def test_unrelated_exception_still_propagates(self):
        # A programming-error-type exception (not in _RECOVERABLE_REST_ERRORS)
        # must NOT be swallowed by the structured-report path.
        api = MagicMock()
        api.list_connections.side_effect = KeyError("not_recoverable")

        report = FivetranSourceReport()
        reader = _make_reader(api, report=report)
        reader._group_ids = ["g1"]

        with pytest.raises(KeyError):
            reader.get_allowed_connectors_list(
                connector_patterns=AllowDenyPattern.allow_all(),
                destination_patterns=AllowDenyPattern.allow_all(),
                syncs_interval=7,
            )


class TestRestParityWithDbReader:
    """Bug-fix regression tests: REST mode must produce the same display name
    and the same sync-history slicing as the DB-based reader.
    """

    def test_connector_name_uses_listed_schema_not_id(self):
        # The DB reader pulls `connection.connection_name` (the user-facing
        # destination schema). REST mode must match that — using `listed.id`
        # would lose the human-readable name in the DataHub UI.
        api = MagicMock()
        api.list_connections.return_value = iter(
            [
                FivetranListedConnection(
                    id="postgres_test_internal_id",
                    schema_="postgres_public",
                    service="postgres",
                    paused=False,
                    sync_frequency=1440,
                    group_id="g1",
                )
            ]
        )
        api.get_connection_schemas.return_value = FivetranConnectionSchemas()
        api.get_sync_history.return_value = iter([])

        reader = _make_reader(api)
        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        assert len(connectors) == 1
        c = connectors[0]
        assert c.connector_id == "postgres_test_internal_id"
        # The display name must come from `schema`, not from the id.
        assert c.connector_name == "postgres_public"


def _wide_schema(num_tables: int, cols_per_table: int) -> FivetranConnectionSchemas:
    """Construct a `FivetranConnectionSchemas` with a tunable number of
    enabled tables and columns. Used to exercise the lineage caps.
    """
    return FivetranConnectionSchemas(
        schemas={
            "public": FivetranSchema(
                name_in_destination="public",
                enabled=True,
                tables={
                    f"t_{i}": FivetranTable(
                        name_in_destination=f"t_{i}",
                        enabled=True,
                        columns={
                            f"c_{j}": FivetranColumn(
                                name_in_destination=f"c_{j}", enabled=True
                            )
                            for j in range(cols_per_table)
                        },
                    )
                    for i in range(num_tables)
                },
            )
        }
    )


class TestScalingGuards:
    """Bounds against pathological accounts: very wide tables, very many
    out-of-window sync runs, and many disallowed groups.
    """

    def test_table_lineage_capped_with_warning(self):
        # 200 tables, cap at 50 — only 50 lineage entries should make it,
        # and a warning should be emitted naming the connector.
        api = MagicMock()
        api.list_connections.return_value = iter(
            [
                FivetranListedConnection(
                    id="wide_connector",
                    schema_="s",
                    service="postgres",
                    paused=False,
                    sync_frequency=1,
                    group_id="g1",
                )
            ]
        )
        api.get_connection_schemas.return_value = _wide_schema(
            num_tables=200, cols_per_table=2
        )
        api.get_sync_history.return_value = iter([])

        report = FivetranSourceReport()
        reader = _make_reader(api, report=report)
        reader._max_table_lineage_per_connector = 50

        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        assert len(connectors[0].lineage) == 50
        titles = [e.title for e in report.warnings]
        assert "Fivetran connector table lineage truncated" in titles

    def test_column_lineage_capped_globally_with_warning(self):
        # 10 tables × 100 columns each = 1,000 columns; cap at 200.
        # Drives the schemas-config endpoint (REST mode's only column-
        # lineage source after the dead Metadata API path was removed).
        api = MagicMock()
        api.list_connections.return_value = iter(
            [
                FivetranListedConnection(
                    id="wide_cols",
                    schema_="s",
                    service="postgres",
                    paused=False,
                    sync_frequency=1,
                    group_id="g1",
                )
            ]
        )
        api.get_connection_schemas.return_value = _wide_schema(
            num_tables=10, cols_per_table=100
        )
        # Per-table column fetch — return the same 100 columns the
        # _wide_schema fixture's tables are named with.
        api.get_table_columns.return_value = {
            f"c_{j}": FivetranColumn(name_in_destination=f"c_{j}", enabled=True)
            for j in range(100)
        }
        api.get_sync_history.return_value = iter([])

        report = FivetranSourceReport()
        reader = _make_reader(api, report=report)
        reader._max_column_lineage_per_connector = 200

        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        total_cols = sum(len(t.column_lineage) for t in connectors[0].lineage)
        assert total_cols == 200
        titles = [e.title for e in report.warnings]
        assert "Fivetran connector column lineage truncated" in titles

    def test_destination_patterns_skip_list_connections_calls(self):
        # Two groups; destination_patterns disallows g1. We must NOT call
        # list_connections on g1 at all.
        api = MagicMock()
        api.list_connections.return_value = iter(
            [
                FivetranListedConnection(
                    id="c_in_g2",
                    schema_="s",
                    service="postgres",
                    paused=False,
                    sync_frequency=1,
                    group_id="g2",
                )
            ]
        )
        api.get_connection_schemas.return_value = FivetranConnectionSchemas()
        api.get_sync_history.return_value = iter([])

        report = FivetranSourceReport()
        reader = _make_reader(api, report=report)
        reader._group_ids = ["g1", "g2"]

        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern(allow=["g2"]),
            syncs_interval=7,
        )

        assert [c.connector_id for c in connectors] == ["c_in_g2"]
        # list_connections should have been called once only — for g2.
        assert api.list_connections.call_count == 1
        api.list_connections.assert_called_with("g2")

    def test_default_page_size_is_500(self):
        # Sanity check that the default was bumped from 100 → 500 across
        # paginated REST methods. Detects accidental regression to 100.
        for method_name in (
            "list_groups",
            "list_connections",
            "list_users",
        ):
            sig = inspect.signature(getattr(FivetranAPIClient, method_name))
            assert sig.parameters["page_size"].default == 500, (
                f"{method_name} page_size default should be 500"
            )


def _listed(connector_id: str, group_id: str = "g1") -> FivetranListedConnection:
    return FivetranListedConnection(
        id=connector_id,
        schema_=connector_id,
        service="postgres",
        paused=False,
        sync_frequency=1,
        group_id=group_id,
    )


class TestParallelism:
    """Per-connector schema + sync_history fetches run in a ThreadPoolExecutor.
    Verify (a) ordering is preserved, (b) the report machinery doesn't lose
    warnings under concurrency, (c) max_workers=1 stays sequential.
    """

    def test_output_order_matches_listed_connections(self):
        # 20 connectors; with max_workers=4 the result list must still match
        # the API's listing order (which our mock returns in submission
        # order). Goldenfile-friendly determinism.
        ids = [f"c_{i:02d}" for i in range(20)]
        api = MagicMock()
        api.list_connections.return_value = iter([_listed(i) for i in ids])
        api.get_connection_schemas.return_value = FivetranConnectionSchemas()
        api.get_sync_history.return_value = iter([])

        reader = _make_reader(api, max_workers=4)
        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )
        assert [c.connector_id for c in connectors] == ids

    def test_warnings_from_concurrent_workers_all_recorded(self):
        # Every worker triggers a schema-fetch failure simultaneously; all
        # warnings must surface. Ensures the report.warning lock works.
        ids = [f"failing_{i}" for i in range(15)]
        api = MagicMock()
        api.list_connections.return_value = iter([_listed(i) for i in ids])
        # Every schema fetch raises a recoverable error.
        api.get_connection_schemas.side_effect = ValueError("boom")
        api.get_sync_history.return_value = iter([])

        report = FivetranSourceReport()
        reader = _make_reader(api, report=report, max_workers=8)
        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        # All connectors emitted (with empty lineage).
        assert len(connectors) == 15
        # All 15 contexts represented in the structured-log entry's context.
        entry = _entry_with_title(
            report.warnings, "Failed to fetch Fivetran connection schemas"
        )
        # `LossyList[str]` may sample down, but every recorded context
        # must be one of our connector ids — i.e., the lock kept the
        # entries well-formed.
        for c in entry.context:
            assert any(i in c for i in ids), f"unexpected context fragment: {c}"

    def test_max_workers_one_is_sequential(self):
        # Sanity: max_workers=1 still works (degrades to single-thread
        # ThreadPoolExecutor, no special-casing needed).
        ids = [f"c_{i}" for i in range(5)]
        api = MagicMock()
        api.list_connections.return_value = iter([_listed(i) for i in ids])
        api.get_connection_schemas.return_value = FivetranConnectionSchemas()
        api.get_sync_history.return_value = iter([])

        reader = _make_reader(api, max_workers=1)
        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )
        assert [c.connector_id for c in connectors] == ids

    def test_unrelated_exception_in_worker_propagates(self):
        # A non-recoverable exception (KeyError) raised inside a worker
        # must propagate to the main thread, not be silently swallowed.
        api = MagicMock()
        api.list_connections.return_value = iter([_listed("crashy")])
        api.get_connection_schemas.side_effect = KeyError("programming bug")
        api.get_sync_history.return_value = iter([])

        reader = _make_reader(api, max_workers=4)
        with pytest.raises(KeyError):
            reader.get_allowed_connectors_list(
                connector_patterns=AllowDenyPattern.allow_all(),
                destination_patterns=AllowDenyPattern.allow_all(),
                syncs_interval=7,
            )


class TestPerConnectorTimeout:
    """A single hung HTTP call shouldn't stall the whole ingest. Per-future
    timeouts skip the offending connector with a warning, the rest continue.
    """

    def test_timeout_skips_connector_other_workers_continue(self):
        # Connector A's schema fetch hangs; connector B is fine. With a
        # short per_connector_timeout_sec, A is skipped (warning) and B
        # is emitted normally.
        api = MagicMock()
        api.list_connections.return_value = iter([_listed("hangs"), _listed("fine")])
        api.get_sync_history.return_value = iter([])

        def _maybe_hang(connection_id: str) -> FivetranConnectionSchemas:
            if connection_id == "hangs":
                # Sleep longer than the test's timeout — we expect the
                # main thread to abandon this future and move on.
                time.sleep(2.0)
            return FivetranConnectionSchemas()

        api.get_connection_schemas.side_effect = _maybe_hang

        report = FivetranSourceReport()
        reader = _make_reader(
            api,
            report=report,
            max_workers=2,
            per_connector_timeout_sec=1,  # 1-second cap
        )

        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        # `fine` got through; `hangs` was abandoned.
        ids = [c.connector_id for c in connectors]
        assert "fine" in ids
        assert "hangs" not in ids
        titles = [e.title for e in report.warnings]
        assert "Fivetran connector REST fetch timed out" in titles
        # Warning context names the connector that was skipped.
        entry = _entry_with_title(
            report.warnings, "Fivetran connector REST fetch timed out"
        )
        assert _context_contains(entry, "hangs")

    def test_fail_fast_on_programming_bug_does_not_wait_for_pool_drain(self):
        # If a worker raises a non-recoverable exception, the main thread
        # should propagate quickly — `shutdown(wait=False, cancel_futures=True)`
        # ensures unstarted tasks are cancelled instead of all workers
        # draining. We can't easily measure wall-clock here without
        # flakiness, but we CAN assert that the exception propagates and
        # that subsequent `connectors_scanned` increments don't fire.
        all_done = threading.Event()
        # Build many slow connectors. With max_workers=2, only 2 are in
        # flight at any time; the remaining 98 are pending — those should
        # be cancelled when the first connector raises.
        ids = [f"c_{i:03d}" for i in range(100)]

        def _slow_or_crash(connection_id: str) -> FivetranConnectionSchemas:
            if connection_id == "c_000":
                raise KeyError("crash early")
            # Sleep long enough that without cancel_futures=True we'd be
            # waiting many seconds on pool drain.
            all_done.wait(timeout=10.0)
            return FivetranConnectionSchemas()

        api = MagicMock()
        api.list_connections.return_value = iter([_listed(i) for i in ids])
        api.get_connection_schemas.side_effect = _slow_or_crash
        api.get_sync_history.return_value = iter([])

        report = FivetranSourceReport()
        reader = _make_reader(
            api, report=report, max_workers=2, per_connector_timeout_sec=30
        )

        try:
            with pytest.raises(KeyError):
                reader.get_allowed_connectors_list(
                    connector_patterns=AllowDenyPattern.allow_all(),
                    destination_patterns=AllowDenyPattern.allow_all(),
                    syncs_interval=7,
                )
        finally:
            # Wake up anything still sleeping so test cleanup is fast.
            all_done.set()

        # connectors_scanned should be 0 — the exception propagated before
        # any successful connector was appended.
        assert report.connectors_scanned == 0


class TestHybridDbJobs:
    """REST mode + DB-log jobs: REST builds the connector list and lineage,
    a `db_log_reader` populates the per-run sync history afterwards.
    """

    def test_db_jobs_populated_when_db_log_reader_set(self):
        from datahub.ingestion.source.fivetran.data_classes import Job

        api = MagicMock()
        api.list_connections.return_value = iter(
            [
                FivetranListedConnection(
                    id="c_with_jobs",
                    schema_="s",
                    service="postgres",
                    paused=False,
                    sync_frequency=1,
                    group_id="g1",
                )
            ]
        )
        api.get_connection_schemas.return_value = FivetranConnectionSchemas()

        db_reader = MagicMock()
        db_reader.fetch_jobs_for_connectors.return_value = {
            "c_with_jobs": [
                Job(
                    job_id="run-1",
                    start_time=1000,
                    end_time=2000,
                    status="SUCCESSFUL",
                ),
                Job(
                    job_id="run-2",
                    start_time=3000,
                    end_time=4000,
                    status="SUCCESSFUL",
                ),
            ]
        }

        reader = _make_reader(api)
        reader._db_log_reader = db_reader

        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        assert len(connectors) == 1
        assert [j.job_id for j in connectors[0].jobs] == ["run-1", "run-2"]
        # Single batched call covering all connectors.
        db_reader.fetch_jobs_for_connectors.assert_called_once_with(["c_with_jobs"], 7)

    def test_db_log_reader_failure_warns_and_continues(self):
        api = MagicMock()
        api.list_connections.return_value = iter(
            [
                FivetranListedConnection(
                    id="c1",
                    schema_="s",
                    service="postgres",
                    paused=False,
                    sync_frequency=1,
                    group_id="g1",
                )
            ]
        )
        api.get_connection_schemas.return_value = FivetranConnectionSchemas()

        db_reader = MagicMock()
        # Realistic transient failure: SQLAlchemy raises OperationalError
        # on connection drops / timeouts. The narrow except tuple in the
        # production code intentionally handles this and lets non-DB
        # exceptions (programmer bugs) propagate.
        db_reader.fetch_jobs_for_connectors.side_effect = (
            sqlalchemy.exc.OperationalError(
                statement="SELECT ...",
                params=None,
                orig=Exception("snowflake timeout"),
            )
        )

        report = FivetranSourceReport()
        reader = _make_reader(api, report=report)
        reader._db_log_reader = db_reader

        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        assert len(connectors) == 1
        assert connectors[0].jobs == []
        titles = [e.title for e in report.warnings]
        assert "Failed to fetch Fivetran sync history from log warehouse" in titles

    def test_db_log_reader_warehouse_data_drift_warns_and_continues(self):
        # `KeyError` and `AttributeError` from the row-mapper are the
        # canonical "warehouse log table shape drifted" failures (Fivetran
        # renamed a column, NULL `start_time` from an in-flight sync, etc).
        # These should degrade gracefully — REST mode still emits structural
        # metadata, just without the per-run DPI events.
        api = MagicMock()
        api.list_connections.return_value = iter(
            [
                FivetranListedConnection(
                    id="c1",
                    schema_="s",
                    service="postgres",
                    paused=False,
                    sync_frequency=1,
                    group_id="g1",
                )
            ]
        )
        api.get_connection_schemas.return_value = FivetranConnectionSchemas()

        db_reader = MagicMock()
        db_reader.fetch_jobs_for_connectors.side_effect = KeyError(
            "row['connection_id'] missing — Fivetran renamed the column?"
        )

        report = FivetranSourceReport()
        reader = _make_reader(api, report=report)
        reader._db_log_reader = db_reader

        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        assert len(connectors) == 1
        assert connectors[0].jobs == []
        titles = [e.title for e in report.warnings]
        assert "Failed to fetch Fivetran sync history from log warehouse" in titles

    def test_db_log_reader_programmer_bug_propagates(self):
        # The except tuple intentionally lets non-data-drift programmer
        # bugs propagate so a future regression in `fetch_jobs_for_connectors`
        # surfaces loudly instead of being hidden behind a "we successfully
        # fell back" warning. `TypeError` is the canonical example — it
        # comes from calling code with the wrong argument types, not from
        # warehouse data shape changes.
        api = MagicMock()
        api.list_connections.return_value = iter(
            [
                FivetranListedConnection(
                    id="c1",
                    schema_="s",
                    service="postgres",
                    paused=False,
                    sync_frequency=1,
                    group_id="g1",
                )
            ]
        )
        api.get_connection_schemas.return_value = FivetranConnectionSchemas()

        db_reader = MagicMock()
        db_reader.fetch_jobs_for_connectors.side_effect = TypeError(
            "fetch_jobs_for_connectors() got an unexpected keyword argument"
        )

        report = FivetranSourceReport()
        reader = _make_reader(api, report=report)
        reader._db_log_reader = db_reader

        with pytest.raises(TypeError, match="unexpected keyword argument"):
            reader.get_allowed_connectors_list(
                connector_patterns=AllowDenyPattern.allow_all(),
                destination_patterns=AllowDenyPattern.allow_all(),
                syncs_interval=7,
            )

    def test_no_dpi_info_emitted_when_db_log_reader_set(self):
        # The "REST mode does not emit DPI events" info message must be
        # suppressed when hybrid mode is active.
        api = MagicMock()
        api.list_connections.return_value = iter([])

        db_reader = MagicMock()
        report = FivetranSourceReport()
        reader = _make_reader(api, report=report)
        reader._db_log_reader = db_reader

        reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        info_titles = [e.title for e in report.infos]
        assert "REST mode does not emit DPI events for sync runs" not in info_titles


class TestRestLineageFallback:
    """REST-mode lineage fallback chain:

    1. DB log (when ``db_lineage_reader`` is wired) — preferred.
    2. REST schemas-config endpoint + per-table column fetch.

    The dead ``/v1/metadata/connectors/...`` endpoint and its
    ``name_in_source`` plumbing were removed: they never existed in
    Fivetran's REST API. The schemas-config endpoint's nested dict
    keys are the source-side identifiers per the OpenAPI spec.
    """

    def test_schemas_config_emits_column_lineage_with_source_names(self):
        # No db_lineage_reader → REST gets the schemas/tables list from
        # the bulk endpoint and the per-table column dicts from the
        # per-table endpoint. Source names come from dict keys per the
        # OpenAPI spec; destination names from `name_in_destination`.
        api = MagicMock()
        api.list_connections.return_value = iter(
            [
                FivetranListedConnection(
                    id="c1",
                    schema_="postgres_public",
                    service="postgres",
                    paused=False,
                    sync_frequency=1440,
                    group_id="g1",
                )
            ]
        )
        api.get_connection_schemas.return_value = FivetranConnectionSchemas(
            schemas={
                # Source-side schema name = "public", destination = "postgres_public"
                "public": FivetranSchema(
                    name_in_destination="postgres_public",
                    enabled=True,
                    tables={
                        # Source table = "Users", destination = "users"
                        "Users": FivetranTable(
                            name_in_destination="users",
                            enabled=True,
                        )
                    },
                )
            }
        )
        # The per-table /columns endpoint returns the full column set,
        # including columns the user hasn't explicitly modified.
        api.get_table_columns.return_value = {
            "userID": FivetranColumn(
                name_in_destination="user_id",
                enabled=True,
                is_primary_key=True,
            ),
            "email": FivetranColumn(
                name_in_destination="email",
                enabled=True,
            ),
        }

        reader = _make_reader(api)
        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        assert len(connectors) == 1
        assert len(connectors[0].lineage) == 1
        tl = connectors[0].lineage[0]
        # Source names from dict keys, destination names from name_in_destination.
        assert tl.source_table == "public.Users"
        assert tl.destination_table == "postgres_public.users"
        cols = {(cl.source_column, cl.destination_column) for cl in tl.column_lineage}
        assert cols == {("userID", "user_id"), ("email", "email")}
        # Per-table endpoint was called for the one emitted table.
        api.get_table_columns.assert_called_once_with("c1", "public", "Users")

    def test_per_table_columns_fetch_failure_falls_back_to_inline(self):
        # If the per-table /columns endpoint fails (transient error),
        # the connector falls back to the (often empty) inline columns
        # from the bulk schemas endpoint and emits a one-shot warning.
        api = MagicMock()
        api.list_connections.return_value = iter(
            [
                FivetranListedConnection(
                    id="c1",
                    schema_="s",
                    service="postgres",
                    paused=False,
                    sync_frequency=1,
                    group_id="g1",
                )
            ]
        )
        api.get_connection_schemas.return_value = FivetranConnectionSchemas(
            schemas={
                "public": FivetranSchema(
                    name_in_destination="public",
                    enabled=True,
                    tables={
                        "Users": FivetranTable(
                            name_in_destination="Users",
                            enabled=True,
                            # User-modified columns are inline; the rest
                            # would come from the per-table endpoint.
                            columns={
                                "id": FivetranColumn(
                                    name_in_destination="id",
                                    enabled=True,
                                )
                            },
                        )
                    },
                )
            }
        )
        # Per-table endpoint fails — fall back to inline.
        api.get_table_columns.side_effect = ValueError("API error")

        report = FivetranSourceReport()
        reader = _make_reader(api, report=report)

        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        # Lineage emitted with the inline column.
        assert len(connectors[0].lineage) == 1
        cols = {cl.source_column for cl in connectors[0].lineage[0].column_lineage}
        assert cols == {"id"}
        # One-shot warning surfaced.
        warning_titles = [w.title for w in report.warnings]
        assert "Failed to fetch per-table column metadata" in warning_titles

    def test_db_lineage_reader_used_first_when_wired(self):
        # When db_lineage_reader is set, REST goes to it directly and
        # never hits the schemas-config endpoint.
        from datahub.ingestion.source.fivetran.data_classes import (
            ColumnLineage,
            TableLineage,
        )

        api = MagicMock()
        api.list_connections.return_value = iter(
            [
                FivetranListedConnection(
                    id="c1",
                    schema_="postgres_public",
                    service="postgres",
                    paused=False,
                    sync_frequency=1440,
                    group_id="g1",
                )
            ]
        )

        db_lineage_reader = MagicMock()
        db_lineage_reader.fetch_lineage_for_connectors.return_value = {
            "c1": [
                TableLineage(
                    source_table="public.Users",
                    destination_table="postgres_public.users",
                    column_lineage=[
                        ColumnLineage(
                            source_column="userID", destination_column="user_id"
                        ),
                    ],
                )
            ]
        }

        reader = _make_reader(api)
        reader._db_lineage_reader = db_lineage_reader

        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        assert len(connectors[0].lineage) == 1
        assert connectors[0].lineage[0].column_lineage[0].source_column == "userID"
        # The schemas-config endpoint must not have been touched —
        # DB log was the sole lineage source.
        api.get_connection_schemas.assert_not_called()

    def test_db_lineage_empty_for_connector_falls_through_to_rest(self):
        # Hybrid mode with a connector visible to REST but whose log
        # lives in a *different* warehouse — DB lineage query succeeds
        # but returns no rows for this connector_id. Must fall through
        # to REST (schemas-config + per-table /columns) rather than
        # silently dropping lineage. This is the real-world case for
        # accounts whose connectors fan out across multiple destination
        # warehouses with a single Platform Connector log.
        api = MagicMock()
        api.list_connections.return_value = iter(
            [
                FivetranListedConnection(
                    id="connector_in_other_warehouse",
                    schema_="postgres_public",
                    service="postgres",
                    paused=False,
                    sync_frequency=1440,
                    group_id="g1",
                )
            ]
        )
        api.get_connection_schemas.return_value = FivetranConnectionSchemas(
            schemas={
                "public": FivetranSchema(
                    name_in_destination="postgres_public",
                    enabled=True,
                    tables={
                        "Users": FivetranTable(
                            name_in_destination="users",
                            enabled=True,
                        )
                    },
                )
            }
        )
        api.get_table_columns.return_value = {
            "id": FivetranColumn(name_in_destination="id", enabled=True),
            "email": FivetranColumn(name_in_destination="email", enabled=True),
        }

        # DB lineage reader returns empty for this connector — its log
        # lives in a different warehouse, not the configured one.
        db_lineage_reader = MagicMock()
        db_lineage_reader.fetch_lineage_for_connectors.return_value = {}

        reader = _make_reader(api)
        reader._db_lineage_reader = db_lineage_reader

        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        assert len(connectors) == 1
        # REST schemas-config + per-table /columns produced lineage even
        # though DB log had nothing for this connector.
        assert len(connectors[0].lineage) == 1
        cols = {cl.source_column for cl in connectors[0].lineage[0].column_lineage}
        assert cols == {"id", "email"}
        # Both code paths exercised: DB tried first, REST took over.
        db_lineage_reader.fetch_lineage_for_connectors.assert_called_once()
        api.get_connection_schemas.assert_called_once_with(
            "connector_in_other_warehouse"
        )

    def test_db_lineage_reader_failure_falls_through_to_schemas_config(self):
        # If the DB lineage reader errors transiently, REST falls back to
        # the schemas-config endpoint rather than emitting empty lineage.
        api = MagicMock()
        api.list_connections.return_value = iter(
            [
                FivetranListedConnection(
                    id="c1",
                    schema_="postgres_public",
                    service="postgres",
                    paused=False,
                    sync_frequency=1440,
                    group_id="g1",
                )
            ]
        )
        api.get_connection_schemas.return_value = FivetranConnectionSchemas(
            schemas={
                "public": FivetranSchema(
                    name_in_destination="postgres_public",
                    enabled=True,
                    tables={
                        "Users": FivetranTable(
                            name_in_destination="users",
                            enabled=True,
                        )
                    },
                )
            }
        )
        # Per-table /columns returns the full column set.
        api.get_table_columns.return_value = {
            "id": FivetranColumn(name_in_destination="id", enabled=True),
        }

        db_lineage_reader = MagicMock()
        db_lineage_reader.fetch_lineage_for_connectors.side_effect = (
            sqlalchemy.exc.OperationalError(
                statement="SELECT ...",
                params=None,
                orig=Exception("snowflake timeout"),
            )
        )

        report = FivetranSourceReport()
        reader = _make_reader(api, report=report)
        reader._db_lineage_reader = db_lineage_reader

        connectors = reader.get_allowed_connectors_list(
            connector_patterns=AllowDenyPattern.allow_all(),
            destination_patterns=AllowDenyPattern.allow_all(),
            syncs_interval=7,
        )

        # Lineage from schemas-config — column lineage included via
        # the per-table endpoint.
        assert len(connectors[0].lineage) == 1
        assert connectors[0].lineage[0].source_table == "public.Users"
        assert len(connectors[0].lineage[0].column_lineage) == 1
        # The DB-fallback failure surfaces as a one-shot warning.
        warning_titles = [w.title for w in report.warnings]
        assert "DB log lineage fallback failed" in warning_titles
