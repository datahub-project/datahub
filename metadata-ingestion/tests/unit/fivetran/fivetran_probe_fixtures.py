"""One Fivetran account, served to ingestion and the probe alike: three
connectors, `conn_a1` sales_pg and `conn_a2` sheets on `dest_a`, `conn_b1`
hr_pg on `dest_b`, read from the log warehouse or the REST API."""

import datetime
from contextlib import ExitStack, contextmanager
from typing import Any, Dict, Iterator, List
from unittest import mock
from unittest.mock import MagicMock

import requests

from datahub.ingestion.source.fivetran.fivetran_rest_api import FivetranAPIClient
from datahub.ingestion.source.fivetran.response_models import (
    FivetranConnectionSchemas,
    FivetranDestinationDetails,
    FivetranGroup,
    FivetranListedConnection,
    FivetranListedUser,
)

_CONNECTOR_ROWS: List[Dict[str, object]] = [
    {
        "connection_id": "conn_a1",
        "connecting_user_id": "user_x",
        "connector_type_id": "postgres",
        "connection_name": "sales_pg",
        "paused": False,
        "sync_frequency": 1440,
        "destination_id": "dest_a",
    },
    {
        "connection_id": "conn_b1",
        "connecting_user_id": "user_x",
        "connector_type_id": "mysql",
        "connection_name": "hr_pg",
        "paused": True,
        "sync_frequency": 360,
        "destination_id": "dest_b",
    },
    {
        "connection_id": "conn_a2",
        "connecting_user_id": "user_x",
        "connector_type_id": "google_sheets",
        "connection_name": "sheets",
        "paused": False,
        "sync_frequency": 360,
        "destination_id": "dest_a",
    },
]


_TABLE_LINEAGE_ROWS: List[Dict[str, object]] = [
    {
        "connection_id": connection_id,
        "source_table_id": f"{connection_id}_st",
        "source_table_name": "orders",
        "source_schema_name": "public",
        "destination_table_id": f"{connection_id}_dt",
        "destination_table_name": "orders",
        "destination_schema_name": "sales",
        "created_at": datetime.datetime(2026, 1, 1),
    }
    for connection_id in ("conn_a1", "conn_a2")
]

_COLUMN_LINEAGE_ROWS: List[Dict[str, object]] = [
    {
        "source_table_id": f"{connection_id}_st",
        "destination_table_id": f"{connection_id}_dt",
        "source_column_name": "id",
        "destination_column_name": "id",
    }
    for connection_id in ("conn_a1", "conn_a2")
]

_SYNC_ROWS: List[Dict[str, object]] = [
    {
        "connection_id": "conn_a1",
        "sync_id": "sync_1",
        "start_time": datetime.datetime(2026, 1, 1, 10, 0),
        "end_time": datetime.datetime(2026, 1, 1, 10, 5),
        "end_message_data": '"{\\"status\\":\\"SUCCESSFUL\\"}"',
    }
]


def _route(query: str) -> List[Dict[str, object]]:
    # Order matters: the column query joins source_table, the sync query
    # reads the log table, and only the connectors query names connection_name.
    if "ranked_syncs" in query:
        return _SYNC_ROWS
    if "column_lineage" in query:
        return _COLUMN_LINEAGE_ROWS
    if "table_lineage" in query:
        return _TABLE_LINEAGE_ROWS
    if "connection_name" in query:
        return _CONNECTOR_ROWS
    return []


def db_recipe(**overrides: object) -> Dict[str, object]:
    recipe: Dict[str, object] = {
        "fivetran_log_config": {
            "destination_platform": "snowflake",
            "snowflake_destination_config": {
                "account_id": "acct",
                "username": "u",
                "password": "p",
                "warehouse": "wh",
                "database": "log_db",
                "log_schema": "log_schema",
            },
        },
    }
    recipe.update(overrides)
    return recipe


def _execute(clause: Any, *args: Any, **kwargs: Any) -> MagicMock:
    # FivetranLogDbReader._query runs `conn.execute(text(q))` (SQLAlchemy 2.0)
    # and reads each row through `row._mapping`.
    query = clause.text if hasattr(clause, "text") else str(clause)
    result = MagicMock()
    result.__iter__.return_value = iter(
        [MagicMock(_mapping=row) for row in _route(query)]
    )
    return result


@contextmanager
def mocked_log_db() -> Iterator[MagicMock]:
    """The log warehouse, answering every query from the rows above. Yields
    the patched create_engine."""
    # event.listens_for is patched too: the Snowflake reader registers a
    # connect listener, which SQLAlchemy refuses on a MagicMock engine.
    with (
        mock.patch(
            "datahub.ingestion.source.fivetran.fivetran_log_db_reader.create_engine"
        ) as create_engine,
        mock.patch(
            "datahub.ingestion.source.fivetran.fivetran_log_db_reader.event.listens_for",
            lambda *args, **kwargs: lambda fn: fn,
        ),
    ):
        conn = create_engine.return_value.connect.return_value.__enter__.return_value
        conn.execute.side_effect = _execute
        yield create_engine


API_CONFIG: Dict[str, str] = {"api_key": "k", "api_secret": "s"}

_GROUPS = [
    FivetranGroup(id="dest_a", name="Warehouse A"),
    FivetranGroup(id="dest_b", name="Warehouse B"),
]

# The same connectors as _CONNECTOR_ROWS. sheets is served as postgres: as a
# Google Sheets connector, REST ingestion would also fetch its connection
# details, and selection does not read the connector type.
_LISTED = [
    FivetranListedConnection(
        id=str(row["connection_id"]),
        schema_=str(row["connection_name"]),
        service="postgres",
        paused=bool(row["paused"]),
        sync_frequency=int(str(row["sync_frequency"])),
        group_id=str(row["destination_id"]),
        connected_by=str(row["connecting_user_id"]),
    )
    for row in _CONNECTOR_ROWS
]


def _list_groups(
    self: FivetranAPIClient, page_size: int = 500
) -> Iterator[FivetranGroup]:
    return iter(_GROUPS)


def _list_connections(
    self: FivetranAPIClient, group_id: str, page_size: int = 500
) -> Iterator[FivetranListedConnection]:
    return iter([c for c in _LISTED if c.group_id == group_id])


def _get_connection_schemas(
    self: FivetranAPIClient, connection_id: str
) -> FivetranConnectionSchemas:
    return FivetranConnectionSchemas()


def _get_table_columns(
    self: FivetranAPIClient, connection_id: str, schema: str, table: str
) -> Dict[str, Any]:
    return {}


def _list_users(
    self: FivetranAPIClient, group_id: str, page_size: int = 100
) -> Iterator[FivetranListedUser]:
    return iter([FivetranListedUser(id="user_x")])


def _get_destination_details_by_id(
    self: FivetranAPIClient, destination_id: str
) -> FivetranDestinationDetails:
    return FivetranDestinationDetails(id=destination_id, service="snowflake")


def _no_http(*args: Any, **kwargs: Any) -> None:
    raise AssertionError("the mocked Fivetran API made an unpatched HTTP request")


@contextmanager
def mocked_rest_api() -> Iterator[None]:
    """The REST API, patched on the client rather than on raw HTTP: ingestion's
    REST reader calls endpoints the probe never does. Any request that slips
    past the patches fails instead of reaching api.fivetran.com."""
    patches = {
        "list_groups": _list_groups,
        "list_connections": _list_connections,
        "get_connection_schemas": _get_connection_schemas,
        "get_table_columns": _get_table_columns,
        "list_users": _list_users,
        "get_destination_details_by_id": _get_destination_details_by_id,
    }
    with ExitStack() as stack:
        for name, fake in patches.items():
            stack.enter_context(
                mock.patch.object(
                    FivetranAPIClient, name, autospec=True, side_effect=fake
                )
            )
        stack.enter_context(
            mock.patch.object(requests.Session, "request", side_effect=_no_http)
        )
        yield
