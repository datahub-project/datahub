"""Shape tests for the Snowflake and BigQuery catalog-query surfaces.

Fakes, not live warehouses: these mirror the row shapes each client returns
(Snowflake's DictCursor, BigQuery's RowIterator + schema), so they pin the
mapping and the limit handling but do not prove either client's real behaviour.

Both expose the query as a `sql` probe method whose `query` parameter the
framework scope-checks before calling (see test_scoped_probe_methods).
"""

from types import SimpleNamespace
from typing import Dict, List, Optional

import pytest
from google.api_core.exceptions import Forbidden, Unauthorized

from datahub.ingestion.agent.probe_methods import ProbeMethodSpec
from datahub.ingestion.source.bigquery_v2.bigquery_probe import BigQueryMetadataProbe
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.snowflake.snowflake_probe import SnowflakeMetadataProbe


class FakeSnowflakeConnection:
    """SnowflakeConnection.query uses a DictCursor, so rows arrive as dicts."""

    def __init__(self, rows: List[Dict[str, object]]) -> None:
        self._rows = rows
        self.closed = False

    def query(self, sql: str) -> object:
        return iter(self._rows)

    def close(self) -> None:
        self.closed = True


class _Field:
    def __init__(self, name: str) -> None:
        self.name = name


class FakeBigQueryClient:
    def __init__(self, columns: List[str], rows: List[Dict[str, object]]) -> None:
        self._columns = columns
        self._rows = rows
        self.max_results_seen: Optional[int] = None
        self.job_config_seen: object = None
        self.closed = False

    # Signature mirrors google.cloud.bigquery.Client.query, which takes
    # job_config -- a narrower fake passes while the real client raises.
    def query(self, sql: str, job_config: object = None) -> object:
        client = self
        client.job_config_seen = job_config

        class _Iterator:
            def __init__(self, capped: List[Dict[str, object]]) -> None:
                self.schema = [_Field(c) for c in client._columns]
                self._capped = capped

            def __iter__(self) -> object:
                return iter(self._capped)

        class _Job:
            def result(self, max_results: int, timeout: Optional[int] = None) -> object:
                client.max_results_seen = max_results
                return _Iterator(client._rows[:max_results])

        return _Job()

    def close(self) -> None:
        self.closed = True


def test_snowflake_maps_dict_rows_to_columns_and_values() -> None:
    conn = FakeSnowflakeConnection(
        [
            {"TABLE_NAME": "ORDERS", "ROW_COUNT": 3},
            {"TABLE_NAME": "USERS", "ROW_COUNT": 1},
        ]
    )
    with SnowflakeMetadataProbe(conn) as probe:
        assert probe.sql_dialect == "snowflake"
        rows = probe.sql("SELECT 1", 10)
    assert rows["columns"] == ["TABLE_NAME", "ROW_COUNT"]
    assert rows["rows"] == [["ORDERS", 3], ["USERS", 1]]
    assert conn.closed


def test_snowflake_stops_at_the_limit() -> None:
    conn = FakeSnowflakeConnection([{"N": i} for i in range(10)])
    rows = SnowflakeMetadataProbe(conn).sql("SELECT 1", 3)
    assert rows["rows"] == [[0], [1], [2]]


def test_bigquery_caps_the_page_it_asks_the_server_for() -> None:
    client = FakeBigQueryClient(
        ["table_name"], [{"table_name": f"t{i}"} for i in range(9)]
    )
    with BigQueryMetadataProbe(client) as probe:
        assert probe.sql_dialect == "bigquery"
        rows = probe.sql("SELECT 1", 4)
    # Capped server-side (one past the limit, to observe truncation), so a broad
    # catalog query is not streamed then discarded.
    assert client.max_results_seen == 5
    assert rows["columns"] == ["table_name"]
    assert len(rows["rows"]) == 4
    assert client.closed


def test_both_declare_query_as_the_scope_checked_parameter() -> None:
    # Without the declaration the framework would gate nothing and raw SQL would
    # reach the client, so this is the load-bearing part of exposing `sql`.
    for cls in (SnowflakeMetadataProbe, BigQueryMetadataProbe):
        spec = getattr(cls.sql, "__probe_command__", None)
        assert isinstance(spec, ProbeMethodSpec)
        assert spec.command == "sql"
        assert spec.scoped_sql_param == "query"


class _Project:
    def __init__(self, project_id: str) -> None:
        self.project_id = project_id


class FakeProjectsClient:
    """A bigquery.Client that only answers list_projects."""

    def __init__(self, projects: List[str], error: Optional[Exception] = None) -> None:
        self._projects = projects
        self._error = error
        self.closed = False

    def list_projects(self) -> object:
        if self._error is not None:
            raise self._error
        return [_Project(p) for p in self._projects]

    def close(self) -> None:
        self.closed = True


def test_bigquery_lists_the_projects_a_dataset_is_qualified_with() -> None:
    """`probe filter` cannot judge a BigQuery dataset without its project, so
    this is the command its refusal points at. Sorted, because a caller
    reading the list wants a stable order rather than the API's."""
    client = FakeProjectsClient(["proj-b", "proj-a"])
    with BigQueryMetadataProbe(client) as probe:
        assert probe.projects() == ["proj-a", "proj-b"]
    assert client.closed


def test_bigquery_projects_declares_the_kind_the_refusal_looks_up() -> None:
    """The refusal finds this command by the kind it declares, not by name, so
    the declaration is the contract -- and the bound keeps a large org's
    paginated project walk from running unbounded."""
    spec = getattr(BigQueryMetadataProbe.projects, "__probe_command__", None)
    assert isinstance(spec, ProbeMethodSpec)
    assert spec.command == "projects"
    assert spec.kind == str(DatasetContainerSubTypes.BIGQUERY_PROJECT)
    assert spec.row_limit_param == "limit"


def test_bigquery_stops_listing_projects_at_the_limit() -> None:
    """Clamped before the call, not trimmed after: an org with thousands of
    projects should not page them all back to be discarded."""
    client = FakeProjectsClient([f"proj-{i}" for i in range(10)])
    assert BigQueryMetadataProbe(client).projects(limit=3) == [
        "proj-0",
        "proj-1",
        "proj-2",
    ]


def test_bigquery_without_permission_to_list_projects_degrades() -> None:
    """`resourcemanager.projects.list` is exactly the permission `project_ids`
    exists for recipes that cannot have it, so a 403 is an expected answer
    rather than a failed probe. It comes back empty, and says why."""
    probe = BigQueryMetadataProbe(FakeProjectsClient([], error=Forbidden("denied")))
    assert probe.projects() == []
    assert any("project_ids" in w for w in probe.warnings), probe.warnings


def test_bigquery_does_not_swallow_an_unrelated_listing_failure() -> None:
    """Only the permission case degrades: anything else is a real failure and
    an empty list would read as "this credential sees no projects"."""
    probe = BigQueryMetadataProbe(FakeProjectsClient([], error=ValueError("boom")))
    with pytest.raises(ValueError):
        probe.projects()


def test_bigquery_does_not_sell_an_auth_failure_as_a_missing_permission() -> None:
    """A 401 is not a permission `project_ids` can route around.

    The credential failed to authenticate at all -- an expired token, a bad
    key -- and is about to fail every other command and the run itself.
    Degrading it to an empty list, exit 0, and advice to name the projects
    reads as "this credential sees no projects" and sends the reader to fix
    the wrong thing.
    """
    probe = BigQueryMetadataProbe(
        FakeProjectsClient([], error=Unauthorized("invalid credentials"))
    )
    with pytest.raises(Unauthorized):
        probe.projects()


def test_bigquery_names_a_disabled_api_rather_than_a_missing_permission() -> None:
    """Both arrive as 403. The remedy for one is to grant a role; for the
    other it is to enable the API, and `project_ids` helps with neither until
    you know which you have. Keyed on the ErrorInfo reason, as
    gcp_errors.is_service_disabled is, because GCP rewords the message."""
    disabled = Forbidden("Cloud Resource Manager API has not been used")
    disabled._response = None
    disabled._error_info = SimpleNamespace(reason="SERVICE_DISABLED")
    probe = BigQueryMetadataProbe(FakeProjectsClient([], error=disabled))
    assert probe.projects() == []
    assert any("not enabled" in w for w in probe.warnings), probe.warnings
    assert not any("resourcemanager.projects.list" in w for w in probe.warnings)


def test_bigquery_still_names_the_permission_when_the_api_is_enabled() -> None:
    """The control for the test above: a plain 403 keeps the IAM remedy."""
    denied = Forbidden("caller does not have permission")
    denied._response = None
    probe = BigQueryMetadataProbe(FakeProjectsClient([], error=denied))
    assert probe.projects() == []
    assert any("resourcemanager.projects.list" in w for w in probe.warnings)
