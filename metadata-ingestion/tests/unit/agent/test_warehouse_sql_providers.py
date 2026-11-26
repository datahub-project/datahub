"""Shape tests for the Snowflake and BigQuery catalog-query surfaces.

Fakes, not live warehouses: these mirror the row shapes each client returns
(Snowflake's DictCursor, BigQuery's RowIterator + schema), so they pin the
mapping and the limit handling but do not prove either client's real behaviour.

Both expose the query as a `sql` probe method whose `query` parameter the
framework scope-checks before calling (see test_scoped_probe_methods).
"""

from typing import Callable, Dict, List, Optional

import pytest
from google.api_core.exceptions import Forbidden, Unauthorized
from google.api_core.retry import Retry

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
    """A bigquery.Client that only answers list_projects.

    Takes the real client's keyword arguments and records them, and yields
    lazily in the order given: a fake that returned a sorted list could not
    tell a truncation bug from correct behaviour.
    """

    def __init__(self, projects: List[str], error: Optional[Exception] = None) -> None:
        self._projects = projects
        self._error = error
        self.closed = False
        self.pulled = 0
        self.kwargs: Dict[str, object] = {}

    def list_projects(self, **kwargs: object) -> object:
        if self._error is not None:
            raise self._error
        self.kwargs = kwargs

        def _pages() -> object:
            for name in self._projects:
                self.pulled += 1
                yield _Project(name)

        return _pages()

    def close(self) -> None:
        self.closed = True


def _forbidden(
    reason: Optional[str], service: str = "bigquery.googleapis.com"
) -> Forbidden:
    """A 403 shaped as google-cloud-bigquery's REST path really raises one:
    ErrorInfo as a plain dict, which is what `GoogleAPICallError.reason`
    cannot read."""
    import json

    import requests
    from google.api_core import exceptions

    detail = (
        {}
        if reason is None
        else {
            "details": [
                {
                    "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                    "reason": reason,
                    "domain": "googleapis.com",
                    "metadata": {"service": service},
                }
            ]
        }
    )
    response = requests.Response()
    response.status_code = 403
    response.request = requests.Request(
        "GET", "https://bigquery.googleapis.com/bigquery/v2/projects"
    ).prepare()
    response._content = json.dumps(
        {
            "error": {
                "code": 403,
                "message": "denied",
                "status": "PERMISSION_DENIED",
                **detail,
            }
        }
    ).encode()
    return exceptions.from_http_response(response)


def test_bigquery_lists_the_projects_a_dataset_is_qualified_with() -> None:
    """`probe filter` cannot judge a BigQuery dataset without its project, so
    this is the command its refusal points at."""
    client = FakeProjectsClient(["proj-b", "proj-a"])
    with BigQueryMetadataProbe(client) as probe:
        assert probe.projects() == ["proj-a", "proj-b"]
    assert client.closed


def test_bigquery_bounds_the_listing_on_both_sides() -> None:
    """The client leaves `timeout` at None and DEFAULT_RETRY would keep
    retrying a read timeout for ten minutes -- on the one command every
    refusal points to."""
    client = FakeProjectsClient(["p1"])
    BigQueryMetadataProbe(client).projects(limit=5)
    # As given: the framework already added its extra item before calling.
    assert client.kwargs["max_results"] == 5
    assert (
        client.kwargs["timeout"] == BigQueryMetadataProbe.query_budget.timeout_seconds
    )
    # The bound itself, not merely that some retry was passed: DEFAULT_RETRY
    # is non-None too, and is the ten minutes this test exists to rule out.
    retry = client.kwargs["retry"]
    assert isinstance(retry, Retry)
    assert retry.timeout == BigQueryMetadataProbe.query_budget.timeout_seconds


def test_bigquery_truncates_before_sorting_not_after() -> None:
    """The framework asks for one past the limit and drops the last, so on a
    truncated listing that must be the extra item. Sorting first instead
    dropped whichever project sorted last, even one well inside the limit --
    and a different one as the limit moved."""
    client = FakeProjectsClient(["zeta", "alpha", "mike", "bravo", "charlie"])
    # limit=3 is what the framework passes for a caller's 2.
    assert BigQueryMetadataProbe(client).projects(limit=3) == ["zeta", "alpha", "mike"]
    # Lazy: a bounded listing must not page the whole org to discard it.
    assert client.pulled == 3


def test_bigquery_does_not_sort_a_page_that_exactly_fills_the_limit() -> None:
    """The off-by-one that came with the double count: a page of exactly
    `limit` items is the truncated case, since `limit` already includes the
    framework's extra. Sorting it let the framework drop the alphabetically
    last project rather than the extra one."""
    client = FakeProjectsClient(["zeta", "alpha", "mike"])
    assert BigQueryMetadataProbe(client).projects(limit=3) == ["zeta", "alpha", "mike"]


def test_bigquery_sorts_a_complete_listing() -> None:
    client = FakeProjectsClient(["zeta", "alpha"])
    assert BigQueryMetadataProbe(client).projects(limit=10) == ["alpha", "zeta"]


def test_bigquery_answers_for_the_recipe_not_for_the_credential() -> None:
    """A refusal sends multi-project recipes here, so a listing wider than
    what ingestion reads would offer a project a run never touches -- and
    `--parent` on it would come back included with nothing said."""
    from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config

    config = BigQueryV2Config.model_validate({"project_ids": ["p2", "p1"]})
    client = FakeProjectsClient(["p1", "p2", "p3"])
    probe = BigQueryMetadataProbe(client, config)
    assert probe.projects() == ["p1", "p2"]
    # No call at all: ingestion reads these and no others.
    assert client.pulled == 0
    assert any("project_ids" in w for w in probe.warnings), probe.warnings


def test_bigquery_runs_the_label_search_ingestion_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`project_labels` sends ingestion down Resource Manager's
    `search_projects`, not BigQuery's own `projects.list`, and it reads only
    what comes back. Listing the credential's view instead answered a
    question the recipe does not ask -- the same fault as ignoring
    `project_ids`, since the refusal points callers here.

    The permission follows from the alignment: ingestion needs
    `resourcemanager.projects.list` for this recipe shape, so the probe does
    too, and a recipe the probe can answer is one ingestion can run.
    """
    import datahub.ingestion.source.common.gcp_project_filter as filter_module
    from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
    from datahub.ingestion.source.common.gcp_project_filter import GcpProject

    config = BigQueryV2Config.model_validate({"project_labels": ["env:prod"]})
    searched: Dict[str, object] = {}

    def _fake_search(labels: object, client: object = None) -> object:
        searched["labels"] = labels
        return iter([GcpProject(id="labelled-1", name="one")])

    monkeypatch.setattr(filter_module, "iter_projects_by_labels", _fake_search)
    monkeypatch.setattr(
        type(config), "get_projects_client", lambda _self: object(), raising=False
    )

    bq_client = FakeProjectsClient(["unlabelled"])
    assert BigQueryMetadataProbe(bq_client, config).projects() == ["labelled-1"]
    assert searched["labels"] == frozenset({"env:prod"})
    # The credential's own view is not consulted on this branch.
    assert bq_client.pulled == 0


def test_the_label_search_classifies_a_403_the_way_the_plain_listing_does(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both branches reach an API that can refuse for several reasons, so both
    have to tell them apart. The labelled one wrapped every `Forbidden` as a
    missing IAM grant, so a disabled API or a quota blip came back as "no
    projects exist, change your permissions"."""
    import datahub.ingestion.source.common.gcp_project_filter as filter_module
    from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config

    config = BigQueryV2Config.model_validate({"project_labels": ["env:prod"]})
    monkeypatch.setattr(
        type(config), "get_projects_client", lambda _self: object(), raising=False
    )

    def _raising(error: Exception) -> Callable[..., object]:
        def _search(labels: object, client: object = None) -> object:
            raise error

        return _search

    # A quota blip is transient and ingestion retries it: it must not become
    # an empty list at exit 0.
    monkeypatch.setattr(
        filter_module,
        "iter_projects_by_labels",
        _raising(_forbidden("RATE_LIMIT_EXCEEDED")),
    )
    with pytest.raises(Forbidden):
        BigQueryMetadataProbe(FakeProjectsClient([]), config).projects()

    # A disabled API names the API, not a role.
    monkeypatch.setattr(
        filter_module,
        "iter_projects_by_labels",
        _raising(
            _forbidden(
                "SERVICE_DISABLED", service="cloudresourcemanager.googleapis.com"
            )
        ),
    )
    probe = BigQueryMetadataProbe(FakeProjectsClient([]), config)
    assert probe.projects() == []
    assert any("cloudresourcemanager" in w for w in probe.warnings), probe.warnings


def test_bigquery_projects_declares_the_kind_the_refusal_looks_up() -> None:
    spec = getattr(BigQueryMetadataProbe.projects, "__probe_command__", None)
    assert isinstance(spec, ProbeMethodSpec)
    assert spec.command == "projects"
    assert spec.kind == str(DatasetContainerSubTypes.BIGQUERY_PROJECT)
    assert spec.row_limit_param == "limit"


def test_bigquery_handles_the_403_shape_a_rest_client_really_raises() -> None:
    """The reason lives in a plain dict on the REST path, where
    `GoogleAPICallError.reason` raises AttributeError. Hand-building
    `_error_info` hid that: every real 403 crashed the handler and exited 1
    instead of degrading."""
    probe = BigQueryMetadataProbe(
        FakeProjectsClient([], error=_forbidden("IAM_PERMISSION_DENIED"))
    )
    assert probe.projects() == []
    assert any("project_ids" in w for w in probe.warnings), probe.warnings


def test_bigquery_names_the_service_that_is_not_enabled() -> None:
    """And names the one the error names: this call is BigQuery's own
    projects.list, not Resource Manager."""
    probe = BigQueryMetadataProbe(
        FakeProjectsClient([], error=_forbidden("SERVICE_DISABLED"))
    )
    assert probe.projects() == []
    assert any("bigquery.googleapis.com" in w for w in probe.warnings), probe.warnings


def test_bigquery_does_not_blame_iam_for_a_transient_403() -> None:
    """A quota blip on concurrent projects.list calls is retried by ingestion.
    Degrading it to an empty list tells the reader to change roles, or narrow
    the recipe, over something that will pass on its own."""
    probe = BigQueryMetadataProbe(
        FakeProjectsClient([], error=_forbidden("RATE_LIMIT_EXCEEDED"))
    )
    with pytest.raises(Forbidden):
        probe.projects()


def test_bigquery_treats_a_403_with_no_reason_as_a_denial() -> None:
    """A legacy 403 carries no ErrorInfo; it degraded before and still does."""
    probe = BigQueryMetadataProbe(FakeProjectsClient([], error=_forbidden(None)))
    assert probe.projects() == []


def test_bigquery_does_not_sell_an_auth_failure_as_a_missing_permission() -> None:
    """A 401 is not a permission `project_ids` can route around."""
    probe = BigQueryMetadataProbe(
        FakeProjectsClient([], error=Unauthorized("invalid credentials"))
    )
    with pytest.raises(Unauthorized):
        probe.projects()


def test_bigquery_does_not_swallow_an_unrelated_listing_failure() -> None:
    probe = BigQueryMetadataProbe(FakeProjectsClient([], error=ValueError("boom")))
    with pytest.raises(ValueError):
        probe.projects()
