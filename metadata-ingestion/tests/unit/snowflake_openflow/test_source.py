from typing import Any, Callable, Dict, List, Optional, Tuple

import pytest
from snowflake.connector.errors import OperationalError

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.snowflake import snowflake_openflow
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnectionConfig,
)
from datahub.ingestion.source.snowflake.snowflake_openflow import (
    _MAX_RUNTIMES_FOR_URL_LOOKUP as _MAX,
    SnowflakeOpenflowSource,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_config import (
    SnowflakeOpenflowSourceConfig,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_models import (
    OpenflowConnector,
    OpenflowRuntime,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_query import (
    CONNECTOR_HISTORY,
    DEPLOYMENT_HISTORY,
    RUNTIME_HISTORY,
    SnowflakeOpenflowQuery,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_report import (
    SnowflakeOpenflowReport,
)

MINIMAL_CONNECTION = {
    "connection": {
        "account_id": "abc12345",
        "username": "user",
        "password": "pass",
    }
}


def _make_source(**config_overrides: Any) -> SnowflakeOpenflowSource:
    # Bypasses __init__ (which opens a real Snowflake connection via
    # config.connection.get_connection()) and PipelineContext entirely. These
    # tests drive the pure extraction/pagination logic through the same
    # _query_rows seam the source itself calls -- no network, no pipeline.
    config = SnowflakeOpenflowSourceConfig.model_validate(
        {**MINIMAL_CONNECTION, **config_overrides}
    )
    source = object.__new__(SnowflakeOpenflowSource)
    source.config = config
    source.platform = "openflow"
    source.report = SnowflakeOpenflowReport()
    return source


def _warning_titles(report: SnowflakeOpenflowReport) -> List[Optional[str]]:
    return [entry.title for entry in report.warnings]


def _row(created_on: Optional[str]) -> Dict[str, Any]:
    return {"CREATED_ON": created_on}


def _fake_query_rows(
    show_query: str,
    history_marker: str,
    show_rows: List[Dict[str, Any]],
    history_rows: List[Dict[str, Any]],
) -> Callable[[str], List[Dict[str, Any]]]:
    def fake(query: str) -> List[Dict[str, Any]]:
        if query == show_query:
            return show_rows
        if history_marker in query:
            return history_rows
        raise AssertionError(f"unexpected query: {query!r}")

    return fake


# --- _paged_history: the two guards demonstrated as real defects before this
# --- brief was written, plus the two paths that must stay quiet/complete.


def test_paged_history_stops_on_null_created_on_boundary():
    # A full page whose last row has a NULL CREATED_ON must not become the
    # literal cursor string "None" -- that would produce a next query of
    # `WHERE CREATED_ON >= 'None'`. The fake is bounded to a few repeats of
    # the NULL-boundary page before it goes empty: a lone Guard-1 regression
    # would still be caught by Guard 2 at the second call (wrong warning,
    # fast failure), but with *both* guards gone nothing else would stop the
    # loop, so the bound is what turns that case into an immediate
    # call-count assertion failure instead of an unbounded loop.
    source = _make_source()
    page_size = SnowflakeOpenflowQuery.PAGE_SIZE
    full_page = [_row(f"2024-01-01T00:00:{i:02d}") for i in range(page_size - 1)] + [
        _row(None)
    ]
    calls: List[str] = []
    max_null_boundary_pages = 3  # generous margin above the 1 call the guard allows

    def fake_query_rows(query: str) -> List[Dict[str, Any]]:
        calls.append(query)
        if len(calls) > max_null_boundary_pages:
            return []
        return full_page

    source._query_rows = fake_query_rows  # type: ignore[method-assign]
    rows = source._paged_history(lambda cursor: f"query cursor={cursor}")

    assert rows == full_page
    assert len(calls) == 1
    titles = _warning_titles(source.report)
    assert "Cannot paginate past a NULL CREATED_ON" in titles
    assert source.report.num_history_pages_beyond_first == 0


def test_paged_history_stops_on_non_advancing_cursor():
    # A full page where every row shares one CREATED_ON can never advance the
    # cursor. The fake is deliberately bounded to a few repeats of the tied
    # page before it goes empty, so that regressing the cursor-equality
    # guard fails this test on the call-count assertion below -- fast and
    # self-explaining -- rather than looping unboundedly against an
    # unconditional fake.
    source = _make_source()
    page_size = SnowflakeOpenflowQuery.PAGE_SIZE
    tied_page = [_row("2024-01-01T00:00:00") for _ in range(page_size)]
    calls: List[str] = []
    max_tied_pages = 3  # generous margin above the 2 calls the guard allows

    def fake_query_rows(query: str) -> List[Dict[str, Any]]:
        calls.append(query)
        if len(calls) > max_tied_pages:
            return []
        return tied_page

    source._query_rows = fake_query_rows  # type: ignore[method-assign]
    rows = source._paged_history(lambda cursor: f"query cursor={cursor}")

    # Page 1: cursor None -> "…:00" advances the cursor once. Page 2: the
    # cursor recomputes to the same "…:00" value, and the guard stops there.
    # If that guard were removed, the fake would keep being called past
    # max_tied_pages, and this assertion is what would catch it.
    assert len(calls) == 2
    assert rows == tied_page + tied_page
    titles = _warning_titles(source.report)
    assert "Pagination stalled on identical timestamps" in titles
    # Two full pages were consumed before the guard fired. Paging beyond the
    # first is counted, not warned about: with the inclusive cursor it is the
    # ordinary case for any account with churn, so a warning here would be noise
    # that buries the stall guard above, which is the real signal.
    assert source.report.num_history_pages_beyond_first == 1


def test_paged_history_normal_termination_stays_quiet():
    # The common case -- a page short of PAGE_SIZE -- must produce no warning
    # at all. This is what proves the guards above do not fire spuriously.
    source = _make_source()
    short_page = [_row("2024-01-01T00:00:00") for _ in range(3)]
    calls: List[str] = []

    def fake_query_rows(query: str) -> List[Dict[str, Any]]:
        calls.append(query)
        return short_page

    source._query_rows = fake_query_rows  # type: ignore[method-assign]
    rows = source._paged_history(lambda cursor: f"query cursor={cursor}")

    assert rows == short_page
    assert len(calls) == 1
    assert len(source.report.warnings) == 0


def test_paged_history_multi_page_collects_all_rows_and_advances_cursor():
    source = _make_source()
    page_size = SnowflakeOpenflowQuery.PAGE_SIZE
    page1 = [_row(f"2024-01-01T00:00:{i:02d}") for i in range(page_size)]
    page2 = [_row(f"2024-01-02T00:00:{i:02d}") for i in range(page_size)]
    page3 = [_row("2024-01-03T00:00:00")]  # short page: ends pagination
    remaining_pages = [page1, page2, page3]
    cursors_seen: List[Optional[str]] = []

    def fake_query_rows(query: str) -> List[Dict[str, Any]]:
        return remaining_pages.pop(0)

    def builder(cursor: Optional[str]) -> str:
        cursors_seen.append(cursor)
        return f"query cursor={cursor}"

    source._query_rows = fake_query_rows  # type: ignore[method-assign]
    rows = source._paged_history(builder)

    assert rows == page1 + page2 + page3
    assert cursors_seen == [None, page1[-1]["CREATED_ON"], page2[-1]["CREATED_ON"]]
    assert source.report.num_history_pages_beyond_first == 2
    # Neither guard fired: the cursor genuinely advanced on every page.
    titles = _warning_titles(source.report)
    assert "Cannot paginate past a NULL CREATED_ON" not in titles
    assert "Pagination stalled on identical timestamps" not in titles


# --- _fetch_deployments / _fetch_runtimes: empty inventory and filtering ----


def test_fetch_deployments_reports_empty_inventory_when_nothing_visible():
    source = _make_source()
    source._query_rows = _fake_query_rows(  # type: ignore[assignment]
        SnowflakeOpenflowQuery.show_deployments(), DEPLOYMENT_HISTORY, [], []
    )
    result = source._fetch_deployments()

    assert result == []
    assert "No Openflow objects found" in _warning_titles(source.report)


def test_fetch_runtimes_reports_empty_inventory_when_nothing_visible():
    source = _make_source()
    source._query_rows = _fake_query_rows(  # type: ignore[assignment]
        SnowflakeOpenflowQuery.show_runtimes(), RUNTIME_HISTORY, [], []
    )
    result = source._fetch_runtimes()

    assert result == []
    assert "No Openflow objects found" in _warning_titles(source.report)


def test_fetch_deployments_filters_by_pattern_without_flagging_empty_inventory():
    # The distinction the review specifically flagged: a legitimate
    # AllowDenyPattern exclusion is not an empty account, and must not be
    # reported as one.
    source = _make_source(deployment_pattern=AllowDenyPattern(deny=["dep-b"]))
    show_rows = [
        {"key": "dep-a", "name": "dep-a"},
        {"key": "dep-b", "name": "dep-b"},
    ]
    source._query_rows = _fake_query_rows(  # type: ignore[assignment]
        SnowflakeOpenflowQuery.show_deployments(), DEPLOYMENT_HISTORY, show_rows, []
    )
    result = source._fetch_deployments()

    assert [deployment.key for deployment in result] == ["dep-a"]
    assert "dep-b" in source.report.filtered_deployments
    assert "No Openflow objects found" not in _warning_titles(source.report)


def test_fetch_runtimes_filters_by_pattern_without_flagging_empty_inventory():
    source = _make_source(runtime_pattern=AllowDenyPattern(deny=["rt-b"]))
    show_rows = [
        {"key": "rt-a", "name": "rt-a", "deployment": "dep-a"},
        {"key": "rt-b", "name": "rt-b", "deployment": "dep-a"},
    ]
    source._query_rows = _fake_query_rows(  # type: ignore[assignment]
        SnowflakeOpenflowQuery.show_runtimes(), RUNTIME_HISTORY, show_rows, []
    )
    result = source._fetch_runtimes()

    assert [runtime.key for runtime in result] == ["rt-a"]
    assert "rt-b" in source.report.filtered_runtimes
    assert "No Openflow objects found" not in _warning_titles(source.report)


# --- get_workunits_internal: the orphaned-runtime branch -------------------


def test_orphaned_runtime_is_skipped_with_warning_and_no_exception():
    source = _make_source()
    deployment_show = [{"key": "dep-a", "name": "dep-a"}]
    runtime_show = [
        {"key": "rt-orphan", "name": "rt-orphan", "deployment": "dep-unknown"}
    ]

    def fake_query_rows(query: str) -> List[Dict[str, Any]]:
        if query == SnowflakeOpenflowQuery.show_deployments():
            return deployment_show
        if query == SnowflakeOpenflowQuery.show_runtimes():
            return runtime_show
        # get_workunits_internal also fetches connectors (Task 9); this test
        # only cares about the runtime/deployment pairing, so connectors are
        # deliberately empty rather than asserted on here.
        if query == SnowflakeOpenflowQuery.show_connectors():
            return []
        if (
            DEPLOYMENT_HISTORY in query
            or RUNTIME_HISTORY in query
            or CONNECTOR_HISTORY in query
        ):
            return []
        raise AssertionError(f"unexpected query: {query!r}")

    source._query_rows = fake_query_rows  # type: ignore[method-assign]

    workunits = list(source.get_workunits_internal())  # must not raise

    assert workunits  # the visible deployment still emits containers
    assert source.report.num_deployments == 1
    assert source.report.num_runtimes == 0
    assert "Runtime with no visible parent deployment" in _warning_titles(source.report)


# --- Lifecycle: the three methods the framework itself calls -----------------


class _FakeConnection:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def fake_snowflake_connection(monkeypatch: pytest.MonkeyPatch) -> _FakeConnection:
    # __init__ opens a real Snowflake connection. These three tests must drive
    # the real __init__ (that is the point of testing create/close), so the
    # connection -- and only the connection -- is stubbed.
    connection = _FakeConnection()
    monkeypatch.setattr(
        SnowflakeConnectionConfig, "get_connection", lambda self: connection
    )
    return connection


def test_create_validates_the_recipe_into_the_config_class(
    fake_snowflake_connection: _FakeConnection,
) -> None:
    # create() is the factory the source registry calls with the raw recipe
    # dict. It must parse that dict through the config class -- handing the dict
    # straight to __init__ would leave every default and validator unapplied.
    ctx = PipelineContext(run_id="test-run")

    source = SnowflakeOpenflowSource.create(
        {**MINIMAL_CONNECTION, "deployment_pattern": {"allow": ["^prod-.*"]}}, ctx
    )

    assert isinstance(source, SnowflakeOpenflowSource)
    assert isinstance(source.config, SnowflakeOpenflowSourceConfig)
    assert source.ctx is ctx
    assert source.config.deployment_pattern.allowed("prod-deployment")
    assert not source.config.deployment_pattern.allowed("dev-deployment")
    # A validator-supplied default, proving the dict went through validation.
    assert source.config.snowflake_env == source.config.env


def test_get_report_returns_the_live_report(
    fake_snowflake_connection: _FakeConnection,
) -> None:
    source = SnowflakeOpenflowSource.create(
        dict(MINIMAL_CONNECTION), PipelineContext(run_id="test-run")
    )

    source.report.num_connectors += 1

    # Identity, not equality: the framework reads counters through this after
    # ingestion, so returning a copy would report zeroes.
    assert source.get_report() is source.report
    assert source.get_report().num_connectors == 1


def test_close_closes_the_snowflake_connection(
    fake_snowflake_connection: _FakeConnection,
) -> None:
    source = SnowflakeOpenflowSource.create(
        dict(MINIMAL_CONNECTION), PipelineContext(run_id="test-run")
    )
    assert not fake_snowflake_connection.closed

    source.close()

    assert fake_snowflake_connection.closed


_CASING_WARNING = "Snowflake platform instance case may not match the snowflake source"


def test_uppercase_platform_instance_warns_rather_than_rejecting():
    # Warn, never reject. AutoLowercaseUrnsProcessor.should_enable gates on the key
    # being PRESENT in the raw recipe, so a `snowflake` recipe that omits it -- the
    # default -- leaves the platform_instance prefix verbatim, exactly as this source
    # does. An uppercase instance is therefore CORRECT against that recipe. An
    # earlier revision raised ValueError here and would have pushed operators off
    # the likelier geometry.
    source = _make_source(snowflake_platform_instance="PROD_SF")
    source._warn_if_platform_instance_casing_is_ambiguous()
    assert _CASING_WARNING in _warning_titles(source.report)


def test_lowercase_platform_instance_is_quiet():
    source = _make_source(snowflake_platform_instance="prod_sf")
    source._warn_if_platform_instance_casing_is_ambiguous()
    assert _CASING_WARNING not in _warning_titles(source.report)


def test_absent_platform_instance_is_quiet():
    source = _make_source()
    source._warn_if_platform_instance_casing_is_ambiguous()
    assert _CASING_WARNING not in _warning_titles(source.report)


_UPSTREAM_WARNING = "Upstream coordinates cannot be verified from this source"


def _info_titles(report: SnowflakeOpenflowReport) -> List[Optional[str]]:
    return [entry.title for entry in report.infos]


def test_configured_upstream_coordinates_are_reported_as_info():
    # The upstream mirror of the destination-side casing warning. Correctness of
    # these three fields depends on the recipe that ingests the upstream system,
    # which this source cannot read, so a mismatch emits a well-formed URN naming
    # a dataset that does not exist and renders exactly like a live one.
    source = _make_source(source_platform_instance="pg_prod")
    source._warn_if_upstream_folding_is_unverifiable()
    # info, not warning: no action by the operator can ever clear it, so a warning
    # would be permanently unclearable noise beside four warnings that CAN be acted on.
    assert _UPSTREAM_WARNING in _info_titles(source.report)
    assert _UPSTREAM_WARNING not in _warning_titles(source.report)


def test_upstream_warning_is_quiet_when_nothing_is_configured():
    # With no upstream coordinates and no folding there is nothing to mismatch,
    # so the default recipe must not draw a warning on every run.
    source = _make_source()
    source._warn_if_upstream_folding_is_unverifiable()
    assert _UPSTREAM_WARNING not in _info_titles(source.report)


def test_upstream_warning_is_quiet_when_lineage_is_disabled():
    # No upstream URNs are built at all, so the coordinates cannot be wrong.
    source = _make_source(
        source_platform_instance="pg_prod", include_openflow_lineage=False
    )
    source._warn_if_upstream_folding_is_unverifiable()
    assert _UPSTREAM_WARNING not in _info_titles(source.report)


def test_upstream_info_fires_on_the_fold_flag_alone():
    # Second arm of the predicate. Deleting it left the suite green before this.
    source = _make_source(source_convert_urns_to_lowercase=True)
    source._warn_if_upstream_folding_is_unverifiable()
    assert _UPSTREAM_WARNING in _info_titles(source.report)


def test_upstream_info_fires_when_source_env_differs_from_env():
    # Third arm. source_env cannot be tested for None -- a validator fills it from
    # env -- so "the operator chose one" means it differs from env.
    source = _make_source(env="PROD", source_env="DEV")
    source._warn_if_upstream_folding_is_unverifiable()
    assert _UPSTREAM_WARNING in _info_titles(source.report)


def test_upstream_info_is_quiet_when_source_env_merely_restates_env():
    # Restating the default is not a choice: the URN is byte-identical to the
    # default recipe's, so reporting it would be reporting on nothing.
    source = _make_source(env="PROD", source_env="PROD")
    source._warn_if_upstream_folding_is_unverifiable()
    assert _UPSTREAM_WARNING not in _info_titles(source.report)


# --- connector external URL (DESCRIBE-only column) ------------------------


def _never_queried(query: str) -> List[Dict[str, Any]]:
    raise AssertionError(f"should not have queried: {query!r}")


def _addressable_connector() -> OpenflowConnector:
    return OpenflowConnector(
        name="conn",
        runtime_name="rt",
        database_name="DB",
        schema_name="SCH",
    )


def test_connector_url_is_read_from_describe():
    source = _make_source()
    seen: List[str] = []

    def fake(query: str) -> List[Dict[str, Any]]:
        seen.append(query)
        return [{"CONNECTOR_URL": "https://host/rt/nifi/#/connectors/abc/"}]

    source._query_rows = fake  # type: ignore[method-assign]
    # The canvas root, not the reported deep link -- see _canvas_url.
    assert source._read_connector_url(_addressable_connector()) == (
        "https://host/rt/nifi/"
    )
    assert seen == [SnowflakeOpenflowQuery.describe_connector('"DB"."SCH"."conn"')]


def test_connector_url_disabled_issues_no_describe():
    # The flag exists to buy back one query per connector; if it still queried,
    # it would buy nothing.
    source = _make_source(include_connector_external_url=False)

    def fake(query: str) -> List[Dict[str, Any]]:
        raise AssertionError(f"should not have queried: {query!r}")

    source._query_rows = fake  # type: ignore[method-assign]
    assert source._read_connector_url(_addressable_connector()) is None


def test_connector_url_failure_is_counted_and_warned_not_raised():
    # A missing link must cost one aspect field, never the connector.
    source = _make_source()

    def fake(query: str) -> List[Dict[str, Any]]:
        raise RuntimeError("DESCRIBE denied")

    source._query_rows = fake  # type: ignore[method-assign]
    assert source._read_connector_url(_addressable_connector()) is None
    assert source.report.num_connector_urls_failed == 1
    assert _warning_titles(source.report) == ["Could not read connector URL"]


def test_history_only_connector_is_counted_quietly_not_warned():
    # A connector seen only in the history view has no DATABASE_NAME, so it
    # cannot be addressed. For a dropped connector that is the steady state --
    # warning on it would fire on every run forever.
    source = _make_source()

    def fake(query: str) -> List[Dict[str, Any]]:
        raise AssertionError("unaddressable connector must not be queried")

    source._query_rows = fake  # type: ignore[method-assign]
    assert (
        source._read_connector_url(OpenflowConnector(name="c", runtime_name="rt"))
        is None
    )
    assert source.report.num_connectors_without_fqn == 1
    assert _warning_titles(source.report) == []


def test_connector_url_retries_a_transient_connection_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The DESCRIBE shipped without a retry while the stage GET beside it had
    # one, so a single blip permanently cost that connector its link. Both
    # per-connector calls now share the same bounded policy.
    monkeypatch.setattr(snowflake_openflow, "_RETRY_BACKOFF_MULTIPLIER", 0)
    source = _make_source()
    attempts: List[str] = []

    def flaky(query: str) -> List[Dict[str, Any]]:
        attempts.append(query)
        if len(attempts) < 3:
            raise OperationalError(msg="connection reset by peer")
        return [{"CONNECTOR_URL": "https://host/rt/nifi/#/connectors/abc/"}]

    source._query_rows = flaky  # type: ignore[method-assign]
    # The canvas root, not the reported deep link -- see _canvas_url.
    assert source._read_connector_url(_addressable_connector()) == (
        "https://host/rt/nifi/"
    )
    assert len(attempts) == 3
    assert source.report.num_connector_urls_failed == 0


def test_connector_url_gives_up_after_the_bounded_number_of_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(snowflake_openflow, "_RETRY_BACKOFF_MULTIPLIER", 0)
    source = _make_source()
    attempts: List[str] = []

    def always_failing(query: str) -> List[Dict[str, Any]]:
        attempts.append(query)
        raise OperationalError(msg="connection reset by peer")

    source._query_rows = always_failing  # type: ignore[method-assign]
    assert source._read_connector_url(_addressable_connector()) is None
    assert len(attempts) == snowflake_openflow._RETRY_MAX_ATTEMPTS
    assert source.report.num_connector_urls_failed == 1


@pytest.mark.parametrize(
    "describe_rows",
    [
        pytest.param([], id="no rows at all"),
        pytest.param([{"NAME": "conn"}], id="a row without the CONNECTOR_URL column"),
    ],
)
def test_describe_answering_without_a_url_is_counted_and_warned(
    describe_rows: List[Dict[str, Any]],
) -> None:
    # Both shapes mean DESCRIBE answered but not per its contract -- what a
    # Snowflake-side surface change would look like. Silently returning None
    # would make it indistinguishable from a connector that has no link.
    source = _make_source()
    source._query_rows = lambda query: describe_rows  # type: ignore[method-assign]
    assert source._read_connector_url(_addressable_connector()) is None
    assert source.report.num_connector_urls_failed == 1
    assert _warning_titles(source.report) == ["Connector URL missing from DESCRIBE"]


def test_url_lookup_is_dropped_on_an_oversized_account() -> None:
    # The DESCRIBE is a second serial round trip per connector. Past the
    # threshold a convenience link is not worth doubling the run.
    source = _make_source()
    source._decide_url_lookup(_MAX + 1, _MAX + 1)
    source._query_rows = _never_queried  # type: ignore[method-assign]

    assert source._read_connector_url(_addressable_connector()) is None
    assert _warning_titles(source.report) == ["Connector external links skipped"]


def test_url_lookup_survives_at_the_threshold() -> None:
    # Boundary is inclusive: the warning must not fire for an account sitting
    # exactly on the limit, or the message would name a count it permits.
    source = _make_source()
    source._decide_url_lookup(_MAX, _MAX)
    assert source._fetch_connector_urls
    assert _warning_titles(source.report) == []


def test_explicitly_requested_urls_are_fetched_at_any_scale() -> None:
    # Auto-degrade applies to the `None` (auto) state only. An operator who
    # asked for true has taken the cost decision.
    source = _make_source(include_connector_external_url=True)
    source._decide_url_lookup(_MAX * 100, _MAX * 100)
    assert source._fetch_connector_urls
    assert _warning_titles(source.report) == []


def test_disabled_flag_needs_no_scale_warning() -> None:
    # Nothing was going to be fetched, so there is nothing to announce.
    source = _make_source(include_connector_external_url=False)
    source._decide_url_lookup(_MAX * 100, _MAX * 100)
    assert _warning_titles(source.report) == []


def test_auto_state_survives_a_recipe_round_trip() -> None:
    # The bool + model_fields_set version failed exactly here: model_dump ->
    # model_validate marks every field explicit, so the scale guard silently
    # vanished for any path that re-serialises a recipe (a UI edit-and-save,
    # recipe templating). The tri-state carries the choice in the value.
    config = SnowflakeOpenflowSourceConfig.model_validate(MINIMAL_CONNECTION)
    round_tripped = SnowflakeOpenflowSourceConfig.model_validate(config.model_dump())
    assert round_tripped.include_connector_external_url is None

    source = _make_source()
    source.config = round_tripped
    source._decide_url_lookup(_MAX + 1, _MAX + 1)
    assert not source._fetch_connector_urls


@pytest.mark.parametrize(
    ("reported", "expected"),
    [
        # What DESCRIBE actually returns. Its fragment 404s -- the canvas app
        # has no #/connectors route -- so only the prefix both the broken and a
        # working canvas URL agree on is kept.
        (
            "https://h.snowflakecomputing.app:443/rt-100/nifi/#/connectors/0000-0f10/",
            "https://h.snowflakecomputing.app/rt-100/nifi/",
        ),
        # Snowflake reports the default port; its own UI omits it.
        ("https://h.app:443/rt/nifi/#/x", "https://h.app/rt/nifi/"),
        # A non-default port is meaningful and kept.
        ("https://h.app:8443/rt/nifi/#/x", "https://h.app:8443/rt/nifi/"),
        # No canvas marker: nothing derivable, so emit nothing rather than a guess.
        ("https://h.app/rt/somewhere-else", None),
        ("", None),
    ],
)
def test_canvas_url_keeps_only_the_part_that_resolves(
    reported: str, expected: Optional[str]
) -> None:
    assert snowflake_openflow._canvas_url(reported) == expected


def test_connector_url_emits_the_canvas_not_the_reported_deep_link() -> None:
    # Regression: the reported CONNECTOR_URL was emitted verbatim and returned
    # "Route not found" for every connector, because its uuid is a Snowflake
    # connector id and the canvas addresses NiFi process-group ids instead.
    source = _make_source()
    source._query_rows = lambda query: [  # type: ignore[method-assign]
        {"CONNECTOR_URL": "https://h.app:443/rt-1/nifi/#/connectors/0000-0f10/"}
    ]
    assert source._read_connector_url(_addressable_connector()) == (
        "https://h.app/rt-1/nifi/"
    )


def test_canvas_url_is_fetched_once_per_runtime_not_once_per_connector() -> None:
    # The URL is per-runtime -- _canvas_url truncates away everything
    # connector-specific -- so paying one DESCRIBE per connector was fetching a
    # parent's value once per child. Cached, three siblings cost one query.
    source = _make_source()
    calls: List[str] = []

    def counting(query: str) -> List[Dict[str, Any]]:
        calls.append(query)
        return [{"CONNECTOR_URL": "https://h.app:443/rt-1/nifi/#/connectors/x/"}]

    source._query_rows = counting  # type: ignore[method-assign]
    urls = [
        source._read_connector_url(
            OpenflowConnector(
                name=f"conn{n}",
                runtime_name="rt",
                database_name="DB",
                schema_name="SCH",
            )
        )
        for n in range(3)
    ]

    assert urls == ["https://h.app/rt-1/nifi/"] * 3
    assert len(calls) == 1


def test_a_failed_lookup_is_not_cached_against_the_runtime() -> None:
    # Caching a transient failure would deny the link to every sibling
    # afterwards -- trading an N+1 for a correctness regression.
    source = _make_source()
    attempts: List[str] = []

    def flaky(query: str) -> List[Dict[str, Any]]:
        attempts.append(query)
        return (
            []
            if len(attempts) == 1
            else [{"CONNECTOR_URL": "https://h.app/rt-1/nifi/#/x"}]
        )

    source._query_rows = flaky  # type: ignore[method-assign]
    first = source._read_connector_url(_addressable_connector())
    second = source._read_connector_url(_addressable_connector())

    assert first is None
    assert second == "https://h.app/rt-1/nifi/"


def test_gate_counts_runtimes_not_connectors() -> None:
    # The whole point of the rename: the DESCRIBE is one per runtime, so an
    # account with many connectors on few runtimes costs a handful of queries
    # and must not be degraded. Reverting the gate to connector_count fails
    # here, which the previous tests did not catch -- they passed both counts
    # the same value.
    source = _make_source()
    source._decide_url_lookup(connector_count=100_000, runtime_count=10)
    assert source._fetch_connector_urls
    assert _warning_titles(source.report) == []


def test_gate_fires_on_runtime_count_even_with_few_connectors() -> None:
    source = _make_source()
    source._decide_url_lookup(connector_count=5, runtime_count=_MAX + 1)
    assert not source._fetch_connector_urls
    assert _warning_titles(source.report) == ["Connector external links skipped"]


@pytest.mark.parametrize(
    ("differing_field"),
    [
        pytest.param("database_name", id="database"),
        pytest.param("schema_name", id="schema"),
        # The ordinary shape: one account keeps all Openflow objects in one
        # schema, so two runtimes there would share a canvas URL if the runtime
        # component were ever dropped from the key.
        pytest.param("runtime_name", id="runtime"),
    ],
)
def test_identically_named_runtimes_in_different_scopes_are_not_shared(
    differing_field: str,
) -> None:
    # Runtime names are scoped to their deployment, not the account, so two
    # deployments can each hold a runtime called the same thing. Keying the
    # cache on the bare name would hand one deployment's connectors the other's
    # canvas host -- silently, since both URLs are well-formed.
    source = _make_source()
    urls = iter(
        [
            [{"CONNECTOR_URL": "https://a.app/rt-a/nifi/#/connectors/x/"}],
            [{"CONNECTOR_URL": "https://b.app/rt-b/nifi/#/connectors/y/"}],
        ]
    )
    calls: List[str] = []

    def per_call(query: str) -> List[Dict[str, Any]]:
        calls.append(query)
        return next(urls)

    source._query_rows = per_call  # type: ignore[method-assign]
    base = {"runtime_name": "default", "database_name": "DB", "schema_name": "S"}
    first = source._read_connector_url(
        OpenflowConnector(name="c1", **{**base, differing_field: "one"})  # type: ignore[arg-type]
    )
    second = source._read_connector_url(
        OpenflowConnector(name="c2", **{**base, differing_field: "two"})  # type: ignore[arg-type]
    )

    assert first == "https://a.app/rt-a/nifi/"
    assert second == "https://b.app/rt-b/nifi/"
    assert len(calls) == 2


@pytest.mark.parametrize(
    "malformed",
    [
        pytest.param("https://[unclosed/nifi/#/x", id="unclosed IPv6 bracket"),
        pytest.param("https://host:notaport/nifi/#/x", id="non-numeric port"),
    ],
)
def test_canvas_url_never_raises_on_a_malformed_url(malformed: str) -> None:
    # The value comes from Snowflake, not from us: urlparse raises on a
    # malformed authority and .port raises on a non-numeric port. A surface
    # change must cost this connector its link, never the whole run.
    assert snowflake_openflow._canvas_url(malformed) is None


def test_fetch_connectors_filters_by_pattern() -> None:
    # connector_pattern had no test at all, while its two siblings had two each.
    # Swapping it for allow_all() passed the whole suite.
    source = _make_source(connector_pattern=AllowDenyPattern(deny=["excluded"]))
    show_rows = [
        {"name": "kept", "runtime": "rt"},
        {"name": "excluded", "runtime": "rt"},
    ]
    source._query_rows = _fake_query_rows(  # type: ignore[assignment]
        SnowflakeOpenflowQuery.show_connectors(), CONNECTOR_HISTORY, show_rows, []
    )

    assert [c.name for c in source._fetch_connectors()] == ["kept"]
    assert "excluded" in source.report.filtered_connectors


def test_gate_is_decided_from_the_connectors_actually_fetched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # test_gate_counts_runtimes_not_connectors calls _decide_url_lookup directly, so
    # the call site itself was unpinned: replacing the runtime set with
    # len(connectors) there passed every test.
    source = _make_source()
    seen: List[Tuple[int, int]] = []
    monkeypatch.setattr(
        source,
        "_decide_url_lookup",
        lambda connector_count, runtime_count: seen.append(
            (connector_count, runtime_count)
        ),
    )
    monkeypatch.setattr(source, "_fetch_deployments", lambda: [])
    monkeypatch.setattr(source, "_fetch_runtimes", lambda: [])
    monkeypatch.setattr(
        source,
        "_fetch_connectors",
        lambda: [
            OpenflowConnector(
                name=f"c{n}", runtime_name=rt, database_name="DB", schema_name="S"
            )
            for n, rt in enumerate(["rt_a", "rt_a", "rt_b"])
        ],
    )
    list(source.get_workunits_internal())

    # Three connectors, two distinct locations -- not three, and not one.
    assert seen == [(3, 2)]


def test_excluding_a_deployment_produces_no_grant_monitor_warnings() -> None:
    # Filtering a deployment removed it from by_deployment_name, so every
    # runtime under it was told "Grant MONITOR on the deployment" -- wrong
    # remediation, fires every run, and the operator cannot clear it because
    # they asked for the exclusion. It then cascaded: those runtimes never
    # entered runtime_keys_by_name, so each of their connectors warned too.
    # One pattern entry yielded 1 + N unclearable warnings.
    source = _make_source(deployment_pattern=AllowDenyPattern(deny=["excluded_dep"]))
    monkeypatch_free_runtime = OpenflowRuntime(
        key="rt-1", name="rt", deployment_name="excluded_dep"
    )
    source._fetch_deployments = lambda: []  # type: ignore[method-assign]
    source._fetch_runtimes = lambda: [monkeypatch_free_runtime]  # type: ignore[method-assign]
    source._fetch_connectors = lambda: [  # type: ignore[method-assign]
        OpenflowConnector(name="c1", runtime_name="rt")
    ]

    list(source.get_workunits_internal())

    # Neither the runtime-level nor the cascaded connector-level warning.
    # (An unrelated "no config location" warning is expected: the stub
    # connector carries no version_location_uri, which is not what this pins.)
    assert "Runtime with no visible parent deployment" not in _warning_titles(
        source.report
    )
    assert "Connector with no visible parent runtime" not in _warning_titles(
        source.report
    )
    # Still counted, so the total stays visible even though it is not warned.
    assert source.report.num_connectors_without_runtime_parent == 1


def test_a_genuinely_orphaned_runtime_still_warns() -> None:
    # The waiver must be narrow: a runtime whose deployment is missing for any
    # reason OTHER than the operator's filter is a real permissions gap.
    source = _make_source()
    source._fetch_deployments = lambda: []  # type: ignore[method-assign]
    source._fetch_runtimes = lambda: [  # type: ignore[method-assign]
        OpenflowRuntime(key="rt-1", name="rt", deployment_name="invisible_dep")
    ]
    source._fetch_connectors = lambda: []  # type: ignore[method-assign]

    list(source.get_workunits_internal())

    assert _warning_titles(source.report) == [
        "Runtime with no visible parent deployment"
    ]


def test_rows_missing_their_identity_column_are_counted_and_reported() -> None:
    # A row whose identity column is absent cannot become a model, and dropping
    # it silently is how a healthy object disappears: it is then missing from
    # the stale-entity checkpoint, so the NEXT run soft-deletes it while
    # reporting success. The two surviving runtimes prove the drop is partial --
    # the total-failure case is already covered by the empty-inventory report,
    # and it is the partial one that used to be invisible.
    source = _make_source()
    source._query_rows = _fake_query_rows(  # type: ignore[assignment]
        SnowflakeOpenflowQuery.show_runtimes(),
        "OPENFLOW_RUNTIME_HISTORY",
        [
            {"key": "rt-1", "name": "one"},
            {"name": "two-but-no-key"},
            {"key": "rt-3", "name": "three"},
        ],
        [],
    )

    runtimes = source._fetch_runtimes()

    assert sorted(r.key for r in runtimes) == ["rt-1", "rt-3"]
    assert source.report.num_rows_missing_identity == 1
    assert "Rows skipped: identity column missing" in _warning_titles(source.report)


def test_a_transient_error_on_the_inventory_fetch_is_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # SHOW OPENFLOW is outside the shared connection's retry, which fires only
    # for ACCOUNT_USAGE text AND a permission error. Without a retry here one
    # blip aborts the run with zero entities, while the OPTIONAL per-connector
    # url enrichment -- which already had one -- survives. SHOW is read-only, so
    # retrying cannot double an effect.
    monkeypatch.setattr(snowflake_openflow, "_RETRY_BACKOFF_MULTIPLIER", 0)
    source = _make_source()
    attempts: List[str] = []

    class _FlakyConnection:
        def query(self, query: str) -> List[Dict[str, Any]]:
            attempts.append(query)
            if len(attempts) == 1:
                raise ConnectionResetError("transient")
            return [{"key": "rt-1", "name": "one"}]

    source.connection = _FlakyConnection()  # type: ignore[assignment]

    rows = source._query_rows(SnowflakeOpenflowQuery.show_runtimes())

    assert rows == [{"key": "rt-1", "name": "one"}]
    assert len(attempts) == 2, "the first attempt must be retried, not surfaced"
