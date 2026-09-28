"""A raw-SQL or raw-path parameter is gated by the framework, not by the getter.

`sql` and `api` are ordinary probe methods, so `probe run` reaches them through
the same path as `columns` or `topics`. What keeps them safe is that the getter
*declares* which parameter carries the dangerous value and the framework checks
it before invoking -- a connector cannot forget a check it does not perform.
"""

import datetime
import decimal
import inspect as _inspect
from typing import Dict, List, Sequence

import pytest

from datahub.configuration.common import ConfigModel
from datahub.ingestion.agent.api_gate import ApiScopeError
from datahub.ingestion.agent.probe_methods import (
    MAX_PROBE_ITEMS,
    ProbeMethodSpec,
    _bounded_kwargs,
    _enforce_gates,
    _refuse_withheld_passthrough,
    probe_method,
)
from datahub.ingestion.agent.sql_gate import SqlScopeError
from datahub.ingestion.agent.sql_passthrough import sql_result
from datahub.ingestion.source.kafka.kafka_probe import KafkaMetadataProbe
from datahub.ingestion.source.sql.sqlalchemy_probe import SqlAlchemyMetadataProbe


class FakeSqlProvider:
    sql_dialect = "postgres"

    def __init__(self) -> None:
        self.ran: List[str] = []

    @probe_method(
        name="sql",
        scoped_sql_param="query",
        row_limit_param="limit",
        shapes_own_result=True,
    )
    def sql(self, query: str, limit: int = 50) -> Dict[str, object]:
        """Run a catalog query."""
        self.ran.append(query)
        return sql_result(["c"], [["v"]], limit)


class FakeListingProvider:
    """A typed listing: returns a bare list, so the framework owns its +1."""

    def __init__(self) -> None:
        self.asked: List[int] = []

    @probe_method(row_limit_param="limit")
    def containers(self, limit: int = 200) -> List[str]:
        """Every container."""
        self.asked.append(limit)
        return [f"c{i}" for i in range(500)][:limit]


class FakeApiProvider:
    api_allowlist: Sequence[str] = ("GET /spaces", "GET /spaces/{token}/reports")

    def __init__(self) -> None:
        self.ran: List[str] = []

    @probe_method(name="api", scoped_path_param="path")
    def api(self, path: str) -> object:
        """Fetch one listed endpoint."""
        self.ran.append(path)
        return {"ok": True}


class DialectlessProvider:
    @probe_method(name="sql", scoped_sql_param="query")
    def sql(self, query: str) -> Dict[str, object]:
        """Run a catalog query."""
        return {}


class AllowlistlessProvider:
    @probe_method(name="api", scoped_path_param="path")
    def api(self, path: str) -> object:
        """Fetch an endpoint, without listing which ones exist."""
        return {}


def _spec(provider: object, command: str) -> ProbeMethodSpec:
    # Accepts an instance or a class: the spec is stamped on the function.
    owner = provider if isinstance(provider, type) else type(provider)
    spec = getattr(getattr(owner, command), "__probe_command__", None)
    assert isinstance(spec, ProbeMethodSpec)
    return spec


def test_a_refused_query_is_stopped_before_the_getter_runs():
    provider = FakeSqlProvider()
    with pytest.raises(SqlScopeError):
        _enforce_gates(
            _spec(provider, "sql"), provider, {"query": "SELECT * FROM public.orders"}
        )
    assert provider.ran == []


def test_a_permitted_query_passes_the_gate():
    provider = FakeSqlProvider()
    query = "SELECT table_name FROM information_schema.tables"
    _enforce_gates(_spec(provider, "sql"), provider, {"query": query})
    assert provider.sql(query)["columns"] == ["c"]


def test_a_provider_without_a_dialect_cannot_run_sql():
    # Falling back to a default dialect would parse against the wrong grammar and
    # clear references it had misread, so this refuses instead.
    provider = DialectlessProvider()
    with pytest.raises(ValueError, match="no sql_dialect"):
        _enforce_gates(_spec(provider, "sql"), provider, {"query": "SELECT 1"})


def test_an_unlisted_path_is_stopped_before_the_getter_runs():
    provider = FakeApiProvider()
    with pytest.raises(ApiScopeError):
        _enforce_gates(
            _spec(provider, "api"), provider, {"path": "/spaces/sp1/members"}
        )
    assert provider.ran == []


def test_a_listed_path_passes_the_gate():
    provider = FakeApiProvider()
    _enforce_gates(_spec(provider, "api"), provider, {"path": "/spaces/sp1/reports"})


def test_a_provider_with_no_allowlist_at_all_is_a_provider_bug():
    # Distinct from an unlisted path: falling through to an empty allowlist would
    # blame the caller ("not in this connector's allowlist") for a connector that
    # never listed anything, sending them to rewrite a path that cannot work.
    provider = AllowlistlessProvider()
    with pytest.raises(ValueError, match="no api_allowlist") as caught:
        _enforce_gates(_spec(provider, "api"), provider, {"path": "/spaces"})
    assert not isinstance(caught.value, ApiScopeError)


def test_a_row_limit_beyond_the_maximum_is_clamped_before_the_fetch():
    # The getter fetches `limit + 1` rows, so an unclamped limit is a fetch the
    # connector actually performs -- capping the output afterwards would be too
    # late to matter.
    bounded = _bounded_kwargs(
        _spec(FakeSqlProvider(), "sql"), {"query": "SELECT 1", "limit": 10_000_000}
    )
    assert bounded["limit"] == MAX_PROBE_ITEMS


@pytest.mark.parametrize("limit", [0, -1])
def test_a_row_limit_below_one_is_clamped_to_one(limit: int) -> None:
    # rows[:-1] silently drops the last row and still reports truncated=True.
    bounded = _bounded_kwargs(
        _spec(FakeSqlProvider(), "sql"), {"query": "SELECT 1", "limit": limit}
    )
    assert bounded["limit"] == 1


def test_a_row_limit_within_range_is_left_alone():
    """`sql` declares shapes_own_result, so it gets exactly what was asked for
    -- it does its own +1 internally, and a second one here would return
    limit+1 rows and compute `truncated` against the wrong number."""
    bounded = _bounded_kwargs(
        _spec(FakeSqlProvider(), "sql"), {"query": "SELECT 1", "limit": 50}
    )
    assert bounded["limit"] == 50


def test_an_omitted_row_limit_is_filled_from_the_getters_own_default():
    """Both shapes, because the +1 is what differs and only one of them had
    a test.

    _bounded_kwargs fills an omitted limit from the getter's signature so the
    clamp and the report describe the same number the getter will use. The
    explicit-limit cases above cover the clamping; this is the default-fill,
    and the shapes_own_result half of it lost its only coverage when
    test_an_omitted_row_limit_is_left_to_the_getter_default was deleted.
    """
    # `sql` owns its envelope and does its own +1, so it gets exactly its
    # declared default -- a second +1 here would hand back one row more than
    # asked for and compute `truncated` against the wrong number.
    assert (
        _bounded_kwargs(_spec(FakeSqlProvider(), "sql"), {"query": "SELECT 1"})["limit"]
        == 50
    )
    # A listing returns a bare list, so the framework owns the +1 and asks for
    # one past the getter's default.
    assert (
        _bounded_kwargs(_spec(FakeListingProvider(), "containers"), {})["limit"] == 201
    )


def test_a_listing_is_asked_for_one_past_its_limit():
    """The other half of the same contract. A listing returns a bare list, so
    the framework owns the +1 -- without it a getter returning exactly `limit`
    items is indistinguishable from one that returned everything."""
    bounded = _bounded_kwargs(_spec(FakeListingProvider(), "containers"), {"limit": 50})
    assert bounded["limit"] == 51


def test_an_omitted_row_limit_uses_the_getters_own_declared_default():
    """It used to be left out entirely, which meant the framework did not know
    the limit and so could not tell a truncated listing from a complete one --
    and calling with no --limit is the common case. The default is read off the
    signature rather than guessed, so it is still the getter's own number."""
    bounded = _bounded_kwargs(_spec(FakeListingProvider(), "containers"), {})
    assert bounded["limit"] == 201  # the getter's own 200, plus the probe row


def test_declaring_a_row_limit_param_that_does_not_exist_is_rejected_at_import():
    with pytest.raises(ValueError, match="no such parameter"):

        class Broken:
            @probe_method(row_limit_param="lmit")
            def sql(self, query: str, limit: int = 50) -> Dict[str, object]:
                """Typo in the declared row-limit parameter."""
                return {}


def test_sql_result_will_not_emit_more_than_the_maximum():
    # Defence in depth: the clamp above bounds the fetch, and this bounds what a
    # provider that builds its own rows can hand back.
    out = sql_result(["c"], [[i] for i in range(MAX_PROBE_ITEMS + 10)], 10_000_000)
    assert out["row_count"] == MAX_PROBE_ITEMS
    assert out["truncated"]


def test_declaring_a_parameter_that_does_not_exist_is_rejected_at_import():
    # A typo'd declaration would silently gate nothing, so it fails loudly where
    # the decorator is applied rather than at call time.
    with pytest.raises(ValueError, match="no such parameter"):

        class Broken:
            @probe_method(scoped_sql_param="qeury")
            def sql(self, query: str) -> Dict[str, object]:
                """Typo in the declared parameter name."""
                return {}


def test_an_operator_can_switch_raw_access_off_entirely(monkeypatch):
    # An env var rather than a recipe field on purpose: the agent writes the
    # recipe, so a recipe field would let it grant itself the access.
    monkeypatch.setenv("DATAHUB_PROBE_DISABLE_RAW_ACCESS", "true")
    provider = FakeSqlProvider()
    with pytest.raises(ValueError, match="DATAHUB_PROBE_DISABLE_RAW_ACCESS"):
        _refuse_withheld_passthrough(_spec(provider, "sql"), "postgres")
    assert provider.ran == []


def test_the_switch_is_checked_before_anything_connects(monkeypatch):
    """It lived in _enforce_gates, which runs inside `with builder(config)` --
    after the provider has authenticated. On a source that was slow or down
    the operator's refusal never appeared: the caller got a connection error
    and went off to fix credentials for a command that was never going to
    run.

    Asserted through run_probe_method against a source that cannot be
    reached, because that is the situation the bug was about. The earlier
    version of this test only checked that _refuse_withheld_passthrough
    takes no `provider` argument, which is a property of the signature:
    it stays true if someone moves the CALL back inside the `with`, which
    is the regression itself.
    """
    import datahub.ingestion.agent.probe_methods as _pm

    monkeypatch.setenv("DATAHUB_PROBE_DISABLE_RAW_ACCESS", "true")

    built: List[object] = []

    class Unreachable(FakeSqlProvider):
        @classmethod
        def for_config(cls, config: object) -> "Unreachable":
            built.append(config)
            raise OSError("connection refused: the source is down")

    class _Config(ConfigModel):
        pass

    monkeypatch.setattr(_pm, "_provider_class", lambda source_type: Unreachable)
    monkeypatch.setattr(_pm, "config_class_for", lambda source_type: _Config)

    with pytest.raises(ValueError, match="DATAHUB_PROBE_DISABLE_RAW_ACCESS"):
        _pm.run_probe_method("postgres", {}, "sql", {"query": "SELECT 1"})

    # Not merely "a ValueError surfaced first" -- the connection was never
    # attempted at all, so a slow source cannot delay the refusal either.
    assert built == [], "the provider was built before the operator's switch"

    # And the check still needs no provider to perform, which is what lets
    # it sit this early.
    params = _inspect.signature(_refuse_withheld_passthrough).parameters
    assert "provider" not in params, (
        "taking a provider is what put this behind authentication"
    )


def _only_these(monkeypatch, *specs):
    """Pin what the connector is said to expose, instead of asking the registry.

    _refuse_withheld_passthrough calls list_probe_methods(source_type) to
    decide whether to promise "other probe commands still work", and that
    resolves through the live source registry. Two problems with letting it:

    a minimal environment cannot import the snowflake or mode config, so
    config_class_for raises ValueError("unknown or unloadable source
    type...") -- a ValueError, which pytest.raises catches, whose message
    then fails the regex and blames the caller's source type for a missing
    extra;

    and the premise "snowflake exposes sql and nothing else" is registry
    state, so the day snowflake gains a second probe command this test
    quietly starts exercising the other branch while keeping its name.

    Pinning the list tests the message logic, which is what these are about.
    """
    import datahub.ingestion.agent.probe_methods as _pm

    monkeypatch.setattr(_pm, "list_probe_methods", lambda source_type: list(specs))


def test_the_refusal_does_not_promise_commands_the_connector_lacks(monkeypatch):
    """Snowflake and BigQuery expose `sql` as their ONLY probe command, so
    "this connector's other probe commands still work" was false exactly
    where the switch matters most."""
    monkeypatch.setenv("DATAHUB_PROBE_DISABLE_RAW_ACCESS", "true")
    provider = FakeSqlProvider()
    sql_spec = _spec(provider, "sql")
    listing_spec = _spec(FakeListingProvider(), "containers")

    # A connector whose only command is the passthrough.
    _only_these(monkeypatch, sql_spec)
    with pytest.raises(ValueError, match="fully withheld"):
        _refuse_withheld_passthrough(sql_spec, "snowflake")

    # And one that does have others still says so.
    _only_these(monkeypatch, sql_spec, listing_spec)
    with pytest.raises(ValueError, match="other probe commands still work"):
        _refuse_withheld_passthrough(sql_spec, "postgres")


def test_the_switch_covers_api_passthrough_too(monkeypatch):
    monkeypatch.setenv("DATAHUB_PROBE_DISABLE_RAW_ACCESS", "true")
    provider = FakeApiProvider()
    api_spec = _spec(provider, "api")
    _only_these(monkeypatch, api_spec)
    with pytest.raises(ValueError, match="DATAHUB_PROBE_DISABLE_RAW_ACCESS"):
        _refuse_withheld_passthrough(api_spec, "mode")


def test_the_switch_leaves_typed_getters_working(monkeypatch):
    # It turns off the passthroughs, not the probe: a connector's typed listings
    # take no caller-supplied query or path, so there is nothing to withhold.
    monkeypatch.setenv("DATAHUB_PROBE_DISABLE_RAW_ACCESS", "true")

    class TypedProvider:
        @probe_method()
        def columns(self, schema: str, table: str) -> List[str]:
            """Columns of a table."""
            return ["a"]

    provider = TypedProvider()
    _enforce_gates(_spec(provider, "columns"), provider, {"schema": "s", "table": "t"})


def test_sql_result_trims_to_the_limit_and_flags_truncation():
    out = sql_result(["c"], [[i] for i in range(5)], 3)
    assert out["rows"] == [[0], [1], [2]]
    assert out["truncated"]
    assert out["row_count"] == 3


def test_sql_result_coerces_values_the_json_encoder_cannot_handle():

    out = sql_result(
        ["d", "n", "b"],
        [[datetime.date(2020, 1, 2), decimal.Decimal("1.5"), b"raw"]],
        10,
    )
    assert out["rows"] == [["2020-01-02", "1.5", "raw"]]
    assert not out["truncated"]


def test_a_listing_command_declares_the_kind_it_returns():
    # The getter knows what it returns; making the caller retype an exact subtype
    # string is a guess it should never have to make.

    spec = _spec(KafkaMetadataProbe, "topics")
    assert spec.kind == "Topic"
    assert spec.to_dict()["kind"] == "Topic"


def test_sql_declares_no_kind_because_the_caller_chooses_what_to_select():

    assert _spec(SqlAlchemyMetadataProbe, "sql").kind is None


# --- the wider switch: nothing that connects runs at all --------------------


def _disabled_probe_env(monkeypatch):
    """A provider that records whether it was ever built."""
    import datahub.ingestion.agent.probe_methods as _pm

    built: List[str] = []

    class _Provider:
        def __enter__(self) -> "_Provider":
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        @probe_method(row_limit_param="limit")
        def containers(self, limit: int = 50) -> List[str]:
            """Every container."""
            return ["c1"]

        @probe_method(name="sql", scoped_sql_param="query")
        def sql(self, query: str) -> Dict[str, object]:
            """Run a catalog query."""
            return {}

        @classmethod
        def for_config(cls, config: object) -> "_Provider":
            built.append("yes")
            return cls()

    class _Config:
        @classmethod
        def probe_provider_class(cls) -> type:
            return _Provider

        @classmethod
        def model_validate(cls, d: object) -> "_Config":
            built.append("config")
            return cls()

    monkeypatch.setattr(_pm, "_provider_class", lambda st: _Provider)
    monkeypatch.setattr(_pm, "config_class_for", lambda st: _Config)
    return _pm, built


@pytest.mark.parametrize("command", ["containers", "sql"])
def test_the_whole_probe_switch_refuses_every_command_that_connects(
    command, monkeypatch
):
    """DATAHUB_PROBE_DISABLE_RAW_ACCESS withholds `sql` and `api` and leaves
    every typed listing live, which is the right granularity for "no
    arbitrary queries" and the wrong one for "this agent does not touch my
    source" -- those listings still authenticate and still return metadata.

    DATAHUB_PROBE_DISABLED is the wider one, and it is enforced in
    run_probe_method because every probe command funnels through it. That
    is what makes it cover commands that do not exist yet, rather than the
    ones someone remembered to list.
    """
    pm_mod, built = _disabled_probe_env(monkeypatch)
    monkeypatch.setenv("DATAHUB_PROBE_DISABLED", "true")

    with pytest.raises(ValueError, match="DATAHUB_PROBE_DISABLED"):
        pm_mod.run_probe_method("postgres", {}, command, {"query": "SELECT 1"})

    # Nothing was built and nothing was dialled: the refusal is not a late
    # failure dressed up, which matters on a source that is slow or down.
    assert built == [], built


def test_the_whole_probe_switch_is_off_by_default(monkeypatch):
    """The control. A switch that is on by accident is an outage."""
    pm_mod, _ = _disabled_probe_env(monkeypatch)
    monkeypatch.delenv("DATAHUB_PROBE_DISABLED", raising=False)

    result = pm_mod.run_probe_method("postgres", {}, "containers", {})
    assert result.result == ["c1"]


def test_the_whole_probe_switch_refuses_before_the_command_is_resolved(monkeypatch):
    """An unknown command name must not change the answer.

    Resolving the command first would make the refusal depend on the caller
    getting the name right, and "unknown probe method 'x'" is a worse answer
    than "the probe is off" -- it invites a retry with a different name.
    """
    pm_mod, built = _disabled_probe_env(monkeypatch)
    monkeypatch.setenv("DATAHUB_PROBE_DISABLED", "true")

    with pytest.raises(ValueError, match="DATAHUB_PROBE_DISABLED"):
        pm_mod.run_probe_method("postgres", {}, "no_such_command", {})
    assert built == []


def test_the_connection_free_commands_still_answer_with_the_probe_off(monkeypatch):
    """The line is the connection, not the feature.

    describe, scaffold, validate, `probe methods` and `probe filter` read
    the connector's own declarations and judge names the caller already
    has. Disabling those too would stop an agent learning what a recipe
    needs or checking one it wrote -- work that never reaches the source.
    """
    monkeypatch.setenv("DATAHUB_PROBE_DISABLED", "true")

    from datahub.ingestion.agent.filter_check import check_filters
    from datahub.ingestion.agent.introspect import describe_source
    from datahub.ingestion.agent.probe_methods import list_probe_methods
    from datahub.ingestion.agent.recipe import scaffold, validate_recipe
    from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes

    assert describe_source("postgres").to_dict()["source_type"] == "postgres"
    assert scaffold("postgres")["source"]
    assert list_probe_methods("postgres")
    assert validate_recipe(scaffold("postgres")) is not None

    verdicts = check_filters(
        source_type="postgres",
        config_dict={"host_port": "h:5432", "username": "u", "password": "p"},
        kind=str(DatasetContainerSubTypes.SCHEMA),
        parent_path=[],
        names=["public"],
    )
    assert verdicts.results[0].target
