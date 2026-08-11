"""A raw-SQL or raw-path parameter is gated by the framework, not by the getter.

`sql` and `api` are ordinary probe methods, so `probe run` reaches them through
the same path as `columns` or `topics`. What keeps them safe is that the getter
*declares* which parameter carries the dangerous value and the framework checks
it before invoking -- a connector cannot forget a check it does not perform.
"""

from typing import Dict, List, Sequence

import pytest

from datahub.ingestion.agent.api_gate import ApiScopeError
from datahub.ingestion.agent.probe_methods import (
    MAX_PROBE_ITEMS,
    ProbeMethodSpec,
    _bounded_kwargs,
    _enforce_gates,
    probe_method,
)
from datahub.ingestion.agent.sql_gate import SqlScopeError
from datahub.ingestion.agent.sql_query import sql_result


class FakeSqlProvider:
    sql_dialect = "postgres"

    def __init__(self) -> None:
        self.ran: List[str] = []

    @probe_method(name="sql", scoped_sql_param="query", row_limit_param="limit")
    def sql(self, query: str, limit: int = 50) -> Dict[str, object]:
        """Run a catalog query."""
        self.ran.append(query)
        return sql_result(["c"], [["v"]], limit)


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
    bounded = _bounded_kwargs(
        _spec(FakeSqlProvider(), "sql"), {"query": "SELECT 1", "limit": 50}
    )
    assert bounded["limit"] == 50


def test_an_omitted_row_limit_is_left_to_the_getter_default():
    bounded = _bounded_kwargs(_spec(FakeSqlProvider(), "sql"), {"query": "SELECT 1"})
    assert "limit" not in bounded


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
    assert out["truncated"] is True


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
        _enforce_gates(
            _spec(provider, "sql"),
            provider,
            {"query": "SELECT table_name FROM information_schema.tables"},
        )
    assert provider.ran == []


def test_the_switch_covers_api_passthrough_too(monkeypatch):
    monkeypatch.setenv("DATAHUB_PROBE_DISABLE_RAW_ACCESS", "true")
    provider = FakeApiProvider()
    with pytest.raises(ValueError, match="DATAHUB_PROBE_DISABLE_RAW_ACCESS"):
        _enforce_gates(_spec(provider, "api"), provider, {"path": "/spaces"})


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
    assert out["truncated"] is True
    assert out["row_count"] == 3


def test_sql_result_coerces_values_the_json_encoder_cannot_handle():
    import datetime
    import decimal

    out = sql_result(
        ["d", "n", "b"],
        [[datetime.date(2020, 1, 2), decimal.Decimal("1.5"), b"raw"]],
        10,
    )
    assert out["rows"] == [["2020-01-02", "1.5", "raw"]]
    assert out["truncated"] is False


def test_a_listing_command_declares_the_kind_it_returns():
    # The getter knows what it returns; making the caller retype an exact subtype
    # string is a guess it should never have to make.
    from datahub.ingestion.source.kafka.kafka_probe import KafkaMetadataProbe

    spec = _spec(KafkaMetadataProbe, "topics")
    assert spec.kind == "Topic"
    assert spec.to_dict()["kind"] == "Topic"


def test_sql_declares_no_kind_because_the_caller_chooses_what_to_select():
    from datahub.ingestion.source.sql.sqlalchemy_probe import SqlAlchemyMetadataProbe

    assert _spec(SqlAlchemyMetadataProbe, "sql").kind is None
