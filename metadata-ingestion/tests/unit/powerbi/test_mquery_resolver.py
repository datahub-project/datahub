"""Tests for the M-Query resolver walk, the parser entry point and the bridge.

The resolver tests drive real M expressions through the parser bridge and
assert on which data-access functions (and navigation chains) are found, so
they pin the scoping rules Power BI relies on (conditionals, functions,
parameter resolution). Nested-let scoping and cycle handling are covered
end-to-end in ``tests/integration/powerbi/test_m_parser.py``. The parser
tests cover the error translation into reporter counters; the bridge tests
cover the contract between Python and the V8 runtime, which is the only
thing mocked here.
"""

import gzip
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from unittest import mock

import pytest

from datahub.ingestion.source.powerbi.m_query import (
    _bridge,
    parser as mquery_parser,
    resolver as mquery_resolver,
)
from datahub.ingestion.source.powerbi.m_query._bridge import (
    MQueryBridge,
    MQueryBridgeError,
    MQueryParseError,
    get_bridge,
)
from datahub.ingestion.source.powerbi.m_query.data_classes import (
    DataAccessFunctionDetail,
)
from datahub.ingestion.source.powerbi.m_query.pattern_handler import (
    MSSqlLineage,
    SupportedPattern,
    _data_platform_pair_for,
)
from datahub.utilities.threading_timeout import TimeoutException
from tests.unit.powerbi.conftest import m_query_lineages


def _resolve(
    expression: str, parameters: Optional[Dict[str, str]] = None
) -> List[DataAccessFunctionDetail]:
    return mquery_resolver.resolve_to_data_access_functions(
        get_bridge().parse(expression), parameters=parameters
    )


def _navigation(detail: DataAccessFunctionDetail) -> List[Dict[str, str]]:
    """The navigation chain of a data-access function, outermost step first."""
    steps: List[Dict[str, str]] = []
    accessor = detail.identifier_accessor
    while accessor is not None:
        steps.append(dict(accessor.items))
        accessor = accessor.next
    return steps


# --- resolver: scoping and control flow -------------------------------------


def test_if_expression_over_bound_sources_yields_both_branches() -> None:
    """Dev/prod switching picks between two previously bound sources; both
    candidate sources must be reported, each with the navigation applied to
    the chosen step, so neither environment's lineage is silently dropped."""
    details = _resolve(
        'let\n prod = Sql.Database("prod-host", "db"),\n'
        ' dev = Sql.Database("dev-host", "db"),\n'
        ' picked = if Env = "prod" then prod else dev,\n'
        ' t = picked{[Schema="dbo",Item="t"]}[Data]\nin\n t'
    )
    assert [d.data_access_function_name for d in details] == [
        "Sql.Database",
        "Sql.Database",
    ]
    assert [_navigation(d) for d in details] == [
        [{"Schema": "dbo", "Item": "t"}],
        [{"Schema": "dbo", "Item": "t"}],
    ]


@pytest.mark.xfail(
    strict=True,
    reason=(
        "resolver only records navigation steps applied to an identifier bound "
        "to the data-access call; a `{[...]}[Data]` chained directly onto the "
        "invocation is walked to the call but its accessor chain is dropped, so "
        "the table is never resolved."
    ),
)
def test_navigation_chained_directly_onto_the_call_is_resolved() -> None:
    details = _resolve(
        'let\n t = Sql.Database("h", "db"){[Schema="dbo",Item="t"]}[Data]\nin\n t'
    )
    assert [_navigation(d) for d in details] == [[{"Schema": "dbo", "Item": "t"}]]


def test_parentheses_do_not_break_the_navigation_chain() -> None:
    details = _resolve(
        'let\n Source = Sql.Database("h", "library"),\n'
        ' tbl = (Source{[Schema="dbo",Item="book_issue"]}[Data])\nin\n (tbl)'
    )
    assert len(details) == 1
    assert _navigation(details[0]) == [{"Schema": "dbo", "Item": "book_issue"}]


def test_function_body_is_walked() -> None:
    details = _resolve(
        'let\n load = (db as text) => Sql.Database("h", db){[Schema="dbo",Item="t"]}[Data]\n'
        "in\n load"
    )
    assert [d.data_access_function_name for d in details] == ["Sql.Database"]


def test_unbound_output_identifier_yields_nothing() -> None:
    assert _resolve('let\n a = Sql.Database("h", "d")\nin\n missing') == []


def test_expression_without_let_yields_nothing() -> None:
    assert _resolve('Sql.Database("h", "d")') == []


def test_literal_output_yields_nothing() -> None:
    assert _resolve("let\n a = 1\nin\n a") == []


def test_field_access_without_item_step_keeps_the_source_call() -> None:
    details = _resolve(
        'let\n Source = Sql.Database("h", "d"),\n t = Source[Data]\nin\n t'
    )
    assert [d.data_access_function_name for d in details] == ["Sql.Database"]
    assert details[0].identifier_accessor is None


def test_navigation_record_resolves_parameter_references() -> None:
    """Parameter names are matched case-insensitively, as Power BI does."""
    details = _resolve(
        'let\n Source = Sql.Database("h", "d"),\n'
        " t = Source{[Schema=SchemaParam,Item=TableParam]}[Data]\nin\n t",
        parameters={"schemaparam": "dbo", "TableParam": "book_issue"},
    )
    assert _navigation(details[0]) == [{"Schema": "dbo", "Item": "book_issue"}]


def test_rename_columns_with_non_text_pairs_records_no_step() -> None:
    details = _resolve(
        'let\n Source = Sql.Database("h", "d"){[Schema="dbo",Item="t"]}[Data],\n'
        ' renamed = Table.RenameColumns(Source, {{"a", 1}})\nin\n renamed'
    )
    assert len(details) == 1
    assert details[0].column_rename_steps == []


def test_rename_columns_with_non_list_argument_records_no_step() -> None:
    details = _resolve(
        'let\n Source = Sql.Database("h", "d"){[Schema="dbo",Item="t"]}[Data],\n'
        ' renamed = Table.RenameColumns(Source, "a")\nin\n renamed'
    )
    assert len(details) == 1
    assert details[0].column_rename_steps == []


# --- parser entry point -----------------------------------------------------


def test_blank_expression_is_not_counted_as_a_parse_attempt() -> None:
    lineages, reporter = m_query_lineages("   \n ")
    assert lineages == []
    assert reporter.m_query_parse_attempts == 0


def test_native_query_is_skipped_when_native_parsing_is_disabled() -> None:
    lineages, reporter = m_query_lineages(
        'let\n Source = Value.NativeQuery(Snowflake.Databases("h","w")'
        '{[Name="db"]}[Data], "select * from db.s.t")\nin\n Source',
        config_overrides={"native_query_parsing": False},
    )
    assert lineages == []
    assert reporter.m_query_native_query_skipped == 1
    assert reporter.m_query_parse_attempts == 0


def test_expression_without_data_access_function_counts_as_no_lineage() -> None:
    lineages, reporter = m_query_lineages("let\n a = 1\nin\n a")
    assert lineages == []
    assert reporter.m_query_parse_successes == 1
    assert reporter.m_query_resolver_no_lineage == 1


def test_handler_exception_is_reported_as_unknown_pattern() -> None:
    """A handler that raises must land in the report as an unknown pattern
    rather than abort ingestion of the dataset."""
    with mock.patch.object(
        MSSqlLineage, "create_lineage", autospec=True, side_effect=RuntimeError("boom")
    ):
        lineages, reporter = m_query_lineages(
            'let\n Source = Sql.Database("h", "library"),\n'
            ' tbl = Source{[Schema="dbo",Item="book_issue"]}[Data]\nin\n tbl'
        )
    assert lineages == []
    assert reporter.m_query_resolver_errors == 1
    assert any(w.title == "Unknown M-Query Pattern" for w in reporter.warnings)


def test_bridge_failure_is_reported_and_bridge_is_reset() -> None:
    """A V8 failure must not poison later parses: the singleton bridge is
    discarded so the next expression gets a fresh runtime."""
    failing_bridge = mock.create_autospec(MQueryBridge, instance=True)
    failing_bridge.parse.side_effect = MQueryBridgeError("V8 crashed")
    with (
        mock.patch.object(mquery_parser, "get_bridge", return_value=failing_bridge),
        mock.patch.object(mquery_parser, "_clear_bridge", autospec=True) as clear,
    ):
        lineages, reporter = m_query_lineages(
            'let\n a = Sql.Database("h", "d")\nin\n a'
        )

    assert lineages == []
    assert reporter.m_query_parse_unknown_errors == 1
    assert any(
        w.title == "Unable to parse M-Query expression" for w in reporter.warnings
    )
    clear.assert_called_once()


def test_parse_timeout_is_reported_and_bridge_is_reset() -> None:
    slow_bridge = mock.create_autospec(MQueryBridge, instance=True)
    slow_bridge.parse.side_effect = TimeoutException("timed out")
    with (
        mock.patch.object(mquery_parser, "get_bridge", return_value=slow_bridge),
        mock.patch.object(mquery_parser, "_clear_bridge", autospec=True) as clear,
    ):
        lineages, reporter = m_query_lineages(
            'let\n a = Sql.Database("h", "d")\nin\n a'
        )

    assert lineages == []
    assert reporter.m_query_parse_timeouts == 1
    assert any(w.title == "M-Query Parsing Timeout" for w in reporter.warnings)
    clear.assert_called_once()


def test_unknown_data_access_function_has_no_pattern_handler() -> None:
    assert SupportedPattern.get_pattern_handler("Web.Contents") is None


def test_unknown_datahub_platform_is_a_programming_error() -> None:
    with pytest.raises(ValueError):
        _data_platform_pair_for("not-a-platform")


# --- bridge contract --------------------------------------------------------


class _FakePromise:
    def __init__(self, value: Any) -> None:
        self._value = value

    def get(self) -> Any:
        return self._value


class _FakeRacer:
    """Stands in for MiniRacer: records the bundle eval and answers parse calls."""

    def __init__(
        self,
        eval_result: Any = None,
        eval_error: Optional[Exception] = None,
        close_error: Optional[Exception] = None,
    ):
        self._eval_result = eval_result
        self._eval_error = eval_error
        self._close_error = close_error
        self.closed = False

    def eval(self, script: str) -> Any:
        if script.startswith("parseExpression("):
            if self._eval_error is not None:
                raise self._eval_error
            return self._eval_result
        return None

    def close(self) -> None:
        self.closed = True
        if self._close_error is not None:
            raise self._close_error


def _bridge_with(racer: _FakeRacer) -> MQueryBridge:
    with (
        mock.patch("py_mini_racer.MiniRacer", return_value=racer),
        mock.patch("py_mini_racer.JSPromise", _FakePromise),
    ):
        return MQueryBridge()


def _parse_with(racer: _FakeRacer, expression: str = "let a = 1 in a") -> Any:
    bridge = _bridge_with(racer)
    with mock.patch("py_mini_racer.JSPromise", _FakePromise):
        return bridge.parse(expression)


def test_bridge_returns_node_map_keyed_by_int() -> None:
    payload = json.dumps(
        {"ok": True, "nodeIdMap": [["1", {"kind": "LetExpression", "id": 1}]]}
    )
    assert _parse_with(_FakeRacer(_FakePromise(payload))) == {
        1: {"kind": "LetExpression", "id": 1}
    }


def test_bridge_parse_error_carries_the_expression() -> None:
    payload = json.dumps({"ok": False, "error": "unexpected token"})
    with pytest.raises(MQueryParseError) as excinfo:
        _parse_with(_FakeRacer(_FakePromise(payload)), expression="let x")
    assert excinfo.value.expression == "let x"


@pytest.mark.parametrize(
    "racer",
    [
        _FakeRacer(eval_result="not a promise"),
        _FakeRacer(eval_error=RuntimeError("JS exception")),
        _FakeRacer(_FakePromise(b"bytes")),
        _FakeRacer(_FakePromise("{not json")),
        _FakeRacer(_FakePromise(json.dumps({"ok": True}))),
    ],
    ids=["non-promise", "v8-error", "non-string", "malformed-json", "no-node-map"],
)
def test_bridge_malformed_runtime_responses_raise_bridge_error(
    racer: _FakeRacer,
) -> None:
    with pytest.raises(MQueryBridgeError):
        _parse_with(racer)


def test_bridge_missing_bundle_is_an_import_error(tmp_path: Path) -> None:
    with mock.patch.object(_bridge, "_BUNDLE_PATH", tmp_path / "missing.js.gz"):
        with pytest.raises(ImportError, match="not found"):
            MQueryBridge()


def test_bridge_corrupt_bundle_is_an_import_error(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle.js.gz"
    bundle.write_bytes(b"definitely not gzip")
    with (
        mock.patch.object(_bridge, "_BUNDLE_PATH", bundle),
        mock.patch("py_mini_racer.MiniRacer", return_value=_FakeRacer()),
    ):
        with pytest.raises(ImportError, match="corrupt"):
            MQueryBridge()


@pytest.mark.parametrize(
    "close_error", [None, RuntimeError("already closed")], ids=["clean", "failing"]
)
def test_clear_bridge_drops_the_singleton_even_when_the_v8_context_fails_to_close(
    tmp_path: Path, close_error: Optional[Exception]
) -> None:
    """``_clear_bridge`` is the recovery path after a V8 failure; a context
    whose ``close`` raises must still be dropped so the next ``get_bridge``
    builds a fresh runtime instead of reusing the broken one."""
    bundle = tmp_path / "bundle.js.gz"
    bundle.write_bytes(gzip.compress(b"var parseExpression = null;"))
    racer = _FakeRacer(close_error=close_error)
    _bridge._clear_bridge()
    try:
        with (
            mock.patch.object(_bridge, "_BUNDLE_PATH", bundle),
            mock.patch("py_mini_racer.MiniRacer", return_value=racer),
        ):
            assert get_bridge() is get_bridge()
        _bridge._clear_bridge()
        assert racer.closed
        assert _bridge._bridge_instance is None
    finally:
        # Leave no fake behind: the next real parse must build a fresh bridge.
        _bridge._clear_bridge()
