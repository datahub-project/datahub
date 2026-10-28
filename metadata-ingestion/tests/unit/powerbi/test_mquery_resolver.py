"""Tests for the M-Query resolver walk, the parser entry point and the bridge.

The resolver tests drive real M expressions through the parser bridge and
assert on which data-access functions (and navigation chains) are found, so
they pin the scoping rules Power BI relies on (nested lets, conditionals,
circular references, parameter resolution). The parser tests cover the error
translation into reporter counters; the bridge tests cover the contract
between Python and the V8 runtime, which is the only thing mocked here.
"""

import gzip
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, cast
from unittest import mock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.powerbi.config import (
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.dataplatform_instance_resolver import (
    create_dataplatform_instance_resolver,
)
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
    Lineage,
)
from datahub.ingestion.source.powerbi.m_query.pattern_handler import (
    SupportedPattern,
    _data_platform_pair_for,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import Table
from datahub.utilities.threading_timeout import TimeoutException

_MSSQL_TABLE = "urn:li:dataset:(urn:li:dataPlatform:mssql,library.dbo.book_issue,PROD)"


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


def _parse(
    expression: str, **config_overrides: Any
) -> Tuple[List[Lineage], PowerBiDashboardSourceReport]:
    config = PowerBiDashboardSourceConfig.model_validate(
        {
            "tenant_id": "fake",
            "client_id": "foo",
            "client_secret": "bar",
            **config_overrides,
        }
    )
    reporter = PowerBiDashboardSourceReport()
    lineages = mquery_parser.get_upstream_tables(
        table=Table(name="t", full_name="ws.ds.t", expression=expression),
        reporter=reporter,
        platform_instance_resolver=create_dataplatform_instance_resolver(config),
        ctx=PipelineContext(run_id="test"),
        config=config,
    )
    return lineages, reporter


# --- resolver: scoping and control flow -------------------------------------


def test_nested_let_can_reach_steps_bound_by_the_outer_let() -> None:
    details = _resolve(
        'let\n Source = Sql.Database("h", "library"),\n'
        ' result = let inner = Source{[Schema="dbo",Item="book_issue"]}[Data] in inner\n'
        "in\n result"
    )
    assert [d.data_access_function_name for d in details] == ["Sql.Database"]
    assert _navigation(details[0]) == [{"Schema": "dbo", "Item": "book_issue"}]


def test_if_expression_yields_both_branches() -> None:
    # Dev/prod switching: both candidate sources must be reported so neither
    # environment's lineage is silently dropped.
    details = _resolve(
        'let\n prod = Sql.Database("prod-host", "db"){[Schema="dbo",Item="t"]}[Data],\n'
        ' dev = Sql.Database("dev-host", "db"){[Schema="dbo",Item="t"]}[Data],\n'
        ' picked = if Env = "prod" then prod else dev\nin\n picked'
    )
    assert len(details) == 2


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


def test_circular_reference_terminates_without_lineage() -> None:
    assert _resolve("let\n a = b,\n b = A\nin\n a") == []


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
    lineages, reporter = _parse("   \n ")
    assert lineages == []
    assert reporter.m_query_parse_attempts == 0


def test_native_query_is_skipped_when_native_parsing_is_disabled() -> None:
    lineages, reporter = _parse(
        'let\n Source = Value.NativeQuery(Snowflake.Databases("h","w")'
        '{[Name="db"]}[Data], "select * from db.s.t")\nin\n Source',
        native_query_parsing=False,
        extract_column_level_lineage=False,
    )
    assert lineages == []
    assert reporter.m_query_native_query_skipped == 1
    assert reporter.m_query_parse_attempts == 0


def test_expression_without_data_access_function_counts_as_no_lineage() -> None:
    lineages, reporter = _parse("let\n a = 1\nin\n a")
    assert lineages == []
    assert reporter.m_query_parse_successes == 1
    assert reporter.m_query_resolver_no_lineage == 1


def test_handler_exception_is_reported_as_unknown_pattern() -> None:
    # The second Sql.Databases step is missing its Schema/Item keys, which the
    # handler does not guard against; the error must land in the report
    # rather than abort ingestion of the dataset.
    lineages, reporter = _parse(
        'let\n Source = Sql.Databases("h"),\n db = Source{[Name="library"]}[Data],\n'
        ' tbl = db{[Name="book_issue"]}[Data]\nin\n tbl'
    )
    assert lineages == []
    assert reporter.m_query_resolver_errors == 1
    assert any(w.title == "Unknown M-Query Pattern" for w in reporter.warnings)


def test_bridge_failure_is_reported_and_bridge_is_reset() -> None:
    failing_bridge = mock.MagicMock(spec=MQueryBridge)
    failing_bridge.parse.side_effect = MQueryBridgeError("V8 crashed")
    with (
        mock.patch.object(mquery_parser, "get_bridge", return_value=failing_bridge),
        mock.patch.object(mquery_parser, "_clear_bridge") as clear_bridge,
    ):
        lineages, reporter = _parse('let\n a = Sql.Database("h", "d")\nin\n a')

    assert lineages == []
    assert reporter.m_query_parse_unknown_errors == 1
    assert any(
        w.title == "Unable to parse M-Query expression" for w in reporter.warnings
    )
    cast(mock.MagicMock, clear_bridge).assert_called_once()


def test_parse_timeout_is_reported_and_bridge_is_reset() -> None:
    slow_bridge = mock.MagicMock(spec=MQueryBridge)
    slow_bridge.parse.side_effect = TimeoutException("timed out")
    with (
        mock.patch.object(mquery_parser, "get_bridge", return_value=slow_bridge),
        mock.patch.object(mquery_parser, "_clear_bridge") as clear_bridge,
    ):
        lineages, reporter = _parse('let\n a = Sql.Database("h", "d")\nin\n a')

    assert lineages == []
    assert reporter.m_query_parse_timeouts == 1
    assert any(w.title == "M-Query Parsing Timeout" for w in reporter.warnings)
    cast(mock.MagicMock, clear_bridge).assert_called_once()


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

    def __init__(self, eval_result: Any = None, eval_error: Optional[Exception] = None):
        self._eval_result = eval_result
        self._eval_error = eval_error
        self.closed = False

    def eval(self, script: str) -> Any:
        if script.startswith("parseExpression("):
            if self._eval_error is not None:
                raise self._eval_error
            return self._eval_result
        return None

    def close(self) -> None:
        self.closed = True


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


def test_clear_bridge_closes_the_v8_context_even_when_close_fails(
    tmp_path: Path,
) -> None:
    bundle = tmp_path / "bundle.js.gz"
    bundle.write_bytes(gzip.compress(b"var parseExpression = null;"))
    racer = _FakeRacer()
    _bridge._clear_bridge()
    try:
        with (
            mock.patch.object(_bridge, "_BUNDLE_PATH", bundle),
            mock.patch("py_mini_racer.MiniRacer", return_value=racer),
        ):
            assert get_bridge() is get_bridge()
        _bridge._clear_bridge()
        assert racer.closed

        failing = mock.MagicMock()
        failing._ctx.close.side_effect = RuntimeError("already closed")
        _bridge._bridge_instance = failing
        _bridge._clear_bridge()
        assert _bridge._bridge_instance is None
    finally:
        # Leave no fake behind: the next real parse must build a fresh bridge.
        _bridge._clear_bridge()
