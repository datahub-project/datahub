"""Tests for the DataHub extraction scope that prevents duplicated OpenLineage work.

The DataHub listener calls the operator's ``get_openlineage_facets_on_*`` methods
in-process, while Airflow's OpenLineage provider calls the same methods inside its
own ``os.fork()``. Those methods are not side-effect free: they open warehouse
connections and (for Snowflake) emit OpenLineage query events. These tests pin the
guards that keep DataHub's pass from re-running work it does not consume.
"""

from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

import pytest
from openlineage.client.event_v2 import Dataset

from datahub_airflow_plugin._constants import DATAHUB_SQL_PARSING_RESULT_KEY
from datahub_airflow_plugin.airflow3 import _sql_parser_patch
from datahub_airflow_plugin.airflow3._extraction_scope import (
    datahub_extraction_scope,
    in_datahub_extraction,
)

_PATCH_MOD = "datahub_airflow_plugin.airflow3._sql_parser_patch"


@pytest.fixture
def fake_parse_result():
    return SimpleNamespace(
        in_tables=["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.src,PROD)"],
        out_tables=["urn:li:dataset:(urn:li:dataPlatform:snowflake,db.sch.dst,PROD)"],
        column_lineage=None,
        debug_info=SimpleNamespace(error=None, table_error=None),
    )


@pytest.fixture
def sql_parser_env(fake_parse_result):
    """Drive the patched SQLParser method with the original parser stubbed out."""
    provider_dataset = Dataset(namespace="snowflake://db", name="db.sch.src")
    original = mock.MagicMock(
        return_value=_sql_parser_patch.OperatorLineage(
            inputs=[provider_dataset],
            outputs=[provider_dataset],
            job_facets={},
            run_facets={"externalQuery": object()},
        )
    )
    config = SimpleNamespace(
        disable_openlineage_plugin=False,
        enable_multi_statement_sql_parsing=False,
    )
    with (
        mock.patch.object(_sql_parser_patch, "_original_sql_parser_method", original),
        mock.patch(
            f"{_PATCH_MOD}.parse_sql_with_datahub", return_value=fake_parse_result
        ),
        mock.patch(
            "datahub_airflow_plugin._config.get_lineage_config", return_value=config
        ),
        mock.patch(
            "datahub_airflow_plugin.datahub_listener.get_airflow_plugin_listener",
            return_value=None,
        ),
        mock.patch(f"{_PATCH_MOD}.get_configured_env", return_value="PROD"),
    ):
        yield original


def _call_patched_parser():
    return _sql_parser_patch._datahub_generate_openlineage_metadata_from_sql(
        self=SimpleNamespace(dialect="snowflake", default_schema="sch"),
        sql="insert into dst select * from src",
        hook=mock.MagicMock(),
        # The patch annotates this as ``dict`` but the provider passes a
        # ``DatabaseInfo``; the code only reads ``.scheme`` / ``.database``.
        database_info=cast(dict, SimpleNamespace(scheme="snowflake", database="db")),
    )


def test_scope_defaults_to_inactive():
    assert in_datahub_extraction() is False


def test_scope_resets_after_exit():
    with datahub_extraction_scope():
        assert in_datahub_extraction() is True
    assert in_datahub_extraction() is False


def test_original_parser_runs_outside_datahub_scope(sql_parser_env):
    """The OpenLineage provider's own pass must keep hitting the warehouse."""
    result = _call_patched_parser()

    assert sql_parser_env.call_count == 1
    assert sql_parser_env.call_args.kwargs["use_connection"] is True
    # The provider's own iolets and facets survive untouched.
    assert [d.name for d in result.inputs] == ["db.sch.src"]
    assert "externalQuery" in result.run_facets
    assert DATAHUB_SQL_PARSING_RESULT_KEY in result.run_facets


def test_original_parser_skipped_inside_datahub_scope(sql_parser_env):
    """DataHub's pass must not re-run the provider parser at all.

    Its only warehouse-touching work is the provider parser, and DataHub takes its
    URNs from its own SQL-parsing facet, which is dialect- and quote-aware.
    """
    with datahub_extraction_scope():
        _call_patched_parser()

    assert sql_parser_env.call_count == 0


def test_datahub_scope_returns_facet_without_provider_iolets(sql_parser_env):
    """URNs come from the DataHub facet alone.

    Reconstructing the warehouse's stored casing from a parse tree is not possible:
    the parser strips quotes, so `FROM MySchema.MyTable` and `FROM "MySchema"."MyTable"`
    are indistinguishable while needing opposite folding on a case-folding database.
    The listener reads in_tables/out_tables from this facet, which is produced by a
    dialect- and quote-aware parser, so no reconstruction is attempted.
    """
    with datahub_extraction_scope():
        result = _call_patched_parser()

    assert result.inputs == []
    assert result.outputs == []
    assert DATAHUB_SQL_PARSING_RESULT_KEY in result.run_facets


class TestSqlOperatorCompleteGuard:
    """``get_openlineage_facets_on_complete`` must not run side effects for DataHub.

    For common-sql operators the complete pass is ``get_openlineage_facets_on_start()``
    merged with ``hook.get_openlineage_database_specific_lineage()``. DataHub reads
    none of the latter, but on Snowflake producing it emits OpenLineage query events
    straight to the provider's transport with a fresh runId.
    """

    @pytest.fixture
    def guarded_operator_class(self) -> Any:
        from datahub_airflow_plugin.airflow3._sql_operator_complete_patch import (
            SqlOperatorCompletePatch,
        )

        patcher = SqlOperatorCompletePatch()
        patcher.patch()
        try:
            from airflow.providers.common.sql.operators.sql import (
                SQLExecuteQueryOperator,
            )

            yield SQLExecuteQueryOperator
        finally:
            patcher.unpatch()

    @staticmethod
    def _fake_operator() -> Any:
        hook = mock.MagicMock()
        hook.get_openlineage_database_specific_lineage.return_value = None
        start_lineage: Any = _sql_parser_patch.OperatorLineage(
            inputs=[], outputs=[], job_facets={}, run_facets={}
        )
        return SimpleNamespace(
            get_openlineage_facets_on_start=mock.MagicMock(return_value=start_lineage),
            get_db_hook=mock.MagicMock(return_value=hook),
            _hook=hook,
            _start_lineage=start_lineage,
        )

    def test_database_specific_lineage_skipped_inside_scope(
        self, guarded_operator_class
    ):
        op = self._fake_operator()

        with datahub_extraction_scope():
            result = guarded_operator_class.get_openlineage_facets_on_complete(
                op, mock.MagicMock()
            )

        op._hook.get_openlineage_database_specific_lineage.assert_not_called()
        assert result is op._start_lineage

    def test_database_specific_lineage_runs_outside_scope(self, guarded_operator_class):
        op = self._fake_operator()

        guarded_operator_class.get_openlineage_facets_on_complete(op, mock.MagicMock())

        op._hook.get_openlineage_database_specific_lineage.assert_called_once()

    def test_subclass_override_still_wins(self, guarded_operator_class):
        """DataHub's own operator patches install on subclasses and must not be bypassed."""
        sentinel = object()
        subclass: Any = type(
            "FakePatchedOperator",
            (guarded_operator_class,),
            {
                "get_openlineage_facets_on_complete": lambda self, ti: sentinel,
            },
        )
        op = self._fake_operator()

        with datahub_extraction_scope():
            result = subclass.get_openlineage_facets_on_complete(op, mock.MagicMock())

        assert result is sentinel

    def test_operator_patch_registered_later_captures_the_guard(
        self, guarded_operator_class
    ):
        """Ordering constraint: operator patches capture their original by MRO lookup.

        ``patch_teradata_operator`` does ``original = TeradataOperator.get_openlineage_
        facets_on_complete`` and installs its wrapper on the subclass. If the guard is
        installed after that, the subclass entry shadows it and the guard never runs.
        Installing the guard first means any later operator patch routes through it.
        """
        subclass: Any = type("FakeTeradataOperator", (guarded_operator_class,), {})
        captured_original = subclass.get_openlineage_facets_on_complete

        calls = []

        def teradata_style_wrapper(self, ti):
            calls.append("teradata")
            return captured_original(self, ti)

        subclass.get_openlineage_facets_on_complete = teradata_style_wrapper
        op = self._fake_operator()

        with datahub_extraction_scope():
            subclass.get_openlineage_facets_on_complete(op, mock.MagicMock())

        assert calls == ["teradata"]
        op._hook.get_openlineage_database_specific_lineage.assert_not_called()

    def test_patch_is_idempotent(self, guarded_operator_class):
        from datahub_airflow_plugin.airflow3._sql_operator_complete_patch import (
            SqlOperatorCompletePatch,
        )

        before = guarded_operator_class.get_openlineage_facets_on_complete
        SqlOperatorCompletePatch().patch()

        assert guarded_operator_class.get_openlineage_facets_on_complete is before


class TestListenerEntersScope:
    def test_facet_method_sees_active_scope(self):
        """The listener must mark its own facet calls, or neither guard engages."""
        from datahub_airflow_plugin.airflow3.datahub_listener import DataHubListener

        observed = []

        def get_openlineage_facets_on_start():
            observed.append(in_datahub_extraction())
            return None

        task = SimpleNamespace(
            task_id="t1",
            get_openlineage_facets_on_start=get_openlineage_facets_on_start,
        )
        listener = SimpleNamespace(config=SimpleNamespace(cluster="PROD"))

        DataHubListener._extract_lineage_from_airflow3(
            cast(Any, listener), cast(Any, task), mock.MagicMock(), complete=False
        )

        assert observed == [True]
        assert in_datahub_extraction() is False


class TestPatchInstallOrder:
    def test_guard_is_installed_before_operator_patches(self):
        """Operator patches capture their original by MRO lookup off the subclass.

        A guard installed after them is shadowed by the subclass entry they write, so
        the install order in _airflow_compat is load-bearing, not cosmetic.
        """
        import inspect

        from datahub_airflow_plugin.airflow3 import _airflow_compat

        source = inspect.getsource(_airflow_compat)
        guard_at = source.index("patch_sql_execute_query_operator()")
        operator_patches = [
            source.index(name)
            for name in (
                "patch_athena_operator()",
                "patch_bigquery_insert_job_operator()",
                "patch_teradata_operator()",
            )
        ]

        assert guard_at < min(operator_patches)


class TestPassCountBaseline:
    """The reported metric: enabling DataHub must not multiply warehouse passes.

    Simulates one task run with both listeners active. Airflow's provider calls the
    facet methods from its fork (outside the scope); the DataHub listener calls them
    in-process (inside it). ``_original_sql_parser_method`` is the only thing that
    issues ``information_schema`` queries, and
    ``get_openlineage_database_specific_lineage`` is the only thing that emits
    provider-side query events.
    """

    def test_datahub_adds_no_warehouse_passes(self, sql_parser_env):
        from datahub_airflow_plugin.airflow3._sql_operator_complete_patch import (
            SqlOperatorCompletePatch,
        )

        patcher = SqlOperatorCompletePatch()
        patcher.patch()
        try:
            from airflow.providers.common.sql.operators.sql import (
                SQLExecuteQueryOperator,
            )

            hook = mock.MagicMock()
            hook.get_openlineage_database_specific_lineage.return_value = None
            operator: Any = SimpleNamespace(
                get_openlineage_facets_on_start=_call_patched_parser,
                get_db_hook=mock.MagicMock(return_value=hook),
            )
            on_complete = SQLExecuteQueryOperator.get_openlineage_facets_on_complete

            # Provider's forked passes, then DataHub's in-process passes.
            operator.get_openlineage_facets_on_start()
            with datahub_extraction_scope():
                operator.get_openlineage_facets_on_start()
            on_complete(operator, mock.MagicMock())
            with datahub_extraction_scope():
                on_complete(operator, mock.MagicMock())
        finally:
            patcher.unpatch()

        # Two passes, not four: exactly what the provider alone would have done.
        assert sql_parser_env.call_count == 2
        assert all(c.kwargs["use_connection"] for c in sql_parser_env.call_args_list)
        hook.get_openlineage_database_specific_lineage.assert_called_once()
