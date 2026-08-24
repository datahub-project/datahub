"""Unit tests for the Airflow 3 DataHub listener."""

import sys
from types import ModuleType, SimpleNamespace
from typing import Any, Iterator, List
from unittest.mock import MagicMock, patch

import pytest
from openlineage.client.facet import SqlJobFacet

import datahub_airflow_plugin.airflow3.datahub_listener as listener_mod
from datahub.metadata.schema_classes import QueryPropertiesClass, QuerySubjectsClass
from datahub.metadata.urns import QueryUrn
from datahub.sql_parsing.fingerprint_utils import generate_hash
from datahub.sql_parsing.sqlglot_lineage import (
    ColumnLineageInfo,
    ColumnRef,
    DownstreamColumnRef,
    SqlParsingResult,
)
from datahub_airflow_plugin.airflow3.datahub_listener import DataHubListener


class AssetAlias:
    """Minimal AssetAlias stand-in.

    is_airflow_asset_alias() walks the MRO looking for class name 'AssetAlias',
    so the class itself must be named AssetAlias. It must NOT have a 'uri'
    attribute (presence of uri excludes the object from is_airflow_asset_alias).
    """

    def __init__(self, name: str):
        self.name = name


class Asset:
    """Minimal Airflow Asset stand-in (has uri)."""

    def __init__(self, uri: str):
        self.uri = uri


def _make_listener_stub(capture_airflow_assets: bool = True) -> Any:
    """Return a SimpleNamespace bound to DataHubListener._get_outlet_urns.

    We avoid instantiating DataHubListener to skip its emitter / log-level
    setup. _get_outlet_urns only reads self.config.{capture_airflow_assets,
    cluster}, so a SimpleNamespace is sufficient.
    """
    config = SimpleNamespace(
        capture_airflow_assets=capture_airflow_assets,
        cluster="PROD",
    )
    return SimpleNamespace(config=config)


def _call_get_outlet_urns(
    stub: Any,
    outlets: List[Any],
    complete: bool,
    task_instance: Any = None,
    session: Any = None,
) -> List[str]:
    """Invoke the unbound _get_outlet_urns method against the stub."""
    task = MagicMock()
    task.task_id = "my_task"
    dagrun = MagicMock()
    dagrun.dag_id = "my_dag"
    dagrun.run_id = "run_1"
    if task_instance is None:
        task_instance = MagicMock()
        task_instance.map_index = -1

    with patch(
        "datahub_airflow_plugin.airflow3.datahub_listener.get_task_outlets",
        return_value=outlets,
    ):
        return DataHubListener._get_outlet_urns(
            stub,
            task,
            dagrun,
            task_instance,
            complete=complete,
            session=session,
        )


class TestGetOutletUrnsAliasOmittedAtStart:
    """AssetAlias outlets must NOT contribute URNs at task start.

    The real Asset URI is only known once the task body runs, so emitting a
    placeholder URN derived from the alias name would create a phantom
    lineage edge that is later orphaned when the task completes with the
    resolved URN.
    """

    def test_alias_only_outlet_returns_empty_when_not_complete(self) -> None:
        stub = _make_listener_stub(capture_airflow_assets=True)
        outlets = [AssetAlias("parsed_data")]

        with (
            patch(
                "datahub_airflow_plugin.airflow3.datahub_listener.extract_urns_from_task_instance_outlet_events"
            ) as mock_in_proc,
            patch(
                "datahub_airflow_plugin.airflow3.datahub_listener.extract_urns_from_resolved_alias_events"
            ) as mock_db,
        ):
            urns = _call_get_outlet_urns(stub, outlets, complete=False)

        assert urns == []
        # Neither resolution path should run at task start
        mock_in_proc.assert_not_called()
        mock_db.assert_not_called()

    def test_mixed_outlets_only_emit_non_alias_at_start(self) -> None:
        stub = _make_listener_stub(capture_airflow_assets=True)
        outlets = [Asset("s3://bucket/known"), AssetAlias("parsed_data")]

        with patch(
            "datahub_airflow_plugin.airflow3.datahub_listener.extract_urns_from_task_instance_outlet_events"
        ) as mock_in_proc:
            urns = _call_get_outlet_urns(stub, outlets, complete=False)

        # The concrete Asset is emitted; the alias is silently dropped.
        assert urns == ["urn:li:dataset:(urn:li:dataPlatform:s3,bucket/known,PROD)"]
        mock_in_proc.assert_not_called()


class TestGetOutletUrnsAliasResolutionAtCompletion:
    """At task completion the listener must resolve alias outlets via
    runtime context (or DB fallback) and forward the Airflow-provided
    session."""

    def test_in_process_resolution_used_when_available(self) -> None:
        stub = _make_listener_stub(capture_airflow_assets=True)
        outlets = [AssetAlias("parsed_data")]
        resolved = ["urn:li:dataset:(urn:li:dataPlatform:s3,bucket/runtime,PROD)"]

        with (
            patch(
                "datahub_airflow_plugin.airflow3.datahub_listener.extract_urns_from_task_instance_outlet_events",
                return_value=resolved,
            ) as mock_in_proc,
            patch(
                "datahub_airflow_plugin.airflow3.datahub_listener.extract_urns_from_resolved_alias_events"
            ) as mock_db,
        ):
            urns = _call_get_outlet_urns(stub, outlets, complete=True)

        assert urns == resolved
        mock_in_proc.assert_called_once()
        # DB fallback must NOT run when in-process resolution succeeded —
        # avoids querying the Airflow DB on the hot path.
        mock_db.assert_not_called()

    def test_db_fallback_used_when_in_process_returns_empty(self) -> None:
        stub = _make_listener_stub(capture_airflow_assets=True)
        outlets = [AssetAlias("parsed_data")]
        resolved = ["urn:li:dataset:(urn:li:dataPlatform:s3,bucket/fromdb,PROD)"]
        fake_session = MagicMock(name="airflow_session")
        task_instance = MagicMock()
        task_instance.map_index = -1

        with (
            patch(
                "datahub_airflow_plugin.airflow3.datahub_listener.extract_urns_from_task_instance_outlet_events",
                return_value=[],
            ),
            patch(
                "datahub_airflow_plugin.airflow3.datahub_listener.extract_urns_from_resolved_alias_events",
                return_value=resolved,
            ) as mock_db,
        ):
            urns = _call_get_outlet_urns(
                stub,
                outlets,
                complete=True,
                task_instance=task_instance,
                session=fake_session,
            )

        assert urns == resolved
        # Caller-provided session must be forwarded — create_session() is
        # blocked inside Airflow 3.0 listener threads, so without forwarding
        # the DB query path would always fail.
        mock_db.assert_called_once()
        call_kwargs = mock_db.call_args.kwargs
        assert call_kwargs["session"] is fake_session
        assert call_kwargs["dag_id"] == "my_dag"
        assert call_kwargs["task_id"] == "my_task"
        assert call_kwargs["run_id"] == "run_1"

    def test_no_alias_outlets_skips_resolution_entirely(self) -> None:
        stub = _make_listener_stub(capture_airflow_assets=True)
        outlets = [Asset("s3://bucket/static")]

        with (
            patch(
                "datahub_airflow_plugin.airflow3.datahub_listener.extract_urns_from_task_instance_outlet_events"
            ) as mock_in_proc,
            patch(
                "datahub_airflow_plugin.airflow3.datahub_listener.extract_urns_from_resolved_alias_events"
            ) as mock_db,
        ):
            urns = _call_get_outlet_urns(stub, outlets, complete=True)

        assert urns == ["urn:li:dataset:(urn:li:dataPlatform:s3,bucket/static,PROD)"]
        mock_in_proc.assert_not_called()
        mock_db.assert_not_called()


@pytest.fixture
def reset_patch_flag() -> Iterator[None]:
    """Reset the module-level _RUNTIME_TI_PATCHED guard around each test.

    _patch_runtime_ti_for_outlet_events() flips this global to True on first
    install; without a reset, test ordering would make later tests see it as
    already-patched and skip the work under test.
    """
    original = listener_mod._RUNTIME_TI_PATCHED
    listener_mod._RUNTIME_TI_PATCHED = False
    try:
        yield
    finally:
        listener_mod._RUNTIME_TI_PATCHED = original


def _inject_task_runner_module(**attrs: Any) -> Any:
    """patch.dict for the lazily-imported airflow.sdk task_runner module.

    _patch_runtime_ti_for_outlet_events / on_starting do
    ``from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance |
    TaskRunnerMarker``.  Injecting a stand-in module (same technique as
    test_airflow_asset_adapter.py) keeps the tests hermetic across Airflow 2.x
    and 3.x.
    """
    module = ModuleType("airflow.sdk.execution_time.task_runner")
    for name, value in attrs.items():
        setattr(module, name, value)
    return patch.dict(
        sys.modules,
        {"airflow.sdk.execution_time.task_runner": module},
    )


class TestPatchRuntimeTiForOutletEvents:
    """Tests for the RuntimeTaskInstance.get_template_context monkeypatch."""

    def test_patch_caches_outlet_events_on_instance(
        self, reset_patch_flag: None
    ) -> None:
        """The installed patch must store the live outlet_events reference on ti.

        Closes the loop between the patch (the writer) and
        extract_urns_from_task_instance_outlet_events (the reader), which reads
        ``ti._datahub_outlet_events``.
        """
        sentinel = object()

        class FakeRuntimeTaskInstance:
            def get_template_context(self) -> dict:
                return {"outlet_events": sentinel}

        with _inject_task_runner_module(RuntimeTaskInstance=FakeRuntimeTaskInstance):
            listener_mod._patch_runtime_ti_for_outlet_events()

            ti = FakeRuntimeTaskInstance()
            context = ti.get_template_context()

        assert listener_mod._RUNTIME_TI_PATCHED is True
        # Original behavior preserved: the context dict is still returned intact.
        assert context["outlet_events"] is sentinel
        # And the mutable accessor reference is cached on the instance.
        assert ti._datahub_outlet_events is sentinel  # type: ignore[attr-defined]

    def test_patch_is_idempotent(self, reset_patch_flag: None) -> None:
        """A second call must NOT re-wrap get_template_context."""

        class FakeRuntimeTaskInstance:
            def get_template_context(self) -> dict:
                return {}

        with _inject_task_runner_module(RuntimeTaskInstance=FakeRuntimeTaskInstance):
            listener_mod._patch_runtime_ti_for_outlet_events()
            after_first = FakeRuntimeTaskInstance.get_template_context
            listener_mod._patch_runtime_ti_for_outlet_events()
            after_second = FakeRuntimeTaskInstance.get_template_context

        assert after_first is after_second

    def test_setattr_failure_does_not_break_task(self, reset_patch_flag: None) -> None:
        """If caching the reference raises, the task must still get its context.

        Guards failure-mode #2 (a future Airflow giving RuntimeTaskInstance
        __slots__ would make object.__setattr__ raise). Lineage capture must
        never crash the user's task.
        """

        class SlottedRuntimeTaskInstance:
            __slots__ = ()  # blocks attribute assignment → object.__setattr__ raises

            def get_template_context(self) -> dict:
                return {"outlet_events": object()}

        with _inject_task_runner_module(RuntimeTaskInstance=SlottedRuntimeTaskInstance):
            listener_mod._patch_runtime_ti_for_outlet_events()
            ti = SlottedRuntimeTaskInstance()
            # Must not raise even though the cache write fails internally.
            context = ti.get_template_context()

        assert "outlet_events" in context

    def test_import_error_is_noop(self, reset_patch_flag: None) -> None:
        """When RuntimeTaskInstance is unavailable, the flag stays False."""
        with patch.dict(sys.modules, {"airflow.sdk.execution_time.task_runner": None}):
            listener_mod._patch_runtime_ti_for_outlet_events()
        assert listener_mod._RUNTIME_TI_PATCHED is False


class TestOnStarting:
    """Tests for the on_starting hook that installs the patch in task workers."""

    def _make_listener_stub(self, capture_airflow_assets: bool = True) -> Any:
        config = SimpleNamespace(capture_airflow_assets=capture_airflow_assets)
        return SimpleNamespace(config=config)

    def test_no_patch_when_capture_disabled(self) -> None:
        stub = self._make_listener_stub(capture_airflow_assets=False)
        with patch.object(
            listener_mod, "_patch_runtime_ti_for_outlet_events"
        ) as mock_patch:
            DataHubListener.on_starting(stub, MagicMock())
        mock_patch.assert_not_called()

    def test_patch_installed_for_task_runner_marker(self) -> None:
        class TaskRunnerMarker:
            pass

        stub = self._make_listener_stub(capture_airflow_assets=True)
        with (
            _inject_task_runner_module(TaskRunnerMarker=TaskRunnerMarker),
            patch.object(
                listener_mod, "_patch_runtime_ti_for_outlet_events"
            ) as mock_patch,
        ):
            DataHubListener.on_starting(stub, TaskRunnerMarker())
        mock_patch.assert_called_once()

    def test_no_patch_for_non_task_runner_component(self) -> None:
        """The scheduler / other components must NOT get the patch."""

        class TaskRunnerMarker:
            pass

        stub = self._make_listener_stub(capture_airflow_assets=True)
        with (
            _inject_task_runner_module(TaskRunnerMarker=TaskRunnerMarker),
            patch.object(
                listener_mod, "_patch_runtime_ti_for_outlet_events"
            ) as mock_patch,
        ):
            DataHubListener.on_starting(stub, SimpleNamespace())  # not a marker
        mock_patch.assert_not_called()

    def test_import_error_swallowed(self) -> None:
        stub = self._make_listener_stub(capture_airflow_assets=True)
        with patch.dict(sys.modules, {"airflow.sdk.execution_time.task_runner": None}):
            # Must not raise (Airflow <3.x: TaskRunnerMarker absent).
            DataHubListener.on_starting(stub, MagicMock())

    def test_patch_exception_swallowed(self) -> None:
        """A non-ImportError failure during install must not break startup."""

        class TaskRunnerMarker:
            pass

        stub = self._make_listener_stub(capture_airflow_assets=True)
        with (
            _inject_task_runner_module(TaskRunnerMarker=TaskRunnerMarker),
            patch.object(
                listener_mod,
                "_patch_runtime_ti_for_outlet_events",
                side_effect=RuntimeError("boom"),
            ),
        ):
            # Must not raise — on_starting runs synchronously on Airflow startup.
            DataHubListener.on_starting(stub, TaskRunnerMarker())


UPSTREAM_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_schema.src,PROD)"
)
DOWNSTREAM_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_schema.dst,PROD)"
)
SQL_STATEMENT = "SELECT col_a FROM my_db.my_schema.src"


def _make_query_entity_stub(capture_query_entities: bool) -> Any:
    config = SimpleNamespace(capture_query_entities=capture_query_entities)
    # _emit_query_entity calls self._make_emit_callback(); the real method
    # doesn't touch self, so a no-op stand-in is sufficient here.
    return SimpleNamespace(config=config, _make_emit_callback=lambda: None)


def _make_operator_lineage_with_sql_facet() -> Any:
    """A minimal OpenLineage OperatorLineage stand-in carrying both facets
    _sql_parser_patch.py sets: the DataHub SqlParsingResult in run_facets, and
    the raw SQL text in the job_facets "sql" SqlJobFacet.

    query_fingerprint is deliberately set to something other than the real SQL
    text: SqlParsingResult has no text field, only this fingerprint, and using
    it instead of the job facet would derive a different (wrong) Query URN.
    """
    sql_parsing_result = SqlParsingResult(
        in_tables=[UPSTREAM_DATASET_URN],
        out_tables=[DOWNSTREAM_DATASET_URN],
        column_lineage=[
            ColumnLineageInfo(
                downstream=DownstreamColumnRef(
                    table=DOWNSTREAM_DATASET_URN, column="col_a"
                ),
                upstreams=[ColumnRef(table=UPSTREAM_DATASET_URN, column="col_a")],
            )
        ],
        query_fingerprint="not-the-real-sql-fingerprint",
    )
    return SimpleNamespace(
        inputs=[],
        outputs=[],
        run_facets={listener_mod.DATAHUB_SQL_PARSING_RESULT_KEY: sql_parsing_result},
        job_facets={"sql": SqlJobFacet(query=SQL_STATEMENT)},
    )


def _extract_lineage_from_airflow3(stub: Any) -> Any:
    task = MagicMock()
    task.task_id = "my_task"
    task.get_openlineage_facets_on_complete = MagicMock(
        return_value=_make_operator_lineage_with_sql_facet()
    )
    return DataHubListener._extract_lineage_from_airflow3(
        stub, task, MagicMock(), complete=True
    )


class TestQueryEntityEmission:
    """capture_query_entities: the SQL statement text must come from the
    OpenLineage SQL job facet (the only place it exists - SqlParsingResult
    carries a fingerprint but no text), become the Query URN via
    sha256(statement text), and drive both the FGL.query link and the emitted
    Query entity aspects.
    """

    def test_query_entity_emitted_and_linked_when_enabled(self) -> None:
        stub = _make_query_entity_stub(capture_query_entities=True)

        _, _, sql_parsing_result, statement_text = _extract_lineage_from_airflow3(stub)
        assert statement_text == SQL_STATEMENT

        datajob = MagicMock()
        datajob.urn = "urn:li:dataJob:(urn:li:dataFlow:(airflow,my_dag,PROD),my_task)"
        _, _, fine_grained_lineages, subject_urns = (
            DataHubListener._process_sql_parsing_result(
                stub, datajob, sql_parsing_result, statement_text
            )
        )

        expected_query_urn = QueryUrn(generate_hash(SQL_STATEMENT)).urn()
        assert fine_grained_lineages
        assert all(fgl.query == expected_query_urn for fgl in fine_grained_lineages)

        mock_emitter = MagicMock()
        DataHubListener._emit_query_entity(
            stub, mock_emitter, statement_text, subject_urns
        )

        emitted_mcps = [call.args[0] for call in mock_emitter.emit.call_args_list]
        assert emitted_mcps
        assert all(mcp.entityUrn == expected_query_urn for mcp in emitted_mcps)

        query_properties = next(
            mcp.aspect
            for mcp in emitted_mcps
            if isinstance(mcp.aspect, QueryPropertiesClass)
        )
        assert query_properties.statement.value == SQL_STATEMENT

        query_subjects = next(
            mcp.aspect
            for mcp in emitted_mcps
            if isinstance(mcp.aspect, QuerySubjectsClass)
        )
        assert query_subjects.subjects

    def test_no_query_entity_when_flag_disabled(self) -> None:
        stub = _make_query_entity_stub(capture_query_entities=False)

        _, _, sql_parsing_result, statement_text = _extract_lineage_from_airflow3(stub)

        datajob = MagicMock()
        datajob.urn = "urn:li:dataJob:(urn:li:dataFlow:(airflow,my_dag,PROD),my_task)"
        _, _, fine_grained_lineages, subject_urns = (
            DataHubListener._process_sql_parsing_result(
                stub, datajob, sql_parsing_result, statement_text
            )
        )
        assert fine_grained_lineages
        assert all(fgl.query is None for fgl in fine_grained_lineages)

        mock_emitter = MagicMock()
        DataHubListener._emit_query_entity(
            stub, mock_emitter, statement_text, subject_urns
        )
        mock_emitter.emit.assert_not_called()
