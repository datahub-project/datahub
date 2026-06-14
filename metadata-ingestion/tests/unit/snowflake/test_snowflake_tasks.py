from datetime import datetime
from typing import List, Optional
from unittest.mock import MagicMock, patch

from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeTask,
    SnowflakeTaskState,
)
from datahub.ingestion.source.snowflake.snowflake_tasks import (
    SnowflakeTasksExtractor,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    DataJobInfoClass,
    DataJobInputOutputClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    OwnershipClass,
    SubTypesClass,
)
from datahub.sql_parsing.schema_resolver import SchemaResolver, SchemaResolverInterface
from datahub.sql_parsing.sqlglot_lineage import SqlParsingDebugInfo, SqlParsingResult


def _make_config() -> SnowflakeV2Config:
    return SnowflakeV2Config(
        account_id="test_account",
        username="user",
        password="pass",  # type: ignore
        include_tasks=True,
    )


def _make_task(
    name: str = "etl_task",
    definition: str = "",
    predecessors: Optional[List[str]] = None,
    state: SnowflakeTaskState = SnowflakeTaskState.STARTED,
    schedule: str = "USING CRON 0 * * * * UTC",
    warehouse: str = "COMPUTE_WH",
) -> SnowflakeTask:
    return SnowflakeTask(
        name=name,
        created=datetime(2024, 1, 1),
        owner="ADMIN",
        database_name="TEST_DB",
        schema_name="PUBLIC",
        definition=definition,
        state=state,
        owner_role_type="ROLE",
        comment=f"Task {name}",
        warehouse=warehouse,
        schedule=schedule,
        predecessors=list(predecessors) if predecessors else [],
    )


def _make_schema_resolver(config: SnowflakeV2Config) -> SchemaResolverInterface:
    return SchemaResolver(
        platform="snowflake",
        platform_instance=config.platform_instance,
        env=config.env,
        graph=None,
    )


def _data_job_input_outputs(wus: List) -> List[DataJobInputOutputClass]:
    return [
        wu.metadata.aspect
        for wu in wus
        if hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, DataJobInputOutputClass)
    ]


def _collect_workunits(
    tasks: List[SnowflakeTask],
    config: Optional[SnowflakeV2Config] = None,
) -> tuple:
    if config is None:
        config = _make_config()
    report = SnowflakeV2Report()
    identifiers = SnowflakeIdentifierBuilder(
        identifier_config=config, structured_reporter=report
    )
    data_dict = MagicMock()
    data_dict.get_tasks_for_schema.return_value = tasks

    extractor = SnowflakeTasksExtractor(
        config=config,
        report=report,
        data_dictionary=data_dict,
        identifiers=identifiers,
        schema_resolver=_make_schema_resolver(config),
    )
    wus = list(extractor.get_workunits("TEST_DB", "PUBLIC"))
    return wus, report


class TestSnowflakeTasksExtractor:
    def test_no_tasks_emits_nothing(self) -> None:
        wus, report = _collect_workunits([])
        assert len(wus) == 0
        assert report.tasks_scanned == 0

    def test_single_task_emits_flow_and_job(self) -> None:
        task = _make_task()
        wus, report = _collect_workunits([task])

        assert report.tasks_scanned == 1
        assert len(wus) >= 5  # DataFlow(3) + DataJob(at least 3-4)

        # Verify subtypes
        subtype_values = []
        for wu in wus:
            mcp = wu.metadata
            if hasattr(mcp, "aspect") and isinstance(mcp.aspect, SubTypesClass):
                subtype_values.extend(mcp.aspect.typeNames)
        assert "Snowflake Task Group" in subtype_values
        assert "Snowflake Task" in subtype_values

    def test_task_custom_properties(self) -> None:
        task = _make_task(
            schedule="USING CRON 0 * * * * UTC",
            warehouse="MY_WH",
            state=SnowflakeTaskState.STARTED,
        )
        wus, _ = _collect_workunits([task])

        job_infos = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, DataJobInfoClass)
        ]
        assert len(job_infos) == 1
        props = job_infos[0].customProperties
        assert props["state"] == "STARTED"
        assert props["warehouse"] == "MY_WH"
        assert props["schedule"] == "USING CRON 0 * * * * UTC"

    def test_predecessor_dag(self) -> None:
        task_a = _make_task(name="task_a")
        task_b = _make_task(name="task_b", predecessors=["task_a"])
        wus, report = _collect_workunits([task_a, task_b])

        assert report.tasks_scanned == 2

        # Find DataJobInputOutput for task_b
        input_outputs = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, DataJobInputOutputClass)
        ]
        # Only task_b has predecessors, so only 1 DataJobInputOutput
        assert len(input_outputs) == 1
        io = input_outputs[0]
        assert io.inputDatajobs is not None
        assert len(io.inputDatajobs) == 1
        assert "task_a" in io.inputDatajobs[0]

    def test_predecessor_fully_qualified_name(self) -> None:
        """Predecessors can be fully qualified like DB.SCHEMA.TASK_NAME."""
        task_a = _make_task(name="task_a")
        task_b = _make_task(name="task_b", predecessors=["TEST_DB.PUBLIC.task_a"])
        wus, _ = _collect_workunits([task_a, task_b])

        input_outputs = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, DataJobInputOutputClass)
        ]
        assert len(input_outputs) == 1
        assert input_outputs[0].inputDatajobs is not None
        assert len(input_outputs[0].inputDatajobs) == 1
        assert "task_a" in input_outputs[0].inputDatajobs[0]

    def test_predecessor_not_in_schema_emits_warning(self) -> None:
        """Predecessor referencing a task not in the current schema is skipped
        with a warning so users can see why input lineage is incomplete."""
        task = _make_task(
            name="task_b", predecessors=["other_db.other_schema.upstream_task"]
        )
        wus, report = _collect_workunits([task])

        assert report.tasks_scanned == 1
        # No DataJobInputOutput emitted since predecessor not found
        input_outputs = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, DataJobInputOutputClass)
        ]
        assert len(input_outputs) == 0

        messages = [w.message for w in report.warnings]
        assert any("Predecessor" in m for m in messages), (
            f"Expected a predecessor warning; got: {messages}"
        )
        contexts = [str(w.context) for w in report.warnings]
        assert any("upstream_task" in c for c in contexts)
        assert any("test_db.public.task_b" in c.lower() for c in contexts)

    def test_ownership_emitted(self) -> None:
        task = _make_task()
        wus, _ = _collect_workunits([task])

        ownerships = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, OwnershipClass)
        ]
        assert len(ownerships) == 1
        assert "ADMIN" in ownerships[0].owners[0].owner

    def test_all_tasks_filtered_emits_nothing(self) -> None:
        config = _make_config()
        config.task_pattern.deny = [".*"]
        wus, report = _collect_workunits([_make_task()], config=config)
        assert len(wus) == 0
        assert report.tasks_scanned == 0

    def test_task_name_map_includes_all_tasks_for_predecessor_resolution(self) -> None:
        """Even if task_a is filtered out by pattern, it should still be in task_name_map
        for predecessor resolution of task_b. Currently we filter then iterate allowed_tasks,
        but build task_name_map from all tasks."""
        config = _make_config()
        config.task_pattern.deny = [".*TASK_A.*"]
        report = SnowflakeV2Report()
        identifiers = SnowflakeIdentifierBuilder(
            identifier_config=config, structured_reporter=report
        )
        task_a = _make_task(name="task_a")
        task_b = _make_task(name="task_b", predecessors=["task_a"])

        data_dict = MagicMock()
        data_dict.get_tasks_for_schema.return_value = [task_a, task_b]

        extractor = SnowflakeTasksExtractor(
            config=config,
            report=report,
            data_dictionary=data_dict,
            identifiers=identifiers,
            schema_resolver=_make_schema_resolver(config),
        )
        wus = list(extractor.get_workunits("TEST_DB", "PUBLIC"))

        # Only task_b should be scanned
        assert report.tasks_scanned == 1

        # task_b should still have task_a as predecessor (task_name_map built from ALL tasks)
        input_outputs = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, DataJobInputOutputClass)
        ]
        assert len(input_outputs) == 1
        assert input_outputs[0].inputDatajobs is not None
        assert "task_a" in input_outputs[0].inputDatajobs[0]

    def test_task_with_insert_select_emits_dataset_lineage(self) -> None:
        task = _make_task(
            name="etl_task",
            definition="INSERT INTO target_tbl(col_a, col_b) "
            "SELECT col_a, col_b FROM source_tbl",
        )
        wus, _ = _collect_workunits([task])

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 1
        io = ios[0]
        assert io.inputDatasets is not None and len(io.inputDatasets) == 1
        assert io.outputDatasets is not None and len(io.outputDatasets) == 1
        assert "source_tbl" in io.inputDatasets[0]
        assert "target_tbl" in io.outputDatasets[0]
        # Default-qualified to the task's database/schema.
        assert "test_db.public" in io.inputDatasets[0]
        assert "test_db.public" in io.outputDatasets[0]

    def test_task_with_merge_emits_dataset_lineage(self) -> None:
        task = _make_task(
            name="merge_task",
            definition=(
                "MERGE INTO target_tbl t USING source_tbl s "
                "ON t.id = s.id "
                "WHEN MATCHED THEN UPDATE SET t.name = s.name "
                "WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)"
            ),
        )
        wus, _ = _collect_workunits([task])

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 1
        io = ios[0]
        assert io.inputDatasets and "source_tbl" in io.inputDatasets[0]
        assert io.outputDatasets and "target_tbl" in io.outputDatasets[0]

    def test_task_with_create_table_as_emits_dataset_lineage(self) -> None:
        task = _make_task(
            name="ctas_task",
            definition=("CREATE OR REPLACE TABLE out_tbl AS SELECT a FROM in_tbl"),
        )
        wus, _ = _collect_workunits([task])

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 1
        io = ios[0]
        assert io.inputDatasets and "in_tbl" in io.inputDatasets[0]
        assert io.outputDatasets and "out_tbl" in io.outputDatasets[0]

    def test_task_emits_column_level_fine_grained_lineages(self) -> None:
        """Each output column maps to its upstream column via FineGrainedLineage."""
        task = _make_task(
            name="cll_task",
            definition=(
                "INSERT INTO target_tbl(col_a, col_b) "
                "SELECT col_a, col_b FROM source_tbl"
            ),
        )
        wus, _ = _collect_workunits([task])

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 1
        fgs = ios[0].fineGrainedLineages
        assert fgs is not None and len(fgs) == 2

        downstream_columns = set()
        for fg in fgs:
            assert fg.downstreamType == FineGrainedLineageDownstreamTypeClass.FIELD
            assert fg.upstreamType == FineGrainedLineageUpstreamTypeClass.FIELD_SET
            downstreams = fg.downstreams or []
            upstreams = fg.upstreams or []
            assert len(downstreams) == 1
            assert "target_tbl" in downstreams[0]
            assert upstreams and all("source_tbl" in u for u in upstreams)
            downstream_columns.add(downstreams[0].rsplit(",", 1)[-1].rstrip(")"))

        assert downstream_columns == {"col_a", "col_b"}

    def test_task_lineage_combined_with_predecessor(self) -> None:
        """A task with both predecessors and a parseable SQL body should emit
        all three of inputDatajobs / inputDatasets / outputDatasets."""
        task_a = _make_task(name="task_a")
        task_b = _make_task(
            name="task_b",
            predecessors=["task_a"],
            definition="INSERT INTO out_tbl SELECT * FROM in_tbl",
        )
        wus, _ = _collect_workunits([task_a, task_b])

        ios = _data_job_input_outputs(wus)
        # task_b emits one DataJobInputOutput with all three populated.
        # task_a has no SQL and no predecessors → no DataJobInputOutput.
        assert len(ios) == 1
        io = ios[0]
        assert io.inputDatajobs and "task_a" in io.inputDatajobs[0]
        assert io.inputDatasets and "in_tbl" in io.inputDatasets[0]
        assert io.outputDatasets and "out_tbl" in io.outputDatasets[0]

    def test_per_task_exception_does_not_halt_remaining_tasks(self) -> None:
        """If one task raises inside _gen_data_job, a warning is emitted but
        the remaining tasks in the schema are still processed."""
        task_a = _make_task(name="bad_task")
        task_b = _make_task(
            name="good_task",
            definition="INSERT INTO out_tbl SELECT a FROM in_tbl",
        )

        original_gen_data_job = SnowflakeTasksExtractor._gen_data_job

        def _raise_on_bad(self_inner, task, **kwargs):  # type: ignore[misc]
            if task.name == "bad_task":
                raise RuntimeError("simulated extraction failure")
            return original_gen_data_job(self_inner, task, **kwargs)

        with patch.object(SnowflakeTasksExtractor, "_gen_data_job", _raise_on_bad):
            wus, report = _collect_workunits([task_a, task_b])

        assert report.tasks_scanned == 2

        messages = [w.message for w in report.warnings]
        assert any("Failed to extract metadata for task" in m for m in messages), (
            f"Expected a task-extraction warning; got: {messages}"
        )
        contexts = [str(w.context) for w in report.warnings]
        assert any("bad_task" in c for c in contexts)

        # good_task must still produce dataset lineage
        ios = _data_job_input_outputs(wus)
        assert any(
            "out_tbl" in (u or "") for io in ios for u in (io.outputDatasets or [])
        )

    def test_partial_column_lineage_dropped_emits_warning_and_keeps_valid_entries(
        self,
    ) -> None:
        """One CLL entry has no downstream table (dropped), one is valid.
        The partial-drop warning is emitted and the valid FineGrainedLineage
        is still present in the aspect."""
        task = _make_task(
            name="partial_drop_task",
            definition="INSERT INTO t(a, b) SELECT a, b FROM s",
        )

        mock_bad = MagicMock()
        mock_bad.downstream.table = None
        mock_bad.downstream.column = "a"

        upstream_ref = MagicMock()
        upstream_ref.table = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.s,PROD)"
        )
        upstream_ref.column = "b"
        mock_good = MagicMock()
        mock_good.downstream.table = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.t,PROD)"
        )
        mock_good.downstream.column = "b"
        mock_good.upstreams = [upstream_ref]

        mock_result = MagicMock(spec=SqlParsingResult)
        mock_result.debug_info = SqlParsingDebugInfo()
        mock_result.in_tables = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.s,PROD)"
        ]
        mock_result.out_tables = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.t,PROD)"
        ]
        mock_result.column_lineage = [mock_bad, mock_good]

        with patch(
            "datahub.ingestion.source.snowflake.snowflake_tasks.sqlglot_lineage",
            return_value=mock_result,
        ):
            wus, report = _collect_workunits([task])

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 1
        assert ios[0].fineGrainedLineages is not None
        assert len(ios[0].fineGrainedLineages) == 1  # bad entry dropped, good kept

        messages = [w.message for w in report.warnings]
        assert any("dropped" in m.lower() for m in messages), (
            f"Expected a partial-drop warning; got: {messages}"
        )
        contexts = [str(w.context) for w in report.warnings]
        assert any("partial_drop_task" in c for c in contexts)
        assert any("1/2" in c for c in contexts)

    def test_sqlglot_lineage_exception_emits_sql_parsing_warning(self) -> None:
        """When sqlglot_lineage raises (e.g. unexpected parser crash), the
        'Task SQL Parsing Failed' warning is emitted and no dataset lineage is
        produced — distinct from the table_error path."""
        task = _make_task(
            name="crash_task",
            definition="INSERT INTO t SELECT a FROM s",
        )
        with patch(
            "datahub.ingestion.source.snowflake.snowflake_tasks.sqlglot_lineage",
            side_effect=RuntimeError("unexpected parser crash"),
        ):
            wus, report = _collect_workunits([task])

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 0

        titles = [w.title for w in report.warnings]
        assert any("SQL Parsing Failed" in t for t in titles), (
            f"Expected 'Task SQL Parsing Failed' title; got: {titles}"
        )
        contexts = [str(w.context) for w in report.warnings]
        assert any("crash_task" in c for c in contexts)

    def test_empty_upstream_fields_drops_entry_and_emits_warning(self) -> None:
        """When downstream is resolved but all upstream refs lack table/column,
        the entry is dropped and a partial-drop warning is emitted."""
        task = _make_task(
            name="no_upstream_task",
            definition="INSERT INTO t SELECT a FROM s",
        )

        unresolvable_ref = MagicMock()
        unresolvable_ref.table = None  # upstream column reference can't be resolved
        unresolvable_ref.column = None

        mock_cll = MagicMock()
        mock_cll.downstream.table = (
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.t,PROD)"
        )
        mock_cll.downstream.column = "a"
        mock_cll.upstreams = [unresolvable_ref]

        mock_result = MagicMock(spec=SqlParsingResult)
        mock_result.debug_info = SqlParsingDebugInfo()
        mock_result.in_tables = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.s,PROD)"
        ]
        mock_result.out_tables = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.t,PROD)"
        ]
        mock_result.column_lineage = [mock_cll]

        with patch(
            "datahub.ingestion.source.snowflake.snowflake_tasks.sqlglot_lineage",
            return_value=mock_result,
        ):
            wus, report = _collect_workunits([task])

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 1
        # Dataset lineage still emitted even though column lineage was dropped.
        assert ios[0].inputDatasets and "s" in ios[0].inputDatasets[0]
        assert ios[0].outputDatasets and "t" in ios[0].outputDatasets[0]
        assert ios[0].fineGrainedLineages is None

        messages = [w.message for w in report.warnings]
        assert any("dropped" in m.lower() for m in messages), (
            f"Expected a dropped-entry warning; got: {messages}"
        )
        contexts = [str(w.context) for w in report.warnings]
        assert any("no_upstream_task" in c for c in contexts)
        assert any("1/1" in c for c in contexts)

    def test_unparseable_task_definition_logs_warning_and_emits_no_dataset_lineage(
        self,
    ) -> None:
        """CALL is not supported by sqlglot's lineage engine."""
        task = _make_task(
            name="proc_task",
            definition="CALL my_proc('arg1', 'arg2')",
        )
        wus, report = _collect_workunits([task])

        ios = _data_job_input_outputs(wus)
        # No predecessors, no parsed datasets → no DataJobInputOutput.
        assert len(ios) == 0
        messages = [w.message for w in report.warnings]
        assert any("task definition" in m.lower() for m in messages), (
            f"Expected a task-definition warning; got: {messages}"
        )
        # The proc_task FQN should appear in the warning context, so users can
        # identify which task triggered it.
        contexts = [str(w.context) for w in report.warnings]
        assert any("proc_task" in c for c in contexts)

    def test_empty_definition_emits_no_dataset_lineage(self) -> None:
        task = _make_task(name="empty_task", definition="")
        wus, _ = _collect_workunits([task])

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 0

    def test_table_error_in_sql_parsing_result_emits_warning(self) -> None:
        """When sqlglot returns a table_error, a warning is emitted and no
        dataset lineage is produced (table lineage extraction failed)."""
        task = _make_task(
            name="bad_table_task",
            definition="INSERT INTO target_tbl SELECT * FROM source_tbl",
        )
        table_exc = ValueError("ambiguous table reference")
        mock_result = MagicMock(spec=SqlParsingResult)
        mock_result.debug_info = SqlParsingDebugInfo(table_error=table_exc)
        mock_result.in_tables = []
        mock_result.out_tables = []
        mock_result.column_lineage = []

        with patch(
            "datahub.ingestion.source.snowflake.snowflake_tasks.sqlglot_lineage",
            return_value=mock_result,
        ):
            wus, report = _collect_workunits([task])

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 0

        messages = [w.message for w in report.warnings]
        assert any("table lineage" in m.lower() for m in messages), (
            f"Expected a table-lineage warning; got: {messages}"
        )
        contexts = [str(w.context) for w in report.warnings]
        assert any("bad_table_task" in c for c in contexts)

    def test_column_error_in_sql_parsing_result_emits_warning_but_keeps_dataset_lineage(
        self,
    ) -> None:
        """When sqlglot sets column_error, a warning is emitted for column lineage
        failure but dataset-level lineage (inputDatasets/outputDatasets) is still
        emitted."""
        task = _make_task(
            name="col_error_task",
            definition="INSERT INTO target_tbl SELECT a FROM source_tbl",
        )
        col_exc = ValueError("column resolution failed")
        mock_result = MagicMock(spec=SqlParsingResult)
        mock_result.debug_info = SqlParsingDebugInfo(column_error=col_exc)
        mock_result.in_tables = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.source_tbl,PROD)"
        ]
        mock_result.out_tables = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.target_tbl,PROD)"
        ]
        mock_result.column_lineage = []

        with patch(
            "datahub.ingestion.source.snowflake.snowflake_tasks.sqlglot_lineage",
            return_value=mock_result,
        ):
            wus, report = _collect_workunits([task])

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 1
        assert ios[0].inputDatasets and "source_tbl" in ios[0].inputDatasets[0]
        assert ios[0].outputDatasets and "target_tbl" in ios[0].outputDatasets[0]
        assert ios[0].fineGrainedLineages is None

        messages = [w.message for w in report.warnings]
        assert any("column lineage" in m.lower() for m in messages), (
            f"Expected a column-lineage warning; got: {messages}"
        )
        contexts = [str(w.context) for w in report.warnings]
        assert any("col_error_task" in c for c in contexts)

    def test_all_column_lineage_entries_dropped_emits_warning(self) -> None:
        """When every column-lineage entry has no downstream table/column,
        a warning is emitted and fineGrainedLineages is absent from the aspect."""
        task = _make_task(
            name="dropped_cll_task",
            definition="INSERT INTO target_tbl SELECT a FROM source_tbl",
        )

        # Build a mock ColumnLineageInfo whose downstream has no table.
        mock_cll = MagicMock()
        mock_cll.downstream.table = None
        mock_cll.downstream.column = "a"

        mock_result = MagicMock(spec=SqlParsingResult)
        mock_result.debug_info = SqlParsingDebugInfo()
        mock_result.in_tables = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.source_tbl,PROD)"
        ]
        mock_result.out_tables = [
            "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.public.target_tbl,PROD)"
        ]
        mock_result.column_lineage = [mock_cll]

        with patch(
            "datahub.ingestion.source.snowflake.snowflake_tasks.sqlglot_lineage",
            return_value=mock_result,
        ):
            wus, report = _collect_workunits([task])

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 1
        assert ios[0].fineGrainedLineages is None

        messages = [w.message for w in report.warnings]
        assert any(
            "column" in m.lower() and "dropped" in m.lower() for m in messages
        ), f"Expected an all-dropped column-lineage warning; got: {messages}"
        contexts = [str(w.context) for w in report.warnings]
        assert any("dropped_cll_task" in c for c in contexts)

    def test_multiple_tasks_same_flow(self) -> None:
        tasks = [_make_task(name=f"task_{i}") for i in range(3)]
        wus, report = _collect_workunits(tasks)

        assert report.tasks_scanned == 3

        # Only 1 DataFlow but 3 DataJobs
        flow_subtypes = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, SubTypesClass)
            and "Snowflake Task Group" in wu.metadata.aspect.typeNames
        ]
        job_subtypes = [
            wu
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, SubTypesClass)
            and "Snowflake Task" in wu.metadata.aspect.typeNames
        ]
        assert len(flow_subtypes) == 1
        assert len(job_subtypes) == 3
