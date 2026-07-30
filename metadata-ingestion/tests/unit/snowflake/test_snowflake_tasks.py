from datetime import datetime
from typing import Callable, List, Optional
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
from datahub.sql_parsing.schema_resolver import SchemaResolver


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


def _make_schema_resolver(config: SnowflakeV2Config) -> SchemaResolver:
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


def _job_urn(wus: List, task_name: str) -> str:
    """The urn the named task was actually emitted under.

    Asserting predecessor edges against this rather than a substring is what
    catches an edge pointing at a DataJob that doesn't exist.
    """
    urns = [
        wu.metadata.entityUrn
        for wu in wus
        if hasattr(wu.metadata, "aspect")
        and isinstance(wu.metadata.aspect, DataJobInfoClass)
        and wu.metadata.aspect.name == task_name
    ]
    assert len(urns) == 1, f"Expected one DataJobInfo for {task_name}; got {urns}"
    return urns[0]


def _collect_workunits(
    tasks: List[SnowflakeTask],
    config: Optional[SnowflakeV2Config] = None,
    is_temp_table: Optional[Callable[[str], bool]] = None,
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
        is_temp_table=is_temp_table or (lambda _: False),
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

    def test_predecessor_fully_qualified_cross_schema_name_collision_is_unresolved(
        self,
    ) -> None:
        """A fully-qualified predecessor pointing at a *different* db/schema must
        not false-match a same-named task in the current schema's task_name_map;
        it should be treated as unresolved instead."""
        task_a = _make_task(name="task_a")
        task_b = _make_task(
            name="task_b", predecessors=["OTHER_DB.OTHER_SCHEMA.task_a"]
        )
        wus, report = _collect_workunits([task_a, task_b])

        input_outputs = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, DataJobInputOutputClass)
        ]
        # No same-schema match should be made despite the leaf-name collision.
        assert len(input_outputs) == 0

        contexts = [str(w.context) for w in report.warnings]
        assert any("OTHER_DB.OTHER_SCHEMA.task_a" in c for c in contexts)

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

    def test_predecessor_partially_qualified_and_quoted_names_resolve(self) -> None:
        """Snowflake reports predecessors as TASK, SCHEMA.TASK or DB.SCHEMA.TASK,
        any part optionally quoted. All forms pointing at the current schema
        must resolve rather than falling through to an unresolved warning."""
        task_a = _make_task(name="task_a")
        task_b = _make_task(
            name="task_b",
            predecessors=["PUBLIC.task_a", '"TEST_DB"."PUBLIC"."task_a"'],
        )
        wus, report = _collect_workunits([task_a, task_b])

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 1
        # Both forms name the same task, so they collapse to one edge.
        assert ios[0].inputDatajobs == [_job_urn(wus, "task_a")]
        assert not report.warnings

    def test_predecessor_listed_twice_emits_single_edge(self) -> None:
        """The same predecessor referenced both bare and fully-qualified must not
        produce a duplicate inputDatajobs entry."""
        task_a = _make_task(name="task_a")
        task_b = _make_task(
            name="task_b", predecessors=["task_a", "TEST_DB.PUBLIC.task_a"]
        )
        wus, _ = _collect_workunits([task_a, task_b])

        ios = _data_job_input_outputs(wus)
        assert ios[0].inputDatajobs == [_job_urn(wus, "task_a")]

    def test_predecessor_urn_matches_emitted_task_urn_without_lowercasing(self) -> None:
        """With convert_urns_to_lowercase disabled the predecessor edge must be
        built from the task's own name, not the upper-cased predecessor literal,
        or it points at a DataJob that was never emitted."""
        config = _make_config()
        config.convert_urns_to_lowercase = False
        task_a = _make_task(name="task_a")
        task_b = _make_task(name="task_b", predecessors=["task_a"])
        wus, _ = _collect_workunits([task_a, task_b], config=config)

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 1
        assert ios[0].inputDatajobs == [_job_urn(wus, "task_a")]

    def test_temp_table_staging_collapses_through_to_real_target(self) -> None:
        """Shared-session temp table resolution is the reason task bodies are
        routed through parse_procedure_code, so the is_temp_table predicate has
        to actually reach it.

        Deliberately a plain ``CREATE TABLE``, not ``CREATE TEMPORARY TABLE``:
        the latter is self-identifying, so it would collapse even with the
        predicate unwired. Here the predicate is the only signal — matching
        production, where _is_temp_table flags a scratch table that the dataset
        patterns allow but that was never ingested.
        """
        task = _make_task(
            name="staged_task",
            definition=(
                "CREATE TABLE scratch_a AS SELECT id FROM src; "
                "INSERT INTO tgt SELECT id FROM scratch_a"
            ),
        )
        wus, _ = _collect_workunits(
            [task], is_temp_table=lambda name: "scratch_" in name
        )

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 1
        io = ios[0]
        datasets = (io.inputDatasets or []) + (io.outputDatasets or [])
        assert not any("scratch_a" in urn for urn in datasets), datasets
        assert any("src" in urn for urn in io.inputDatasets or [])
        assert any("tgt" in urn for urn in io.outputDatasets or [])

    def test_task_body_without_lineage_is_counted_not_warned(self) -> None:
        """COPY INTO is a canonical task body that carries no sqlglot-resolvable
        lineage. That is not a failure and must not warn on every run."""
        task = _make_task(
            name="load_task", definition="COPY INTO my_tbl FROM @my_stage"
        )
        wus, report = _collect_workunits([task])

        assert _data_job_input_outputs(wus) == []
        assert report.tasks_without_sql_lineage == 1
        assert not report.warnings

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
            is_temp_table=lambda _: False,
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

    def test_lineage_skipped_when_include_table_lineage_disabled(self) -> None:
        """No dataset or column lineage should be parsed from task SQL when
        include_table_lineage is off, regardless of include_column_lineage."""
        config = SnowflakeV2Config(
            account_id="test_account",
            username="user",
            password="pass",  # type: ignore
            include_tasks=True,
            include_table_lineage=False,
            include_column_lineage=False,
        )
        task = _make_task(
            name="etl_task",
            definition="INSERT INTO target_tbl(col_a) SELECT col_a FROM source_tbl",
        )
        wus, _ = _collect_workunits([task], config=config)

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 0

    def test_datajob_edge_survives_include_table_lineage_disabled(self) -> None:
        """include_table_lineage governs table-to-table lineage, so disabling it
        must strip the dataset halves without taking the job-to-job edges with
        them: predecessor edges have never been gated on it, and procedures run
        this same CALL path gated only on include_procedures."""
        config = SnowflakeV2Config(
            account_id="test_account",
            username="user",
            password="pass",  # type: ignore
            include_tasks=True,
            include_table_lineage=False,
            include_column_lineage=False,
        )
        task_a = _make_task(name="task_a")
        task_b = _make_task(
            name="task_b",
            predecessors=["task_a"],
            definition="INSERT INTO target_tbl(col_a) SELECT col_a FROM source_tbl",
        )
        wus, _ = _collect_workunits([task_a, task_b], config=config)

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 1
        io = ios[0]
        assert io.inputDatajobs == [_job_urn(wus, "task_a")]
        assert not io.inputDatasets
        assert not io.outputDatasets
        assert io.fineGrainedLineages is None

    def test_column_lineage_skipped_when_include_column_lineage_disabled(
        self,
    ) -> None:
        """Dataset-level lineage is still emitted when only column lineage is
        disabled, but no FineGrainedLineage entries are produced."""
        config = SnowflakeV2Config(
            account_id="test_account",
            username="user",
            password="pass",  # type: ignore
            include_tasks=True,
            include_table_lineage=True,
            include_column_lineage=False,
        )
        task = _make_task(
            name="etl_task",
            definition="INSERT INTO target_tbl(col_a) SELECT col_a FROM source_tbl",
        )
        wus, _ = _collect_workunits([task], config=config)

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 1
        assert ios[0].inputDatasets and "source_tbl" in ios[0].inputDatasets[0]
        assert ios[0].outputDatasets and "target_tbl" in ios[0].outputDatasets[0]
        assert ios[0].fineGrainedLineages is None

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

    def test_sql_parse_failure_still_emits_ownership(self) -> None:
        """The parse guard sits around the parse call, not the whole task, so a
        body that blows up must not take the other four aspects with it. Losing
        Ownership here would silently discard what a previous run wrote."""
        task = _make_task(name="etl_task", definition="INSERT INTO a SELECT 1")

        with patch(
            "datahub.ingestion.source.snowflake.snowflake_tasks.parse_procedure_code",
            side_effect=RuntimeError("simulated parse failure"),
        ):
            wus, report = _collect_workunits([task])

        assert report.tasks_failed == 1
        titles = [w.title for w in report.warnings]
        assert any("Task Lineage Extraction Failed" in (t or "") for t in titles), (
            f"Expected a lineage-parse warning; got: {titles}"
        )

        ownerships = [
            wu.metadata.aspect
            for wu in wus
            if hasattr(wu.metadata, "aspect")
            and isinstance(wu.metadata.aspect, OwnershipClass)
        ]
        assert len(ownerships) == 1
        # No lineage survived, but the task itself is fully ingested.
        assert _data_job_input_outputs(wus) == []
        assert _job_urn(wus, "etl_task")

    def test_execute_immediate_yields_no_datajob_edge(self) -> None:
        """EXECUTE IMMEDIATE runs SQL built at runtime, so there is no call
        target to resolve. It must not be mistaken for a procedure call."""
        task = _make_task(
            name="dynamic_task",
            definition="EXECUTE IMMEDIATE 'INSERT INTO tgt SELECT a FROM src'",
        )
        wus, report = _collect_workunits([task])

        ios = _data_job_input_outputs(wus)
        assert all(not io.inputDatajobs for io in ios)
        assert not report.warnings

    def test_multi_statement_task_emits_combined_lineage(self) -> None:
        """Multi-statement task bodies are split and each statement is parsed
        independently, so lineage from all statements is combined into one
        DataJobInputOutput (this is the behavior gained by delegating to
        parse_procedure_code instead of calling sqlglot_lineage directly)."""
        task = _make_task(
            name="multi_stmt_task",
            definition=(
                "INSERT INTO target_a SELECT * FROM source_a; "
                "INSERT INTO target_b SELECT * FROM source_b"
            ),
        )
        wus, report = _collect_workunits([task])

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 1
        io = ios[0]
        assert io.inputDatasets and len(io.inputDatasets) == 2
        assert io.outputDatasets and len(io.outputDatasets) == 2
        assert any("source_a" in d for d in io.inputDatasets)
        assert any("source_b" in d for d in io.inputDatasets)
        assert any("target_a" in d for d in io.outputDatasets)
        assert any("target_b" in d for d in io.outputDatasets)
        assert not report.warnings

    def test_call_only_task_resolves_to_procedure_job_edge(self) -> None:
        """A CALL-only body has no DML statements to parse, but
        parse_procedure_code resolves the call target to a dataJob->dataJob
        edge (the same procedure-call resolution used by stored procedures),
        with no dataset-level lineage and no warning."""
        task = _make_task(
            name="proc_task",
            definition="CALL my_proc('arg1', 'arg2')",
        )
        wus, report = _collect_workunits([task])

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 1
        io = ios[0]
        assert not io.inputDatasets
        assert not io.outputDatasets
        assert io.inputDatajobs and len(io.inputDatajobs) == 1
        assert "my_proc" in io.inputDatajobs[0]
        assert "stored_procedures" in io.inputDatajobs[0]
        assert not report.warnings

    def test_empty_definition_emits_no_dataset_lineage(self) -> None:
        task = _make_task(name="empty_task", definition="")
        wus, _ = _collect_workunits([task])

        ios = _data_job_input_outputs(wus)
        assert len(ios) == 0

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
