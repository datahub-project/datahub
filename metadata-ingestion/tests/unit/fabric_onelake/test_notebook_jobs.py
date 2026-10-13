"""Notebook process emission: per-table jobs, job->job chaining, run DPIs."""

from typing import Optional, Set
from unittest.mock import MagicMock

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.fabric.common.models import (
    FabricJobInstance,
    FabricSparkSession,
    ItemJobStatus,
)
from datahub.ingestion.source.fabric.onelake.notebook_jobs import (
    NotebookProcessEmitter,
    PendingNotebook,
)
from datahub.ingestion.source.fabric.onelake.notebook_spark_lineage import SparkTableRef
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeSourceReport
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    DataProcessInstanceRelationshipsClass,
)
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob

WS = "11111111-1111-1111-1111-111111111111"
LH = "22222222-2222-2222-2222-222222222222"
NB = "33333333-3333-3333-3333-333333333333"


def _onelake(path: str) -> str:
    return f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{LH}/{path}"


# Two chronological executions in one notebook: silver_a -> gold_mid, then
# gold_mid -> gold_final. gold_mid is both an output and the input of a later
# output, which is exactly the intra-notebook job->job dependency.
_PLAN_MID = f"""== Physical Plan ==
(1) SaveIntoDataSourceCommand
Arguments: [path={_onelake("Tables/gold/gold_mid")}], Overwrite
(2) Scan json
Location: InMemoryFileIndex[{_onelake("Tables/silver/silver_a/_delta_log/0.json")}]
"""
_PLAN_FINAL = f"""== Physical Plan ==
(1) SaveIntoDataSourceCommand
Arguments: [path={_onelake("Tables/gold/gold_final")}], Overwrite
(2) Scan json
Location: InMemoryFileIndex[{_onelake("Tables/gold/gold_mid/_delta_log/0.json")}]
"""


def _ref(table: str, schema: str) -> SparkTableRef:
    return SparkTableRef(workspace_id=WS, item_id=LH, table=table, schema=schema)


def _emit() -> list:
    silver_a = _ref("silver_a", "silver")
    gold_mid = _ref("gold_mid", "gold")
    gold_final = _ref("gold_final", "gold")
    known = {ref.to_urn("PROD", None) for ref in (silver_a, gold_mid, gold_final)}

    client = MagicMock()
    client.list_notebook_spark_sessions.return_value = [
        FabricSparkSession(livy_id="livy-1", application_id="app-1", item_id=NB)
    ]
    client.get_spark_sql_plans.return_value = [_PLAN_MID, _PLAN_FINAL]
    client.get_notebook_runs.return_value = [
        FabricJobInstance(
            id="run-1",
            item_id=NB,
            workspace_id=WS,
            status=ItemJobStatus.COMPLETED,
            start_time_utc="2026-10-12T07:00:00Z",
            end_time_utc="2026-10-12T07:05:00Z",
        )
    ]

    emitter = NotebookProcessEmitter(
        platform_instance=None,
        env="PROD",
        execution_history_days=30,
        report=FabricOneLakeSourceReport(),
        client=client,
        known_table_urns=known,
        normalize_name=lambda name: name,
        owners_fn=lambda _upn: None,
    )
    nb = PendingNotebook(
        workspace_id=WS,
        workspace_name="analytics",
        notebook_id=NB,
        display_name="pipeline",
        description=None,
        owner_user_principal_name=None,
        content=None,
        language="python",
    )
    return list(emitter.emit([nb]))


def test_sibling_job_producing_an_input_is_chained_as_input_datajob() -> None:
    emitted = _emit()
    jobs = {dj.urn.job_id: dj for dj in emitted if isinstance(dj, DataJob)}

    assert set(jobs) == {f"{LH}.gold.gold_mid", f"{LH}.gold.gold_final"}

    mid_io = jobs[f"{LH}.gold.gold_mid"]._get_aspect(DataJobInputOutputClass)
    assert mid_io is not None
    # silver_a is a plain upstream table (not produced by a notebook job).
    assert mid_io.inputDatajobs is None

    final_io = jobs[f"{LH}.gold.gold_final"]._get_aspect(DataJobInputOutputClass)
    assert final_io is not None
    # gold_mid is produced by the sibling job, so it chains job->job.
    assert final_io.inputDatajobs == [str(jobs[f"{LH}.gold.gold_mid"].urn)]


# The same output table written under two different identifier casings across
# executions, each crediting a different input. They normalize to one URN/job.
_PLAN_UPPER = f"""== Physical Plan ==
(1) SaveIntoDataSourceCommand
Arguments: [path={_onelake("Tables/gold/Gold_Out")}], Overwrite
(2) Scan json
Location: InMemoryFileIndex[{_onelake("Tables/silver/in_a/_delta_log/0.json")}]
"""
_PLAN_LOWER = f"""== Physical Plan ==
(1) SaveIntoDataSourceCommand
Arguments: [path={_onelake("Tables/gold/gold_out")}], Overwrite
(2) Scan json
Location: InMemoryFileIndex[{_onelake("Tables/silver/in_b/_delta_log/0.json")}]
"""


def _emit_case_variants() -> list:
    out = _ref("gold_out", "gold")
    in_a = _ref("in_a", "silver")
    in_b = _ref("in_b", "silver")
    known = {ref.to_urn("PROD", None, str.lower) for ref in (out, in_a, in_b)}

    client = MagicMock()
    client.list_notebook_spark_sessions.return_value = [
        FabricSparkSession(livy_id="livy-1", application_id="app-1", item_id=NB)
    ]
    client.get_spark_sql_plans.return_value = [_PLAN_UPPER, _PLAN_LOWER]
    client.get_notebook_runs.return_value = []

    emitter = NotebookProcessEmitter(
        platform_instance=None,
        env="PROD",
        execution_history_days=30,
        report=FabricOneLakeSourceReport(),
        client=client,
        known_table_urns=known,
        # Case-insensitive platform: the source lowercases table names.
        normalize_name=str.lower,
        owners_fn=lambda _upn: None,
    )
    nb = PendingNotebook(
        workspace_id=WS,
        workspace_name="analytics",
        notebook_id=NB,
        display_name="pipeline",
        description=None,
        owner_user_principal_name=None,
        content=None,
        language="python",
    )
    return list(emitter.emit([nb]))


def test_case_variant_outputs_merge_into_one_job_unioning_inputs() -> None:
    emitted = _emit_case_variants()
    jobs = [dj for dj in emitted if isinstance(dj, DataJob)]
    # The two casings of the same output table collapse to a single job...
    assert len(jobs) == 1
    io = jobs[0]._get_aspect(DataJobInputOutputClass)
    assert io is not None
    in_a = _ref("in_a", "silver").to_urn("PROD", None, str.lower)
    in_b = _ref("in_b", "silver").to_urn("PROD", None, str.lower)
    # ...and its inputs are the union of both plans, not overwritten.
    assert set(io.inputDatasets) == {in_a, in_b}


def test_run_attaches_as_dpi_to_every_job_not_the_flow() -> None:
    emitted = _emit()
    flow = next(e for e in emitted if isinstance(e, DataFlow))
    job_urns = {str(dj.urn) for dj in emitted if isinstance(dj, DataJob)}

    templates = {
        wu.metadata.aspect.parentTemplate
        for wu in emitted
        if isinstance(wu, MetadataWorkUnit)
        and isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, DataProcessInstanceRelationshipsClass)
    }

    assert templates == job_urns
    assert str(flow.urn) not in templates


def _emit_with(client: MagicMock, known: Optional[Set[str]] = None) -> tuple:
    report = FabricOneLakeSourceReport()
    if known is None:
        known = {
            _ref(t, "gold").to_urn("PROD", None) for t in ("gold_mid", "gold_final")
        }
    emitter = NotebookProcessEmitter(
        platform_instance=None,
        env="PROD",
        execution_history_days=30,
        report=report,
        client=client,
        known_table_urns=known,
        normalize_name=lambda name: name,
        owners_fn=lambda _upn: None,
    )
    nb = PendingNotebook(
        workspace_id=WS,
        workspace_name="analytics",
        notebook_id=NB,
        display_name="pipeline",
        description=None,
        owner_user_principal_name=None,
        content=None,
        language="python",
    )
    return list(emitter.emit([nb])), report


def test_output_not_in_catalog_produces_no_job() -> None:
    # The notebook writes a table that was never ingested (e.g. filtered out by
    # patterns), so no job/dataset is fabricated for it.
    client = MagicMock()
    client.list_notebook_spark_sessions.return_value = [
        FabricSparkSession(livy_id="livy-1", application_id="app-1", item_id=NB)
    ]
    client.get_spark_sql_plans.return_value = [
        f"""== Physical Plan ==
(1) SaveIntoDataSourceCommand
Arguments: [path={_onelake("Tables/gold/orphan")}], Overwrite
(2) Scan json
Location: InMemoryFileIndex[{_onelake("Tables/gold/gold_mid/_delta_log/0.json")}]
"""
    ]
    client.get_notebook_runs.return_value = []

    emitted, _report = _emit_with(client)
    assert not [e for e in emitted if isinstance(e, DataJob)]


_PLAN_SCHEMALESS = f"""== Physical Plan ==
(1) SaveIntoDataSourceCommand
Arguments: [path={_onelake("Tables/orders")}], Overwrite
(2) Scan json
Location: InMemoryFileIndex[{_onelake("Tables/silver/src/_delta_log/0.json")}]
"""


def test_schemaless_output_job_id_matches_the_dbo_dataset_urn() -> None:
    # A schemaless table (Tables/orders) resolves to the dbo-defaulted dataset
    # URN, so its job id must carry dbo too. Otherwise the same table seen as
    # Tables/orders vs Tables/dbo/orders would split into two jobs with separate
    # run histories.
    orders = SparkTableRef(workspace_id=WS, item_id=LH, table="orders", schema=None)
    src = _ref("src", "silver")
    known = {ref.to_urn("PROD", None) for ref in (orders, src)}

    client = MagicMock()
    client.list_notebook_spark_sessions.return_value = [
        FabricSparkSession(livy_id="livy-1", application_id="app-1", item_id=NB)
    ]
    client.get_spark_sql_plans.return_value = [_PLAN_SCHEMALESS]
    client.get_notebook_runs.return_value = []

    emitted, _report = _emit_with(client, known=known)
    jobs = [dj for dj in emitted if isinstance(dj, DataJob)]
    assert len(jobs) == 1
    assert jobs[0].urn.job_id == f"{LH}.dbo.orders"


def test_queued_and_malformed_runs_are_skipped_without_aborting() -> None:
    # NotStarted runs have no timestamps/result (empty shells) and a malformed
    # timestamp must not abort the remaining runs. Only the good run emits.
    client = MagicMock()
    client.list_notebook_spark_sessions.return_value = []
    client.get_notebook_runs.return_value = [
        FabricJobInstance(
            id="queued",
            item_id=NB,
            workspace_id=WS,
            status=ItemJobStatus.NOT_STARTED,
        ),
        FabricJobInstance(
            id="bad",
            item_id=NB,
            workspace_id=WS,
            status=ItemJobStatus.COMPLETED,
            start_time_utc="not-a-timestamp",
            end_time_utc="also-bad",
        ),
        FabricJobInstance(
            id="good",
            item_id=NB,
            workspace_id=WS,
            status=ItemJobStatus.COMPLETED,
            start_time_utc="2026-10-12T07:00:00Z",
            end_time_utc="2026-10-12T07:05:00Z",
        ),
    ]

    emitted, report = _emit_with(client)
    rel_mcps = [
        wu
        for wu in emitted
        if isinstance(wu, MetadataWorkUnit)
        and isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, DataProcessInstanceRelationshipsClass)
    ]
    # Only the good run produced a DPI (one target: the DataFlow).
    assert len(rel_mcps) == 1
    # The malformed run was reported, not raised.
    assert len(report.warnings) >= 1


def test_spark_api_failure_still_emits_the_dataflow() -> None:
    # The Spark monitoring API is best-effort: when it is unavailable the
    # notebook must still surface as a DataFlow rather than crash ingestion.
    client = MagicMock()
    client.list_notebook_spark_sessions.side_effect = RuntimeError("spark down")
    client.get_notebook_runs.side_effect = RuntimeError("scheduler down")

    emitted, report = _emit_with(client)

    assert [e for e in emitted if isinstance(e, DataFlow)]
    assert not [e for e in emitted if isinstance(e, DataJob)]
    assert len(report.warnings) >= 1


def test_runs_fall_back_to_the_dataflow_when_no_lineage_resolves() -> None:
    # A session exists but its plans are gone, so no per-table jobs resolve;
    # the run history then attaches to the DataFlow instead of to jobs.
    client = MagicMock()
    client.list_notebook_spark_sessions.return_value = [
        FabricSparkSession(livy_id="livy-1", application_id="app-1", item_id=NB)
    ]
    client.get_spark_sql_plans.side_effect = RuntimeError("history expired")
    client.get_notebook_runs.return_value = [
        FabricJobInstance(
            id="run-1",
            item_id=NB,
            workspace_id=WS,
            status=ItemJobStatus.COMPLETED,
            start_time_utc="2026-10-12T07:00:00Z",
            end_time_utc="2026-10-12T07:05:00Z",
        )
    ]

    emitted, _report = _emit_with(client)
    flow = next(e for e in emitted if isinstance(e, DataFlow))

    assert not [e for e in emitted if isinstance(e, DataJob)]
    templates = {
        wu.metadata.aspect.parentTemplate
        for wu in emitted
        if isinstance(wu, MetadataWorkUnit)
        and isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, DataProcessInstanceRelationshipsClass)
    }
    assert templates == {str(flow.urn)}
