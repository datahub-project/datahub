"""Emit Fabric notebooks as a data process (DataFlow + DataJob) with lineage.

A notebook reads and writes OneLake tables, so it is modelled as a pipeline
(DataFlow, subtype "Fabric Spark Pipeline"). Every table it writes becomes its
own DataJob (subtype "Fabric Spark Job") carrying that table's upstreams
resolved from the executed Spark plans, so lineage is table->job->table rather
than one job with a cross-product of every table read and written. A job whose
input is another output of the same notebook chains job->job via inputDatajobs.
The Job Scheduler reports one status per whole-notebook run, so each run becomes
a DataProcessInstance of every per-table job (or of the DataFlow when no
per-table lineage was resolved).
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple, Union

import datahub.emitter.mce_builder as builder
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    DataFlowSubTypes,
    DataJobSubTypes,
)
from datahub.ingestion.source.fabric.common.constants import FABRIC_APP_BASE_URL
from datahub.ingestion.source.fabric.common.models import (
    FabricJobInstance,
    ItemJobStatus,
    WorkspaceKey,
)
from datahub.ingestion.source.fabric.common.urn_generator import (
    DEFAULT_SCHEMA_FOR_SCHEMALESS,
    make_notebook_flow_id,
)
from datahub.ingestion.source.fabric.common.utils import (
    make_workspace_key,
    parse_iso_datetime,
)
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.notebook_code_lineage import (
    NotebookCodeAnalysis,
    NotebookCodeExtractionError,
    analyze_notebook_code,
    reconcile_column_lineage,
    reconcile_spark_lineage,
)
from datahub.ingestion.source.fabric.onelake.notebook_spark_lineage import (
    SparkTableRef,
    extract_spark_lineage,
)
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeSourceReport
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    DataProcessTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)
from datahub.metadata.urns import CorpUserUrn, DataFlowUrn, DataJobUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity

OwnersFn = Callable[[Optional[str]], Optional[List[Tuple[CorpUserUrn, str]]]]

logger = logging.getLogger(__name__)

_PLATFORM = "fabric-onelake"

# Fabric run status -> DataHub InstanceRunResult.
_STATUS_TO_RESULT: Dict[str, InstanceRunResult] = {
    ItemJobStatus.COMPLETED: InstanceRunResult.SUCCESS,
    ItemJobStatus.FAILED: InstanceRunResult.FAILURE,
    ItemJobStatus.CANCELLED: InstanceRunResult.SKIPPED,
    ItemJobStatus.DEDUPED: InstanceRunResult.SKIPPED,
}


def _iso_to_millis(iso_str: str) -> int:
    return int(parse_iso_datetime(iso_str).timestamp() * 1000)


@dataclass
class _JobLineage:
    """Resolved upstream datasets, plus column renames when the code names them."""

    input_urns: List[str] = field(default_factory=list)
    columns: Dict[str, Set[Tuple[str, str]]] = field(default_factory=dict)


@dataclass
class PendingNotebook:
    """A notebook deferred for process emission after all tables are ingested.

    Deferred because notebook table references resolve against tables that may
    live in a different workspace, so the registry must be fully populated
    first (mirrors the deferred shortcut-dataset pass).
    """

    workspace_id: str
    workspace_name: str
    notebook_id: str
    display_name: str
    description: Optional[str]
    owner_user_principal_name: Optional[str]
    content: Optional[str]
    language: Optional[str]


class NotebookProcessEmitter:
    """Builds notebook DataFlow/DataJob/DPI workunits from deferred notebooks."""

    def __init__(
        self,
        *,
        platform_instance: Optional[str],
        env: str,
        execution_history_days: int,
        reconcile_code: bool = False,
        case_sensitive_columns: bool = False,
        report: FabricOneLakeSourceReport,
        client: OneLakeClient,
        known_table_urns: Set[str],
        normalize_name: Callable[[str], str],
        owners_fn: OwnersFn,
    ) -> None:
        self._platform_instance = platform_instance
        self._env = env
        self._execution_history_days = execution_history_days
        self._reconcile_code = reconcile_code
        self._case_sensitive_columns = case_sensitive_columns
        self._report = report
        self._client = client
        self._known_table_urns = known_table_urns
        # Same name normalization the source applied when registering ingested
        # tables, so Spark-plan URNs match the known set regardless of case.
        self._normalize_name = normalize_name
        # Reuse the source's owner builder so ownership modelling stays identical.
        self._owners_fn = owners_fn

    def emit(
        self, pending: List[PendingNotebook]
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        for nb in pending:
            workspace_key = make_workspace_key(
                nb.workspace_id, self._platform_instance, self._env
            )
            dataflow = DataFlow(
                platform=_PLATFORM,
                name=make_notebook_flow_id(nb.workspace_id, nb.notebook_id),
                platform_instance=self._platform_instance,
                env=self._env,
                display_name=nb.display_name,
                subtype=DataFlowSubTypes.FABRIC_SPARK_PIPELINE,
                parent_container=workspace_key,
                external_url=(
                    f"{FABRIC_APP_BASE_URL}/groups/{nb.workspace_id}"
                    f"/synapsenotebooks/{nb.notebook_id}"
                ),
            )
            yield dataflow

            lineage = self._spark_lineage(nb)
            if lineage:
                self._report.report_notebook_spark_lineage_extracted()
            # Output-table URN -> the job that writes it, so a job can chain to
            # the sibling job that produces one of its inputs (job -> job).
            output_job_urns = {
                out.to_urn(
                    self._env, self._platform_instance, self._normalize_name
                ): str(
                    DataJobUrn.create_from_ids(
                        job_id=self._job_id(out),
                        data_flow_urn=str(dataflow.urn),
                    )
                )
                for out in lineage
            }
            # Deterministic order (by job id) so repeated runs emit identically.
            for output in sorted(lineage, key=self._job_id):
                yield from self._emit_output_job(
                    nb,
                    dataflow,
                    workspace_key,
                    output,
                    lineage[output],
                    output_job_urns,
                )

            yield from self._emit_runs(nb, dataflow, output_job_urns)

    def _emit_output_job(
        self,
        nb: PendingNotebook,
        dataflow: DataFlow,
        workspace_key: WorkspaceKey,
        output: SparkTableRef,
        resolved: _JobLineage,
        output_job_urns: Dict[str, str],
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        input_urns = resolved.input_urns
        output_urn = output.to_urn(
            self._env, self._platform_instance, self._normalize_name
        )
        # An input that is itself a notebook output is written by a sibling job,
        # giving a job -> job edge alongside the dataset edge.
        this_job_urn = output_job_urns.get(output_urn)
        input_datajobs = sorted(
            job_urn
            for in_urn in input_urns
            if (job_urn := output_job_urns.get(in_urn)) is not None
            and job_urn != this_job_urn
        )
        io_aspect = DataJobInputOutputClass(
            inputDatasets=input_urns,
            outputDatasets=[output_urn],
            inputDatajobs=input_datajobs or None,
            fineGrainedLineages=self._fine_grained(output_urn, resolved.columns),
        )

        external_url = (
            f"{FABRIC_APP_BASE_URL}/groups/{nb.workspace_id}"
            f"/synapsenotebooks/{nb.notebook_id}"
        )
        custom_properties = {"notebook_id": nb.notebook_id}
        if nb.language:
            custom_properties["language"] = nb.language

        datajob = DataJob(
            name=self._job_id(output),
            flow=dataflow,
            display_name=f"{nb.display_name} -> {self._output_label(output)}",
            description=nb.description,
            subtype=DataJobSubTypes.FABRIC_SPARK_JOB,
            external_url=external_url,
            custom_properties=custom_properties,
            extra_aspects=[io_aspect],
            # The SDK coerces (userPrincipalName, ownershipType) string tuples
            # into owner URNs at runtime; its stub only advertises urn inputs.
            owners=self._owners_fn(nb.owner_user_principal_name),
        )
        datajob._set_container(workspace_key)
        yield datajob

    @staticmethod
    def _output_label(output: SparkTableRef) -> str:
        return f"{output.schema}.{output.table}" if output.schema else output.table

    def _job_id(self, output: SparkTableRef) -> str:
        """A per-output job id, unique within the notebook's DataFlow.

        Built the same way the output URN is, so the id is stable across runs
        regardless of the case a given plan reported and regardless of whether
        the Spark path carried the schema: a schemaless table defaults to the
        same ``dbo`` schema ``make_onelake_urn`` uses, so ``Tables/orders`` and
        ``Tables/dbo/orders`` map to one job (and one run history), not two.
        """
        table = self._normalize_name(output.table)
        schema = (
            self._normalize_name(output.schema)
            if output.schema
            else DEFAULT_SCHEMA_FOR_SCHEMALESS
        )
        return f"{output.item_id}.{schema}.{table}"

    def _fine_grained(
        self, output_urn: str, columns: Dict[str, Set[Tuple[str, str]]]
    ) -> Optional[List[FineGrainedLineageClass]]:
        entries: List[FineGrainedLineageClass] = []
        for downstream, upstreams in sorted(columns.items()):
            upstream_urns = sorted(
                {
                    builder.make_schema_field_urn(dataset_urn, column)
                    for dataset_urn, column in upstreams
                }
            )
            if not upstream_urns:
                continue
            entries.append(
                FineGrainedLineageClass(
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    upstreams=upstream_urns,
                    downstreams=[builder.make_schema_field_urn(output_urn, downstream)],
                )
            )
        return entries or None

    def _spark_lineage(self, nb: PendingNotebook) -> Dict[SparkTableRef, _JobLineage]:
        """Resolve per-output-table lineage from the executed Spark plans.

        Returns each written table (that was itself ingested in this run) mapped
        to the sorted URNs of its upstream tables. Only edges to tables ingested
        in this run are kept (no ghost datasets); a GUID-built URN that does not
        match an ingested table is dropped.
        """
        lookback_start = datetime.now(timezone.utc) - timedelta(
            days=self._execution_history_days
        )
        try:
            sessions = self._client.list_notebook_spark_sessions(
                nb.workspace_id, nb.notebook_id, lookback_start
            )
        except Exception as e:
            self._report.warning(
                title="Failed to Fetch Notebook Spark Sessions",
                message="Could not retrieve Spark run history for lineage. Skipping.",
                context=f"workspace={nb.workspace_name}, notebook={nb.display_name}",
                exc=e,
                log=False,
            )
            return {}

        # Accumulate output->inputs across every run in the window; each run may
        # execute only some cells, so lineage is the union over all of them.
        raw: Dict[SparkTableRef, Set[SparkTableRef]] = {}
        for session in sessions:
            self._report.report_notebook_spark_session_scanned()
            try:
                plans = self._client.get_spark_sql_plans(
                    nb.workspace_id,
                    nb.notebook_id,
                    session.livy_id,
                    session.application_id,
                )
            except Exception as e:
                self._report.warning(
                    title="Failed to Fetch Spark SQL Plans",
                    message="Could not retrieve a Spark run's SQL plans. Skipping.",
                    context=f"notebook={nb.display_name}, app={session.application_id}",
                    exc=e,
                    log=False,
                )
                continue
            for output, inputs in extract_spark_lineage(plans).items():
                raw.setdefault(output, set()).update(inputs)

        # Plans credit every earlier read to the next write. The notebook
        # source says which of those reads each write actually uses, and which
        # source column each output column was renamed from.
        code_columns: Dict[
            SparkTableRef, Dict[str, Set[Tuple[SparkTableRef, str]]]
        ] = {}
        if self._reconcile_code and nb.content and nb.language in (None, "python"):
            code: Optional[NotebookCodeAnalysis] = None
            try:
                code = analyze_notebook_code(
                    nb.content,
                    case_sensitive_columns=self._case_sensitive_columns,
                )
            except NotebookCodeExtractionError as exc:
                self._report.report_notebook_code_lineage_failed()
                self._report.warning(
                    title="Failed to Extract Notebook Code Lineage",
                    message=(
                        "Could not read the notebook source for lineage. "
                        "Keeping Spark-plan table lineage."
                    ),
                    context=f"notebook={nb.display_name}",
                    exc=exc,
                    log=False,
                )
            if code is not None and code.tables:
                reconciled = reconcile_spark_lineage(
                    code.tables, raw, self._normalize_name
                )
                if reconciled:
                    logger.debug(
                        "Reconciled notebook %s lineage from source text (%s output table(s))",
                        nb.display_name,
                        len(reconciled),
                    )
                    raw = reconciled
                    code_columns = reconcile_column_lineage(
                        code.columns, raw, self._normalize_name
                    )

        known = self._known_table_urns
        # Case/space variants of one output table normalize to a single URN and
        # job id, so merge their input sets by that URN. Otherwise each variant
        # emits the same job and the later emission overwrites the earlier,
        # dropping the inputs that were only seen under the other casing.
        merged: Dict[
            str,
            Tuple[SparkTableRef, Set[SparkTableRef], Dict[str, Set[Tuple[str, str]]]],
        ] = {}
        for output, inputs in raw.items():
            output_urn = output.to_urn(
                self._env, self._platform_instance, self._normalize_name
            )
            _, acc, cols = merged.setdefault(output_urn, (output, set(), {}))
            acc.update(inputs)
            for downstream, upstreams in code_columns.get(output, {}).items():
                # Same normalization as schema fields, so a code name
                # ``person_PersonParty`` matches the ingested ``person_personparty``.
                resolved = cols.setdefault(self._normalize_name(downstream), set())
                for ref, column in upstreams:
                    dataset_urn = ref.to_urn(
                        self._env, self._platform_instance, self._normalize_name
                    )
                    resolved.add((dataset_urn, self._normalize_name(column)))

        lineage: Dict[SparkTableRef, _JobLineage] = {}
        for output_urn, (output, inputs, columns) in merged.items():
            if output_urn not in known:
                continue
            input_urns = {
                ref.to_urn(self._env, self._platform_instance, self._normalize_name)
                for ref in inputs
            }
            kept_inputs = input_urns & known - {output_urn}
            kept_columns = {
                downstream: {
                    (dataset_urn, column)
                    for dataset_urn, column in upstreams
                    if dataset_urn in kept_inputs
                }
                for downstream, upstreams in columns.items()
            }
            lineage[output] = _JobLineage(
                input_urns=sorted(kept_inputs),
                columns={
                    downstream: refs
                    for downstream, refs in kept_columns.items()
                    if refs
                },
            )
        return lineage

    def _emit_runs(
        self,
        nb: PendingNotebook,
        dataflow: DataFlow,
        output_job_urns: Dict[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        try:
            lookback_start = datetime.now(timezone.utc) - timedelta(
                days=self._execution_history_days
            )
            runs = self._client.get_notebook_runs(
                nb.workspace_id,
                nb.notebook_id,
                lookback_start,
            )
        except Exception as e:
            self._report.warning(
                title="Failed to Fetch Notebook Runs",
                message="Could not retrieve notebook run history. Skipping.",
                context=f"workspace={nb.workspace_name}, notebook={nb.display_name}",
                exc=e,
                log=False,
            )
            return

        # The run is recorded against every per-table job; fall back to the
        # DataFlow when no per-table lineage was resolved.
        targets: List[Union[DataJobUrn, DataFlowUrn]] = [
            DataJobUrn.from_string(job_urn)
            for job_urn in sorted(set(output_job_urns.values()))
        ] or [dataflow.urn]
        for run in runs:
            if run.status == ItemJobStatus.NOT_STARTED:
                # Queued runs have no timestamps and no result mapping, so a DPI
                # would be an empty shell. Mirror the Fabric Data Factory source
                # and skip them.
                continue
            try:
                # Materialize before yielding so a malformed run (e.g. a bad
                # timestamp) is skipped whole, and one bad run never aborts the
                # remaining runs or notebooks.
                run_wus = list(self._emit_run(run, targets))
            except Exception as e:
                self._report.warning(
                    title="Failed to Emit Notebook Run",
                    message="Could not emit a notebook run instance. Skipping.",
                    context=(
                        f"workspace={nb.workspace_name}, "
                        f"notebook={nb.display_name}, run={run.id}"
                    ),
                    exc=e,
                    log=False,
                )
                continue
            yield from run_wus
            self._report.report_notebook_run_scanned()

    def _emit_run(
        self,
        run: FabricJobInstance,
        template_urns: List[Union[DataJobUrn, DataFlowUrn]],
    ) -> Iterable[MetadataWorkUnit]:
        custom_properties: Dict[str, str] = {
            "run_id": run.id,
            "workspace_id": run.workspace_id,
            "notebook_id": run.item_id,
        }
        if run.invoke_type is not None:
            custom_properties["invoke_type"] = run.invoke_type
        if run.failure_reason:
            custom_properties["failure_reason"] = run.failure_reason

        start_ts = _iso_to_millis(run.start_time_utc) if run.start_time_utc else None
        result = _STATUS_TO_RESULT.get(run.status)
        for template_urn in template_urns:
            dpi = DataProcessInstance(
                id=self._run_dpi_id(template_urn, run.id),
                orchestrator=_PLATFORM,
                cluster=self._env or "PROD",
                template_urn=template_urn,
                type=DataProcessTypeClass.BATCH_SCHEDULED,
                properties=custom_properties,
                data_platform_instance=self._platform_instance,
            )
            for mcp in dpi.generate_mcp(
                created_ts_millis=start_ts, materialize_iolets=False
            ):
                yield mcp.as_workunit()
            if start_ts:
                for mcp in dpi.start_event_mcp(start_timestamp_millis=start_ts):
                    yield mcp.as_workunit()
            if result is not None and run.end_time_utc:
                for mcp in dpi.end_event_mcp(
                    end_timestamp_millis=_iso_to_millis(run.end_time_utc),
                    result=result,
                    result_type=_PLATFORM,
                    start_timestamp_millis=start_ts,
                ):
                    yield mcp.as_workunit()

    @staticmethod
    def _run_dpi_id(template_urn: Union[DataJobUrn, DataFlowUrn], run_id: str) -> str:
        # The DPI urn is derived from this id, so namespace by job when the run
        # attaches to a per-table DataJob to keep each job's instance distinct.
        if isinstance(template_urn, DataJobUrn):
            return f"{template_urn.job_id}.{run_id}"
        return run_id
