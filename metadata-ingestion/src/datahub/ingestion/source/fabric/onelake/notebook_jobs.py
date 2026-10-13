"""Emit Fabric notebooks as a data process (DataFlow + DataJob) with lineage.

A Fabric notebook is executable code that reads and writes OneLake tables, so
it is modelled as a process, not just a document: each notebook is its own
pipeline (a DataFlow, subtype "Fabric Spark Pipeline"). Every OneLake table the
notebook writes becomes its own Spark-job DataJob (subtype "Fabric Spark Job")
under that pipeline, carrying that table's precise upstreams resolved from the
executed Spark physical plans — so lineage is table->job->table, not one job
with a cross-product of every table read and written. When one output table is
itself an input to another output table's job within the same notebook, the two
jobs are chained job->job (inputDatajobs). Edges accumulate across the
notebook's runs, which may each execute different cells. Run history is a
property of the whole-notebook run (the Job Scheduler reports one status per
run), so each run becomes a DataProcessInstance of every per-table DataJob (or
of the DataFlow when no per-table jobs were resolved).

The notebook Dataset (subtypes View + Notebook, carrying the code as its view
definition) the connector emits alongside is the human-facing document.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple, Union

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
    make_notebook_flow_id,
)
from datahub.ingestion.source.fabric.common.utils import (
    make_workspace_key,
    parse_iso_datetime,
)
from datahub.ingestion.source.fabric.onelake.client import OneLakeClient
from datahub.ingestion.source.fabric.onelake.notebook_spark_lineage import (
    SparkTableRef,
    extract_spark_lineage,
)
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeSourceReport
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    DataProcessTypeClass,
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
        report: FabricOneLakeSourceReport,
        client: OneLakeClient,
        known_table_urns: Set[str],
        normalize_name: Callable[[str], str],
        owners_fn: OwnersFn,
    ) -> None:
        self._platform_instance = platform_instance
        self._env = env
        self._execution_history_days = execution_history_days
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
        # Each notebook is its own pipeline (DataFlow). Within it, every OneLake
        # table the notebook writes becomes its own Spark-job DataJob carrying
        # that table's precise upstreams, so lineage is table->job->table rather
        # than one job with a cross-product of every table read and written.
        # Jobs whose inputs are produced by sibling jobs chain job->job, and the
        # whole-notebook run attaches as a DataProcessInstance to every job.
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
            # Map each notebook output-table URN to the DataJob that writes it,
            # so a job that reads another job's output table within the same
            # notebook can chain to it as an upstream DataJob (job -> job).
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
        input_urns: List[str],
        output_job_urns: Dict[str, str],
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        output_urn = output.to_urn(
            self._env, self._platform_instance, self._normalize_name
        )
        # Chain to sibling jobs that produce this job's inputs within the same
        # notebook: an input table that is itself a notebook output is written
        # by another DataJob, giving a job -> job dependency edge.
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

        Normalized the same way the output URN is, so the id is stable across
        runs regardless of the case a given plan reported.
        """
        table = self._normalize_name(output.table)
        if output.schema:
            return f"{output.item_id}.{self._normalize_name(output.schema)}.{table}"
        return f"{output.item_id}.{table}"

    def _spark_lineage(self, nb: PendingNotebook) -> Dict[SparkTableRef, List[str]]:
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

        known = self._known_table_urns
        lineage: Dict[SparkTableRef, List[str]] = {}
        for output, inputs in raw.items():
            output_urn = output.to_urn(
                self._env, self._platform_instance, self._normalize_name
            )
            if output_urn not in known:
                continue
            input_urns = {
                ref.to_urn(self._env, self._platform_instance, self._normalize_name)
                for ref in inputs
            }
            lineage[output] = sorted(input_urns & known - {output_urn})
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

        # A Fabric run executes the whole notebook (the Job Scheduler reports one
        # status per run), so the same run is recorded against each per-table
        # DataJob the notebook produces. When no per-table jobs were resolved (no
        # Spark lineage), it falls back to the pipeline DataFlow so run history is
        # not lost.
        targets: List[Union[DataJobUrn, DataFlowUrn]] = [
            DataJobUrn.from_string(job_urn)
            for job_urn in sorted(set(output_job_urns.values()))
        ] or [dataflow.urn]
        for run in runs:
            yield from self._emit_run(run, targets)
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
