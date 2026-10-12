"""Emit Fabric notebooks as a data process (DataFlow + DataJob) with lineage.

A Fabric notebook is executable code that reads and writes OneLake tables, so
it is modelled as a process, not just a document: one DataFlow per workspace
(so a workspace's notebooks group together) with one DataJob per notebook.
Static table lineage parsed from the notebook source becomes the DataJob's
input/output edges, and Job Scheduler run history becomes DataProcessInstances
under the job.

The notebook Dataset (subtype Notebook) the connector already emits is kept as
the human-facing document; this adds the process view alongside it.
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
from datahub.ingestion.source.common.subtypes import DataJobSubTypes
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
from datahub.ingestion.source.fabric.onelake.notebook_lineage import (
    NotebookReferences,
    NotebookTableRegistry,
    extract_table_references,
    resolve_lineage,
)
from datahub.ingestion.source.fabric.onelake.notebook_spark_lineage import (
    extract_spark_io,
)
from datahub.ingestion.source.fabric.onelake.report import FabricOneLakeSourceReport
from datahub.metadata.schema_classes import (
    DataJobInputOutputClass,
    DataProcessTypeClass,
)
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity

OwnersFn = Callable[[Optional[str]], Optional[List[Tuple[str, str]]]]

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
        include_lineage: bool,
        include_spark_lineage: bool,
        include_runs: bool,
        execution_history_days: int,
        report: FabricOneLakeSourceReport,
        client: OneLakeClient,
        registry: NotebookTableRegistry,
        owners_fn: OwnersFn,
    ) -> None:
        self._platform_instance = platform_instance
        self._env = env
        self._include_lineage = include_lineage
        self._include_spark_lineage = include_spark_lineage
        self._include_runs = include_runs
        self._execution_history_days = execution_history_days
        self._report = report
        self._client = client
        self._registry = registry
        # Reuse the source's owner builder so ownership modelling stays identical.
        self._owners_fn = owners_fn

    def emit(
        self, pending: List[PendingNotebook]
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        by_workspace: Dict[str, List[PendingNotebook]] = {}
        for nb in pending:
            by_workspace.setdefault(nb.workspace_id, []).append(nb)

        for workspace_id, notebooks in by_workspace.items():
            workspace_key = make_workspace_key(
                workspace_id, self._platform_instance, self._env
            )
            dataflow = DataFlow(
                platform=_PLATFORM,
                name=make_notebook_flow_id(workspace_id),
                platform_instance=self._platform_instance,
                env=self._env,
                display_name=f"{notebooks[0].workspace_name} notebooks",
                parent_container=workspace_key,
            )
            yield dataflow

            for nb in notebooks:
                yield from self._emit_notebook_job(nb, dataflow, workspace_key)

    def _emit_notebook_job(
        self,
        nb: PendingNotebook,
        dataflow: DataFlow,
        workspace_key: WorkspaceKey,
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        io_aspect = (
            self._build_io(nb)
            if (self._include_lineage or self._include_spark_lineage)
            else None
        )

        external_url = (
            f"{FABRIC_APP_BASE_URL}/groups/{nb.workspace_id}"
            f"/synapsenotebooks/{nb.notebook_id}"
        )
        custom_properties = {"notebook_id": nb.notebook_id}
        if nb.language:
            custom_properties["language"] = nb.language

        datajob = DataJob(
            name=nb.notebook_id,
            flow=dataflow,
            display_name=nb.display_name,
            description=nb.description,
            subtype=DataJobSubTypes.FABRIC_TRIDENT_NOTEBOOK,
            external_url=external_url,
            custom_properties=custom_properties,
            extra_aspects=[io_aspect] if io_aspect is not None else None,
            # The SDK coerces (userPrincipalName, ownershipType) string tuples
            # into owner URNs at runtime; its stub only advertises urn inputs.
            owners=self._owners_fn(nb.owner_user_principal_name),  # type: ignore[arg-type]
        )
        datajob._set_container(workspace_key)
        yield datajob

        if self._include_runs:
            yield from self._emit_runs(nb, datajob)

    def _build_io(self, nb: PendingNotebook) -> Optional[DataJobInputOutputClass]:
        inputs: Set[str] = set()
        outputs: Set[str] = set()

        if self._include_lineage:
            refs: NotebookReferences = extract_table_references(nb.content, nb.language)
            resolved = resolve_lineage(refs, self._registry)
            if resolved.unresolved:
                self._report.report_notebook_lineage_unresolved(resolved.unresolved)
            if resolved.input_urns or resolved.output_urns:
                self._report.report_notebook_lineage_extracted()
            inputs.update(resolved.input_urns)
            outputs.update(resolved.output_urns)

        if self._include_spark_lineage:
            spark_inputs, spark_outputs = self._spark_io(nb)
            if spark_inputs or spark_outputs:
                self._report.report_notebook_spark_lineage_extracted()
            inputs.update(spark_inputs)
            outputs.update(spark_outputs)

        if not inputs and not outputs:
            return None
        return DataJobInputOutputClass(
            inputDatasets=sorted(inputs),
            outputDatasets=sorted(outputs),
        )

    def _spark_io(self, nb: PendingNotebook) -> Tuple[Set[str], Set[str]]:
        """Resolve lineage from the notebook's executed Spark physical plans.

        Only edges to tables ingested in this run are kept (matching the static
        resolver's no-ghost-datasets philosophy); a GUID-built URL that does not
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
            return set(), set()

        known = self._registry.known_urns()
        inputs: Set[str] = set()
        outputs: Set[str] = set()
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
            io = extract_spark_io(plans)
            for ref in io.inputs:
                urn = ref.to_urn(self._env, self._platform_instance)
                if urn in known:
                    inputs.add(urn)
            for ref in io.outputs:
                urn = ref.to_urn(self._env, self._platform_instance)
                if urn in known:
                    outputs.add(urn)
        # A table both read and written in a run is an output only.
        inputs -= outputs
        return inputs, outputs

    def _emit_runs(
        self, nb: PendingNotebook, datajob: DataJob
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

        for run in runs:
            yield from self._emit_run(run, datajob)
            self._report.report_notebook_run_scanned()

    def _emit_run(
        self, run: FabricJobInstance, datajob: DataJob
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

        dpi = DataProcessInstance(
            id=run.id,
            orchestrator=_PLATFORM,
            cluster=self._env or "PROD",
            template_urn=datajob.urn,
            type=DataProcessTypeClass.BATCH_SCHEDULED,
            properties=custom_properties,
            data_platform_instance=self._platform_instance,
        )

        start_ts = _iso_to_millis(run.start_time_utc) if run.start_time_utc else None
        for mcp in dpi.generate_mcp(
            created_ts_millis=start_ts, materialize_iolets=False
        ):
            yield mcp.as_workunit()
        if start_ts:
            for mcp in dpi.start_event_mcp(start_timestamp_millis=start_ts):
                yield mcp.as_workunit()

        result = _STATUS_TO_RESULT.get(run.status)
        if result is not None and run.end_time_utc:
            for mcp in dpi.end_event_mcp(
                end_timestamp_millis=_iso_to_millis(run.end_time_utc),
                result=result,
                result_type=_PLATFORM,
                start_timestamp_millis=start_ts,
            ):
                yield mcp.as_workunit()
