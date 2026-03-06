"""
DataHub ingestion source for dlt (data load tool).

Extracts pipeline metadata from dlt's local state directory and emits:
  - DataFlow entities (one per dlt pipeline)
  - DataJob entities (one per dlt table/resource)
  - Lineage from dlt resources to destination Dataset URNs
  - DataProcessInstance run history (from _dlt_loads, opt-in)

Reference: https://dlthub.com/docs/general-usage/pipeline
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable, List, Optional, Union

import datahub.emitter.mce_builder as builder
from datahub.api.entities.datajob import DataJob as DataJobV1
from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dlt.config import DltSourceConfig
from datahub.ingestion.source.dlt.data_classes import (
    DltLoadInfo,
    DltPipelineInfo,
    DltTableInfo,
)
from datahub.ingestion.source.dlt.dlt_client import DltClient
from datahub.ingestion.source.dlt.dlt_report import DltSourceReport
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
)
from datahub.metadata.urns import DataFlowUrn, DatasetUrn
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.entity import Entity

logger = logging.getLogger(__name__)

# dlt system tables — these exist in every dlt destination schema but are
# not meaningful as lineage targets in DataHub.
DLT_SYSTEM_TABLES = frozenset({"_dlt_loads", "_dlt_version", "_dlt_pipeline_state"})

# dlt load status codes: 0 = complete (success); all other values indicate
# in-progress or failed loads.
_DLT_LOAD_STATUS_MAP: dict[int, InstanceRunResult] = {
    0: InstanceRunResult.SUCCESS,
}


@platform_name("dlt")
@config_class(DltSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Emits outlet lineage from dlt DataJobs to destination Dataset URNs. "
    "Configure destination_platform_map to match your destination connector's env/instance.",
)
@capability(
    SourceCapability.OWNERSHIP,
    "Emitted when dlt pipeline state contains owner information",
    supported=False,
)
@capability(SourceCapability.DELETION_DETECTION, "Enabled via stateful ingestion")
class DltSource(StatefulIngestionSourceBase, TestableSource):
    """
    DataHub ingestion source for dlt (data load tool).

    Reads pipeline metadata from dlt's local state directory
    (default: ~/.dlt/pipelines/) and emits DataFlow, DataJob, and lineage
    entities to DataHub.

    Requires:
      - dlt pipeline state files on disk (created automatically by any dlt.pipeline().run() call)
      - Optionally: dlt package installed + destination credentials for run history

    See: https://dlthub.com/docs/general-usage/pipeline
    """

    platform = "dlt"

    def __init__(self, config: DltSourceConfig, ctx: PipelineContext) -> None:
        super().__init__(config, ctx)
        self.config: DltSourceConfig = config
        self.report = DltSourceReport()
        self.client = DltClient(pipelines_dir=config.pipelines_dir, report=self.report)

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "DltSource":
        config = DltSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    # ------------------------------------------------------------------
    # Workunit processors
    # ------------------------------------------------------------------

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    # ------------------------------------------------------------------
    # Main extraction
    # ------------------------------------------------------------------

    def get_workunits_internal(self) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        pipeline_names = self.client.list_pipeline_names()
        if not pipeline_names:
            self.report.warning(
                title="No dlt pipelines found",
                message="No pipeline state directories were found in pipelines_dir.",
                context=self.config.pipelines_dir,
            )
            return

        for pipeline_name in pipeline_names:
            if not self.config.pipeline_pattern.allowed(pipeline_name):
                self.report.report_pipeline_filtered(pipeline_name)
                continue

            pipeline_info = self.client.get_pipeline_info(pipeline_name)
            if pipeline_info is None:
                self.report.warning(
                    title="Failed to read pipeline metadata",
                    message="Could not read pipeline state. Skipping this pipeline.",
                    context=pipeline_name,
                )
                self.report.report_schema_read_error()
                continue

            if not pipeline_info.schemas:
                self.report.warning(
                    title="No schemas found for pipeline",
                    message="Pipeline directory exists but no schema files were readable. DataJobs will not be emitted.",
                    context=pipeline_name,
                )
                self.report.report_schema_read_error()

            yield from self._process_pipeline(pipeline_info)
            self.report.report_pipeline_scanned()

    def _process_pipeline(
        self, pipeline_info: DltPipelineInfo
    ) -> Iterable[Union[MetadataWorkUnit, Entity]]:
        """Emit DataFlow + DataJobs for a single dlt pipeline."""
        dataflow = self._build_dataflow(pipeline_info)
        yield from dataflow.as_workunits()

        for schema in pipeline_info.schemas:
            for table in schema.tables:
                if table.table_name in DLT_SYSTEM_TABLES:
                    self.report.report_resource_filtered(table.table_name)
                    continue

                outlets = self._build_outlet_urns(pipeline_info, table)
                inlets = self._build_inlet_urns(
                    pipeline_info.pipeline_name, table.table_name
                )
                fine_grained_lineages = self._build_fine_grained_lineages(
                    table, inlets, outlets
                )

                datajob = self._build_datajob(
                    pipeline_info,
                    table,
                    schema.schema_name,
                    dataflow,
                    inlets,
                    outlets,
                    fine_grained_lineages,
                )
                yield from datajob.as_workunits()
                self.report.report_resource_scanned()

        # Run history (opt-in)
        if self.config.include_run_history:
            yield from self._emit_run_history(pipeline_info)

    # ------------------------------------------------------------------
    # Entity builders
    # ------------------------------------------------------------------

    def _build_dataflow(self, pipeline_info: DltPipelineInfo) -> DataFlow:
        """Build a DataFlow entity for a dlt pipeline."""
        custom_props = {
            "destination": pipeline_info.destination,
            "dataset_name": pipeline_info.dataset_name,
            "pipelines_dir": pipeline_info.pipelines_dir,
        }
        if pipeline_info.working_dir:
            custom_props["working_dir"] = pipeline_info.working_dir

        return DataFlow(
            platform=self.platform,
            name=pipeline_info.pipeline_name,
            env=self.config.env,
            display_name=pipeline_info.pipeline_name,
            platform_instance=self.config.platform_instance,
            custom_properties=custom_props,
        )

    def _build_datajob(
        self,
        pipeline_info: DltPipelineInfo,
        table: DltTableInfo,
        schema_name: str,
        dataflow: DataFlow,
        inlets: List[DatasetUrn],
        outlets: List[DatasetUrn],
        fine_grained_lineages: Optional[List[FineGrainedLineageClass]] = None,
    ) -> DataJob:
        """Build a DataJob entity for a single dlt table/resource."""
        custom_props: dict[str, str] = {
            "write_disposition": table.write_disposition,
            "schema_name": schema_name,
        }
        if table.parent_table:
            custom_props["parent_table"] = table.parent_table
        if table.resource_name:
            custom_props["resource_name"] = table.resource_name

        datajob = DataJob(
            name=table.table_name,
            flow=dataflow,
            platform_instance=self.config.platform_instance,
            display_name=table.table_name,
            custom_properties=custom_props,
            inlets=inlets,
            outlets=outlets,
            fine_grained_lineages=fine_grained_lineages or [],
        )
        return datajob

    # ------------------------------------------------------------------
    # URN construction
    # ------------------------------------------------------------------

    def _build_outlet_urns(
        self, pipeline_info: DltPipelineInfo, table: DltTableInfo
    ) -> List[DatasetUrn]:
        """Construct Dataset URNs for the destination table (outlet lineage)."""
        if not self.config.include_lineage:
            return []
        if not pipeline_info.destination:
            return []

        dest_cfg = self.config.destination_platform_map.get(pipeline_info.destination)
        dest_env = dest_cfg.env if dest_cfg else self.config.env
        dest_instance = dest_cfg.platform_instance if dest_cfg else None
        dest_database = dest_cfg.database if dest_cfg else None

        # Construct the table path to match what the destination connector emits.
        # SQL destinations (Postgres, Redshift) use database.schema.table (3-part).
        # Cloud warehouses (BigQuery, Snowflake) use schema.table or project.dataset.table.
        # The optional `database` field in DestinationPlatformConfig provides the prefix.
        if dest_database and pipeline_info.dataset_name:
            table_path = (
                f"{dest_database}.{pipeline_info.dataset_name}.{table.table_name}"
            )
        elif pipeline_info.dataset_name:
            table_path = f"{pipeline_info.dataset_name}.{table.table_name}"
        else:
            table_path = table.table_name

        return [
            DatasetUrn.create_from_ids(
                platform_id=pipeline_info.destination,
                table_name=table_path,
                env=dest_env,
                platform_instance=dest_instance,
            )
        ]

    def _build_inlet_urns(
        self, pipeline_name: str, table_name: str
    ) -> List[DatasetUrn]:
        """Return manually-configured inlet Dataset URNs for this pipeline/table.

        Merges two config sources:
        - source_dataset_urns[pipeline_name]: pipeline-level inlets applied to every DataJob.
          Use for REST API pipelines where all tasks share the same upstream source.
        - source_table_dataset_urns[pipeline_name][table_name]: per-table inlets for 1:1 lineage.
          Use for sql_database pipelines where each DataJob reads from exactly one source table.
        """
        pipeline_urns = self.config.source_dataset_urns.get(pipeline_name, [])
        table_urns = self.config.source_table_dataset_urns.get(pipeline_name, {}).get(
            table_name, []
        )
        inlets = []
        for urn_str in pipeline_urns + table_urns:
            try:
                inlets.append(DatasetUrn.from_string(urn_str))
            except Exception as e:
                logger.warning("Could not parse inlet URN '%s': %s", urn_str, e)
                self.report.warning(
                    title="Invalid inlet dataset URN",
                    message="Could not parse source_dataset_urns entry. Skipping.",
                    context=f"pipeline={pipeline_name}, table={table_name}, urn={urn_str}",
                )
        return inlets

    def _build_fine_grained_lineages(
        self,
        table: DltTableInfo,
        inlets: List[DatasetUrn],
        outlets: List[DatasetUrn],
    ) -> List[FineGrainedLineageClass]:
        """Build 1:1 column-level lineage for direct-copy pipelines (e.g. sql_database).

        Only emits CLL when there is exactly one inlet and one outlet — the unambiguous
        1:1 table copy case. dlt system columns (_dlt_id, _dlt_load_id, etc.) are
        excluded since they are injected by dlt and have no corresponding source column.
        """
        if not self.config.include_lineage:
            return []
        if len(inlets) != 1 or len(outlets) != 1:
            # Fanout or fanin — can't assert 1:1 column mapping
            return []

        user_columns = [c for c in table.columns if not c.is_dlt_system_column]
        if not user_columns:
            return []

        inlet_urn = str(inlets[0])
        outlet_urn = str(outlets[0])

        return [
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[builder.make_schema_field_urn(inlet_urn, col.name)],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[builder.make_schema_field_urn(outlet_urn, col.name)],
            )
            for col in user_columns
        ]

    # ------------------------------------------------------------------
    # Run history
    # ------------------------------------------------------------------

    def _emit_run_history(
        self, pipeline_info: DltPipelineInfo
    ) -> Iterable[MetadataWorkUnit]:
        """Query _dlt_loads and emit DataProcessInstance entities for each completed run."""
        # Check dlt availability before querying. get_run_history() returns [] (not None)
        # when dlt is unavailable — which would fall through to a silent debug log with no
        # user-visible warning or error count. The pre-check upgrades that path into a
        # report.warning() + report_run_history_error() so operators see the issue.
        if not self.client.dlt_available:
            self.report.warning(
                title="Run history unavailable",
                message="dlt package is not installed. Install with: pip install dlt",
                context=pipeline_info.pipeline_name,
            )
            self.report.report_run_history_error()
            return

        start_time = self.config.run_history_config.start_time
        end_time = self.config.run_history_config.end_time
        loads = self.client.get_run_history(
            pipeline_info.pipeline_name,
            start_time=start_time,
            end_time=end_time,
        )

        if loads is None:
            # Hard failure — get_run_history already logged a warning and called
            # self.report.warning(). Increment the error counter here.
            self.report.report_run_history_error()
            return

        if not loads:
            # Legitimately empty — no runs exist in the configured time window.
            logger.debug(
                "No run history found for pipeline '%s' in the configured time window.",
                pipeline_info.pipeline_name,
            )
            return

        for load in loads:
            yield from self._emit_dpi_for_load(pipeline_info, load)

        self.report.report_run_history_loaded(len(loads))

    def _emit_dpi_for_load(
        self, pipeline_info: DltPipelineInfo, load: DltLoadInfo
    ) -> Iterable[MetadataWorkUnit]:
        """Construct and emit a DataProcessInstance for a single _dlt_loads row."""
        result = _DLT_LOAD_STATUS_MAP.get(load.status, InstanceRunResult.FAILURE)
        start_ts = int(load.inserted_at.timestamp() * 1000)

        # DataProcessInstance.from_datajob requires the V1 DataJob type.
        # This is a known platform constraint — the same pattern is used in fivetran.
        flow_urn = DataFlowUrn.create_from_ids(
            orchestrator=self.platform,
            flow_id=pipeline_info.pipeline_name,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )
        datajob_v1 = DataJobV1(
            id=load.schema_name or pipeline_info.pipeline_name,
            flow_urn=flow_urn,
            platform_instance=self.config.platform_instance,
            name=load.schema_name or pipeline_info.pipeline_name,
        )

        dpi = DataProcessInstance.from_datajob(
            datajob=datajob_v1,
            id=f"{pipeline_info.pipeline_name}_{load.load_id}",
            clone_inlets=False,
            clone_outlets=False,
        )

        for mcp in dpi.generate_mcp(
            created_ts_millis=start_ts, materialize_iolets=False
        ):
            yield mcp.as_workunit()

        for mcp in dpi.start_event_mcp(start_ts):
            yield mcp.as_workunit()

        for mcp in dpi.end_event_mcp(
            end_timestamp_millis=start_ts,
            result=result,
            result_type=self.platform,
        ):
            yield mcp.as_workunit()

    # ------------------------------------------------------------------
    # Test connection
    # ------------------------------------------------------------------

    @staticmethod
    def _check_pipelines_dir(pipelines_dir: str) -> Optional[CapabilityReport]:
        """Validate that pipelines_dir exists and is a directory.

        Returns a CapabilityReport with capable=False if invalid, or None if valid.
        """
        pipelines_path = Path(pipelines_dir)
        if not pipelines_path.exists():
            return CapabilityReport(
                capable=False,
                failure_reason=(
                    f"pipelines_dir '{pipelines_dir}' does not exist. "
                    "Run a dlt pipeline first to create it."
                ),
            )
        if not pipelines_path.is_dir():
            return CapabilityReport(
                capable=False,
                failure_reason=f"pipelines_dir '{pipelines_dir}' is not a directory.",
            )
        return None

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """Verify pipelines_dir is accessible and optionally that dlt is installed."""
        report = TestConnectionReport()
        try:
            config = DltSourceConfig.model_validate(config_dict)

            dir_error = DltSource._check_pipelines_dir(config.pipelines_dir)
            if dir_error is not None:
                report.basic_connectivity = dir_error
                return report

            report.basic_connectivity = CapabilityReport(capable=True)
            report.capability_report = {}

            try:
                import dlt  # noqa: F401

                report.capability_report[SourceCapability.LINEAGE_COARSE] = (
                    CapabilityReport(capable=True)
                )
            except ImportError:
                # Filesystem fallback works without dlt — lineage is still capable.
                # Use mitigation_message (not failure_reason) to signal an optional improvement.
                report.capability_report[SourceCapability.LINEAGE_COARSE] = (
                    CapabilityReport(
                        capable=True,
                        mitigation_message=(
                            "dlt package not installed — using filesystem YAML fallback. "
                            "Install dlt for richer metadata: pip install dlt"
                        ),
                    )
                )

        except Exception as e:
            report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return report

    def get_report(self) -> DltSourceReport:
        return self.report
