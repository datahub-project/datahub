import logging
import time
from typing import Dict, Iterable, Optional

from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.flink.flink_client import (
    FlinkCheckpointConfig,
    FlinkJobDetail,
)
from datahub.ingestion.source.flink.flink_config import FlinkSourceConfig
from datahub.ingestion.source.flink.flink_lineage import LineageResult
from datahub.metadata.urns import DataJobUrn, DatasetUrn
from datahub.sdk.dataflow import DataFlow as DataFlowEntity
from datahub.sdk.datajob import DataJob as DataJobEntity

logger = logging.getLogger(__name__)


def _compute_dataset_urns(
    lineage_result: LineageResult,
    kafka_platform_instance: Optional[str],
    kafka_env: str,
) -> tuple:
    """Compute inlet/outlet dataset URN strings from lineage. Returns (inlets, outlets)."""
    inlets = [
        make_dataset_urn_with_platform_instance(
            platform="kafka",
            name=src.dataset_name,
            platform_instance=kafka_platform_instance,
            env=kafka_env,
        )
        for src in lineage_result.sources
        if src.dataset_name
    ]
    outlets = [
        make_dataset_urn_with_platform_instance(
            platform="kafka",
            name=sink.dataset_name,
            platform_instance=kafka_platform_instance,
            env=kafka_env,
        )
        for sink in lineage_result.sinks
        if sink.dataset_name
    ]
    return inlets, outlets


class FlinkEntityBuilder:
    """Constructs DataHub entities from Flink job metadata.

    Uses SDK V2 for DataFlow/DataJob entities (handles DataPlatformInstance,
    BrowsePathsV2, Status aspects automatically). Uses DataProcessInstance
    API for DPI emission (no SDK V2 equivalent).
    """

    def __init__(self, config: FlinkSourceConfig) -> None:
        self.config = config

    def build_dataflow(
        self,
        job_detail: FlinkJobDetail,
        checkpoint_config: Optional[FlinkCheckpointConfig],
        flink_version: str,
    ) -> DataFlowEntity:
        """Build DataFlow entity for a Flink job definition using SDK V2."""
        custom_props: Dict[str, str] = {
            "flink_job_id": job_detail.jid,
            "job_state": job_detail.state,
            "flink_version": flink_version,
        }
        if job_detail.job_type:
            custom_props["job_type"] = job_detail.job_type
        if job_detail.max_parallelism is not None:
            custom_props["parallelism"] = str(job_detail.max_parallelism)
        if job_detail.start_time > 0:
            custom_props["start_time"] = str(job_detail.start_time)
        if job_detail.duration > 0:
            custom_props["duration_ms"] = str(job_detail.duration)

        if checkpoint_config:
            if checkpoint_config.state_backend:
                custom_props["state_backend"] = checkpoint_config.state_backend
            if checkpoint_config.interval is not None:
                custom_props["checkpoint_interval_ms"] = str(checkpoint_config.interval)
            if checkpoint_config.mode:
                custom_props["checkpoint_mode"] = checkpoint_config.mode

        return DataFlowEntity(
            platform="flink",
            name=job_detail.name,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            display_name=job_detail.name,
            description=f"Flink {job_detail.job_type or 'streaming'} job",
            external_url=f"{self.config.rest_endpoint}/#/jobs/{job_detail.jid}",
            custom_properties=custom_props,
        )

    def build_datajob(
        self,
        dataflow: DataFlowEntity,
        job_detail: FlinkJobDetail,
        lineage_result: LineageResult,
    ) -> DataJobEntity:
        """Build DataJob entity with lineage using SDK V2 (coalesced mode)."""
        inlets, outlets = _compute_dataset_urns(
            lineage_result,
            self.config.kafka_platform_instance,
            self.config.kafka_dataset_env,
        )

        return DataJobEntity(
            name=job_detail.name,
            flow=dataflow,
            platform_instance=self.config.platform_instance,
            display_name=job_detail.name,
            description=f"Coalesced Flink job operators for {job_detail.name}",
            external_url=f"{self.config.rest_endpoint}/#/jobs/{job_detail.jid}",
            custom_properties={
                "flink_job_id": job_detail.jid,
                "operator_count": str(len(job_detail.plan_nodes)),
            },
            inlets=inlets if inlets else None,
            outlets=outlets if outlets else None,
        )

    def build_dpi_workunits(
        self,
        job_name: str,
        job_detail: FlinkJobDetail,
        datajob: DataJobEntity,
        lineage_result: LineageResult,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit DataProcessInstance for a job execution."""
        from datahub.api.entities.dataprocess.dataprocess_instance import (
            DataProcessInstance,
            InstanceRunResult,
        )
        from datahub.metadata.schema_classes import DataProcessTypeClass

        if job_detail.job_type == "BATCH":
            process_type = DataProcessTypeClass.BATCH_SCHEDULED
        else:
            process_type = DataProcessTypeClass.STREAMING

        # Reuse the same URNs computed for the DataJob, converted to proper types
        inlets, outlets = _compute_dataset_urns(
            lineage_result,
            self.config.kafka_platform_instance,
            self.config.kafka_dataset_env,
        )
        inlet_urns = [DatasetUrn.from_string(urn) for urn in inlets]
        outlet_urns = [DatasetUrn.from_string(urn) for urn in outlets]

        dpi = DataProcessInstance(
            id=f"{job_name}_{job_detail.start_time}",
            orchestrator="flink",
            cluster=self.config.cluster,
            type=process_type,
            template_urn=DataJobUrn.from_string(str(datajob.urn)),
            properties={
                "flink_job_id": job_detail.jid,
                "job_state": job_detail.state,
            },
            inlets=inlet_urns,
            outlets=outlet_urns,
            data_platform_instance=self.config.platform_instance,
        )

        start_ts = job_detail.start_time if job_detail.start_time > 0 else None
        for mcp in dpi.generate_mcp(
            created_ts_millis=start_ts,
            materialize_iolets=False,
        ):
            yield mcp.as_workunit()

        if start_ts:
            for mcp in dpi.start_event_mcp(start_timestamp_millis=start_ts):
                yield mcp.as_workunit()

        state_to_result = {
            "FINISHED": InstanceRunResult.SUCCESS,
            "FAILED": InstanceRunResult.FAILURE,
            "CANCELED": InstanceRunResult.SKIPPED,
        }
        if job_detail.state in state_to_result:
            end_ts = (
                job_detail.end_time
                if job_detail.end_time > 0
                else int(time.time() * 1000)
            )
            result = state_to_result[job_detail.state]
            for mcp in dpi.end_event_mcp(
                end_timestamp_millis=end_ts,
                result=result,
                result_type="flink",
                start_timestamp_millis=start_ts,
            ):
                yield mcp.as_workunit()
