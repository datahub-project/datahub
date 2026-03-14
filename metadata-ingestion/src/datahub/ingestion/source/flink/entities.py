import logging
from typing import Dict, Iterable, List, Optional

from datahub.api.entities.dataprocess.dataprocess_instance import (
    DataProcessInstance,
    InstanceRunResult,
)
from datahub.emitter.mce_builder import make_dataset_urn_with_platform_instance
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.flink.client import (
    FlinkCheckpointConfig,
    FlinkJobDetail,
)
from datahub.ingestion.source.flink.config import FlinkSourceConfig
from datahub.ingestion.source.flink.lineage import ClassifiedNode, LineageResult
from datahub.metadata.schema_classes import DataProcessTypeClass
from datahub.metadata.urns import DataJobUrn, DatasetUrn
from datahub.sdk._shared import DatasetUrnOrStr
from datahub.sdk.dataflow import DataFlow
from datahub.sdk.datajob import DataJob
from datahub.sdk.dataset import Dataset

logger = logging.getLogger(__name__)


def compute_dataset_urns(
    nodes: List[ClassifiedNode],
    config: FlinkSourceConfig,
) -> List[DatasetUrnOrStr]:
    """Build dataset URN strings from classified lineage nodes using platform_instance_map."""
    urns: List[DatasetUrnOrStr] = []
    for node in nodes:
        if not node.dataset_name or not node.platform:
            continue
        platform_instance = (
            config.platform_instance_map.get(node.platform)
            if config.platform_instance_map
            else None
        )
        urns.append(
            make_dataset_urn_with_platform_instance(
                platform=node.platform,
                name=node.dataset_name,
                platform_instance=platform_instance,
                env=config.env,
            )
        )
    return urns


def materialize_dataset_workunits(
    urns: List[DatasetUrnOrStr],
) -> List[MetadataWorkUnit]:
    """Emit key aspects for lineage datasets so they exist in DataHub.

    The new SDK DataJob does not auto-materialize inlet/outlet datasets
    (unlike the legacy API). We emit minimal Dataset entities so the
    UI can render lineage edges to these entities.
    """
    workunits: List[MetadataWorkUnit] = []
    for urn_str in urns:
        dataset_urn = DatasetUrn.from_string(str(urn_str))
        dataset = Dataset(
            platform=str(dataset_urn.platform),
            name=dataset_urn.name,
            env=dataset_urn.env,
        )
        workunits.extend(dataset.as_workunits())
    return workunits


class FlinkEntityBuilder:
    """Constructs DataHub entities from Flink job metadata."""

    def __init__(self, config: FlinkSourceConfig) -> None:
        self.config = config

    def _job_url(self, jid: str) -> str:
        return f"{self.config.connection.rest_api_url}/#/jobs/{jid}"

    def build_dataflow(
        self,
        job_detail: FlinkJobDetail,
        checkpoint_config: Optional[FlinkCheckpointConfig],
        flink_version: str,
    ) -> DataFlow:
        custom_props: Dict[str, str] = {
            "flink_job_id": job_detail.jid,
            "job_state": job_detail.state,
            "flink_version": flink_version,
        }
        if job_detail.job_type:
            custom_props["job_type"] = job_detail.job_type
        if job_detail.max_parallelism is not None and job_detail.max_parallelism > 0:
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
            if checkpoint_config.externalized_enabled is not None:
                custom_props["externalized_checkpoints"] = str(
                    checkpoint_config.externalized_enabled
                ).lower()

        return DataFlow(
            platform="flink",
            name=job_detail.name,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
            display_name=job_detail.name,
            description=f"Flink {job_detail.job_type or 'streaming'} job",
            external_url=self._job_url(job_detail.jid),
            custom_properties=custom_props,
        )

    def build_datajob(
        self,
        dataflow: DataFlow,
        job_detail: FlinkJobDetail,
        lineage_result: LineageResult,
    ) -> DataJob:
        """Build a single coalesced DataJob for the entire job (operator_granularity='job')."""
        inlets = compute_dataset_urns(lineage_result.sources, self.config)
        outlets = compute_dataset_urns(lineage_result.sinks, self.config)

        return DataJob(
            name=job_detail.name,
            flow=dataflow,
            platform_instance=self.config.platform_instance,
            display_name=job_detail.name,
            description=f"Coalesced Flink job operators for {job_detail.name}",
            external_url=self._job_url(job_detail.jid),
            custom_properties={
                "flink_job_id": job_detail.jid,
                "operator_count": str(len(job_detail.plan_nodes)),
            },
            inlets=inlets or None,
            outlets=outlets or None,
        )

    def build_datajobs_per_vertex(
        self,
        dataflow: DataFlow,
        job_detail: FlinkJobDetail,
        lineage_result: LineageResult,
    ) -> List[DataJob]:
        """Build one DataJob per plan node (operator_granularity='vertex')."""
        # Index lineage by node_id for source/sink lookup
        source_by_node: Dict[str, ClassifiedNode] = {
            n.node_id: n for n in lineage_result.sources
        }
        sink_by_node: Dict[str, ClassifiedNode] = {
            n.node_id: n for n in lineage_result.sinks
        }

        jobs = []
        for node in job_detail.plan_nodes:
            inlets: List[DatasetUrnOrStr] = []
            outlets: List[DatasetUrnOrStr] = []

            if node.id in source_by_node:
                inlets = compute_dataset_urns([source_by_node[node.id]], self.config)
            if node.id in sink_by_node:
                outlets = compute_dataset_urns([sink_by_node[node.id]], self.config)

            vertex_name = f"{job_detail.name}_{node.id}"
            jobs.append(
                DataJob(
                    name=vertex_name,
                    flow=dataflow,
                    platform_instance=self.config.platform_instance,
                    display_name=vertex_name,
                    external_url=self._job_url(job_detail.jid),
                    custom_properties={
                        "flink_job_id": job_detail.jid,
                        "vertex_id": node.id,
                        "parallelism": str(node.parallelism),
                        "operator_description": node.description[:500],
                    },
                    inlets=inlets or None,
                    outlets=outlets or None,
                )
            )
        return jobs

    def build_dpi_workunits(
        self,
        job_detail: FlinkJobDetail,
        datajob: DataJob,
        lineage_result: LineageResult,
    ) -> Iterable[MetadataWorkUnit]:
        if job_detail.job_type == "BATCH":
            process_type = DataProcessTypeClass.BATCH_SCHEDULED
        else:
            process_type = DataProcessTypeClass.STREAMING

        inlets = compute_dataset_urns(lineage_result.sources, self.config)
        outlets = compute_dataset_urns(lineage_result.sinks, self.config)

        dpi = DataProcessInstance(
            id=f"{job_detail.name}_{job_detail.start_time}",
            orchestrator="flink",
            cluster=self.config.env,
            type=process_type,
            template_urn=DataJobUrn.from_string(str(datajob.urn)),
            properties={
                "flink_job_id": job_detail.jid,
                "job_state": job_detail.state,
            },
            inlets=[DatasetUrn.from_string(u) for u in inlets],
            outlets=[DatasetUrn.from_string(u) for u in outlets],
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
        if job_detail.state in state_to_result and job_detail.end_time > 0:
            for mcp in dpi.end_event_mcp(
                end_timestamp_millis=job_detail.end_time,
                result=state_to_result[job_detail.state],
                result_type="flink",
                start_timestamp_millis=start_ts,
            ):
                yield mcp.as_workunit()
