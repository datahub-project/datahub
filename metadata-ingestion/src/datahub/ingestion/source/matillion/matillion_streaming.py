import logging
from typing import Iterable

from datahub.emitter.mce_builder import make_data_flow_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.matillion.config import (
    MatillionSourceConfig,
    MatillionSourceReport,
)
from datahub.ingestion.source.matillion.constants import MATILLION_PLATFORM
from datahub.ingestion.source.matillion.matillion_container import (
    MatillionContainerHandler,
)
from datahub.ingestion.source.matillion.models import (
    MatillionProject,
    MatillionStreamingPipeline,
)
from datahub.ingestion.source.matillion.urn_builder import MatillionUrnBuilder
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    StatusClass,
    TimeStampClass,
)

logger = logging.getLogger(__name__)


class MatillionStreamingHandler:
    def __init__(
        self,
        config: MatillionSourceConfig,
        report: MatillionSourceReport,
        urn_builder: MatillionUrnBuilder,
        container_handler: MatillionContainerHandler,
    ):
        self.config = config
        self.report = report
        self.urn_builder = urn_builder
        self.container_handler = container_handler

    def _make_streaming_pipeline_urn(
        self, streaming_pipeline: MatillionStreamingPipeline, project: MatillionProject
    ) -> str:
        return make_data_flow_urn(
            orchestrator=MATILLION_PLATFORM,
            flow_id=f"streaming.{streaming_pipeline.id}",
            cluster=self.config.platform_instance or project.name,
            platform_instance=self.config.platform_instance,
        )

    def emit_streaming_pipeline(
        self,
        streaming_pipeline: MatillionStreamingPipeline,
        project: MatillionProject,
    ) -> Iterable[MetadataWorkUnit]:
        pipeline_urn = self._make_streaming_pipeline_urn(streaming_pipeline, project)

        custom_properties = {
            "streaming_pipeline_id": streaming_pipeline.id,
            "project_id": project.id,
            "pipeline_type": "streaming",
            "description": streaming_pipeline.description or "",
        }

        if streaming_pipeline.source_type:
            custom_properties["source_type"] = streaming_pipeline.source_type
        if streaming_pipeline.target_type:
            custom_properties["target_type"] = streaming_pipeline.target_type
        if streaming_pipeline.source_connection_id:
            custom_properties["source_connection_id"] = (
                streaming_pipeline.source_connection_id
            )
        if streaming_pipeline.target_connection_id:
            custom_properties["target_connection_id"] = (
                streaming_pipeline.target_connection_id
            )
        if streaming_pipeline.status:
            custom_properties["status"] = streaming_pipeline.status

        dataflow_info = DataFlowInfoClass(
            name=streaming_pipeline.name,
            description=streaming_pipeline.description,
            customProperties=custom_properties,
            externalUrl=f"{self.config.api_config.get_base_url().rstrip('/dpc')}/streaming-pipelines/{streaming_pipeline.id}",
        )

        if streaming_pipeline.created_at:
            dataflow_info.created = TimeStampClass(
                time=int(streaming_pipeline.created_at.timestamp() * 1000),
                actor="urn:li:corpuser:datahub",
            )

        yield MetadataChangeProposalWrapper(
            entityUrn=pipeline_urn,
            aspect=dataflow_info,
        ).as_workunit()

        status_aspect = StatusClass(removed=False)
        yield MetadataChangeProposalWrapper(
            entityUrn=pipeline_urn,
            aspect=status_aspect,
        ).as_workunit()

        if self.config.extract_projects_to_containers:
            yield from self.container_handler.add_pipeline_to_container(
                pipeline_urn, project
            )

        self.report.report_streaming_pipeline_emitted()
        logger.debug(
            f"Emitted streaming pipeline: {streaming_pipeline.name} in project {project.name}"
        )
