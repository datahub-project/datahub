import logging
from typing import Iterable

from datahub.emitter.mce_builder import datahub_guid, make_data_flow_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.matillion_dpc.config import (
    MatillionSourceConfig,
    MatillionSourceReport,
)
from datahub.ingestion.source.matillion_dpc.constants import (
    API_PATH_SUFFIX,
    MATILLION_PLATFORM,
    UI_PATH_STREAMING_PIPELINES,
)
from datahub.ingestion.source.matillion_dpc.matillion_container import (
    MatillionContainerHandler,
)
from datahub.ingestion.source.matillion_dpc.matillion_utils import MatillionUrnBuilder
from datahub.ingestion.source.matillion_dpc.models import (
    MatillionProject,
    MatillionStreamingPipeline,
)
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    StatusClass,
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
        # Use GUID to ensure URN safety with special characters in pipeline names
        flow_id = datahub_guid(
            {
                "platform": MATILLION_PLATFORM,
                "instance": self.config.platform_instance,
                "env": self.config.env,
                "project_id": project.id,
                "streaming_pipeline_name": streaming_pipeline.name,
                "streaming_pipeline_id": streaming_pipeline.streaming_pipeline_id,
            }
        )
        return make_data_flow_urn(
            orchestrator=MATILLION_PLATFORM,
            flow_id=flow_id,
            cluster=self.config.env,
            platform_instance=self.config.platform_instance or project.name,
        )

    def emit_streaming_pipeline(
        self,
        streaming_pipeline: MatillionStreamingPipeline,
        project: MatillionProject,
    ) -> Iterable[MetadataWorkUnit]:
        if not self.config.streaming_pipeline_patterns.allowed(streaming_pipeline.name):
            self.report.filtered_streaming_pipelines.append(streaming_pipeline.name)
            return

        pipeline_urn = self._make_streaming_pipeline_urn(streaming_pipeline, project)
        pipeline_id = (
            streaming_pipeline.streaming_pipeline_id or streaming_pipeline.name
        )

        custom_properties = {
            "streaming_pipeline_id": pipeline_id,
            "project_id": project.id,
            "pipeline_type": "streaming",
        }

        base_url = self.config.api_config.get_base_url()
        if base_url.endswith(API_PATH_SUFFIX):
            base_url = base_url[: -len(API_PATH_SUFFIX)]

        dataflow_info = DataFlowInfoClass(
            name=streaming_pipeline.name,
            customProperties=custom_properties,
            externalUrl=f"{base_url}/{UI_PATH_STREAMING_PIPELINES}/{pipeline_id}",
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
