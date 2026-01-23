import logging
from typing import Optional

from datahub.emitter.mce_builder import (
    datahub_guid,
    make_container_urn,
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_dataplatform_instance_urn,
)
from datahub.ingestion.source.matillion_dpc.config import MatillionSourceConfig
from datahub.ingestion.source.matillion_dpc.constants import (
    MATILLION_PLATFORM,
)
from datahub.ingestion.source.matillion_dpc.models import (
    MatillionEnvironment,
    MatillionPipeline,
    MatillionProject,
)

logger = logging.getLogger(__name__)


class MatillionUrnBuilder:
    def __init__(self, config: MatillionSourceConfig):
        self.config = config

    def make_project_container_urn(self, project: MatillionProject) -> str:
        return make_container_urn(
            guid=project.id,
        )

    def make_environment_container_urn(
        self, environment: MatillionEnvironment, project: MatillionProject
    ) -> str:
        return make_container_urn(
            guid=f"{project.id}.{environment.name}",
        )

    def make_pipeline_urn(
        self, pipeline: MatillionPipeline, project: MatillionProject
    ) -> str:
        # Use GUID to ensure URN safety with special characters in pipeline names
        flow_id = datahub_guid(
            {
                "platform": MATILLION_PLATFORM,
                "instance": self.config.platform_instance,
                "env": self.config.env,
                "project_id": project.id,
                "pipeline_name": pipeline.name,
            }
        )
        return make_data_flow_urn(
            orchestrator=MATILLION_PLATFORM,
            flow_id=flow_id,
            cluster=self.config.env,
            platform_instance=self.config.platform_instance or project.name,
        )

    def make_data_job_urn(
        self, pipeline: MatillionPipeline, project: MatillionProject
    ) -> str:
        flow_urn = self.make_pipeline_urn(pipeline, project)

        # Use GUID to ensure URN safety with special characters in pipeline names
        # The job_id should be unique within the flow, but we can use the pipeline name
        # since there's a 1:1 relationship between DataFlow and DataJob for Matillion
        job_id = datahub_guid(
            {
                "platform": MATILLION_PLATFORM,
                "instance": self.config.platform_instance,
                "env": self.config.env,
                "project_id": project.id,
                "pipeline_name": pipeline.name,
                "entity_type": "job",  # Distinguish from flow_id
            }
        )

        return make_data_job_urn_with_flow(flow_urn, job_id)

    def make_platform_instance_urn(self) -> Optional[str]:
        if self.config.platform_instance:
            return make_dataplatform_instance_urn(
                platform=MATILLION_PLATFORM,
                instance=self.config.platform_instance,
            )
        return None
