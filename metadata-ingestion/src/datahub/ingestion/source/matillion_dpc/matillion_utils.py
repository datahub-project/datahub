import logging
from typing import Dict, Optional

from datahub.emitter.mce_builder import (
    datahub_guid,
    make_container_urn,
    make_data_flow_urn,
    make_data_job_urn_with_flow,
    make_data_process_instance_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.source.matillion_dpc.config import MatillionSourceConfig
from datahub.ingestion.source.matillion_dpc.constants import (
    MATILLION_PLATFORM,
)
from datahub.ingestion.source.matillion_dpc.models import (
    MatillionDatasetInfo,
    MatillionEnvironment,
    MatillionPipeline,
    MatillionProject,
)

logger = logging.getLogger(__name__)

# Pipeline file suffixes for name normalization
PIPELINE_FILE_SUFFIXES = [".orch.yaml", ".tran.yaml", ".yaml", ".yml"]


# Standalone utility functions


def extract_base_pipeline_name(job_name: str) -> str:
    """
    Extract base pipeline name from job name with path and extension.

    Examples:
        "folder/pipeline.tran.yaml" -> "pipeline"
        "pipeline.orch.yaml" -> "pipeline"
        "simple-pipeline" -> "simple-pipeline"
    """
    # Extract filename from path
    base_name = job_name.split("/")[-1] if "/" in job_name else job_name

    # Strip pipeline file extensions
    for suffix in PIPELINE_FILE_SUFFIXES:
        if base_name.endswith(suffix):
            return base_name[: -len(suffix)]
    return base_name


def normalize_pipeline_name(pipeline_name: str) -> str:
    """
    Normalize pipeline name by stripping file extensions.

    Examples:
        "pipeline.tran.yaml" -> "pipeline"
        "pipeline" -> "pipeline"
    """
    for suffix in PIPELINE_FILE_SUFFIXES:
        if pipeline_name.endswith(suffix):
            return pipeline_name[: -len(suffix)]
    return pipeline_name


def match_pipeline_name(published_name: str, job_name: str) -> bool:
    """
    Check if a published pipeline name matches a job name from lineage events.

    Handles various formats:
    - Exact match
    - With/without path prefixes
    - With/without file extensions
    """
    if published_name == job_name:
        return True

    base_job_name = extract_base_pipeline_name(job_name)
    if published_name == base_job_name:
        return True

    normalized_job = normalize_pipeline_name(job_name)
    if published_name == normalized_job:
        return True

    return False


def make_step_dpi_urn(
    config: MatillionSourceConfig,
    project_id: str,
    pipeline_name: str,
    execution_id: str,
    step_id: str,
) -> str:
    """
    Generate DataProcessInstance URN for a step execution.

    Args:
        config: Matillion source configuration
        project_id: Matillion project ID
        pipeline_name: Pipeline name
        execution_id: Pipeline execution ID
        step_id: Step ID within the execution

    Returns:
        DataProcessInstance URN
    """
    dpi_id = datahub_guid(
        {
            "platform": MATILLION_PLATFORM,
            "instance": config.platform_instance,
            "env": config.env,
            "project_id": project_id,
            "pipeline_name": pipeline_name,
            "execution_id": execution_id,
            "step_id": step_id,
        }
    )
    return make_data_process_instance_urn(dpi_id)


def build_data_job_custom_properties(
    pipeline: MatillionPipeline, project: MatillionProject
) -> Dict[str, str]:
    custom_properties = {
        "project_id": project.id,
    }

    if hasattr(pipeline, "id") and pipeline.id:
        custom_properties["pipeline_id"] = pipeline.id
    if hasattr(pipeline, "published_time") and pipeline.published_time:
        custom_properties["published_time"] = pipeline.published_time.isoformat()

    return custom_properties


def make_dataset_urn_from_matillion_dataset(dataset: MatillionDatasetInfo) -> str:
    return make_dataset_urn_with_platform_instance(
        platform=dataset.platform,
        name=dataset.name,
        env=dataset.env,
        platform_instance=dataset.platform_instance,
    )


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
