import logging
from datetime import datetime
from typing import Dict, List

from datahub.emitter.mce_builder import (
    datahub_guid,
    make_data_process_instance_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.source.matillion_dpc.config import MatillionSourceConfig
from datahub.ingestion.source.matillion_dpc.constants import (
    MATILLION_PLATFORM,
)
from datahub.ingestion.source.matillion_dpc.models import (
    MatillionDatasetInfo,
    MatillionPipeline,
    MatillionProject,
)

logger = logging.getLogger(__name__)

# Pipeline file suffixes for name normalization
PIPELINE_FILE_SUFFIXES = [".orch.yaml", ".tran.yaml", ".yaml", ".yml"]


# Standalone utility functions


def parse_iso_timestamp(timestamp_str: str) -> datetime:
    """Parse an ISO timestamp, tolerating Matillion's non-standard microsecond precision.

    The Matillion API sometimes returns timestamps with fewer than 6 microsecond
    digits (e.g. "2026-02-02T22:53:51.41235+00:00"), which `fromisoformat` rejects,
    so the fractional part is padded/truncated to exactly 6 digits first.
    """
    normalized = timestamp_str.replace("Z", "+00:00")

    if "." in normalized and "+" in normalized:
        parts = normalized.split(".")
        if len(parts) == 2:
            microseconds_and_tz = parts[1]
            if "+" in microseconds_and_tz:
                microseconds, tz = microseconds_and_tz.split("+")
                microseconds = microseconds.ljust(6, "0")[:6]
                normalized = f"{parts[0]}.{microseconds}+{tz}"
            elif "-" in microseconds_and_tz:
                microseconds, tz = microseconds_and_tz.rsplit("-", 1)
                microseconds = microseconds.ljust(6, "0")[:6]
                normalized = f"{parts[0]}.{microseconds}-{tz}"

    return datetime.fromisoformat(normalized)


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


def extract_pipeline_file_name(job_name: str) -> str:
    """Return the leaf file name, dropping any folder path.

    The console observability search matches the file name, not the folder path.
    """
    return job_name.split("/")[-1]


def extract_folder_segments(job_name: str) -> List[str]:
    """Return the folder path segments that precede the pipeline file name.

    Examples:
        "ingest/staging/orders/load.orch.yaml" ->
            ["ingest", "staging", "orders"]
        "pipeline.orch.yaml" -> []
    """
    if "/" not in job_name:
        return []
    return [segment for segment in job_name.split("/")[:-1] if segment]


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


def make_execution_dpi_urn(
    config: MatillionSourceConfig,
    project_id: str,
    pipeline_name: str,
    execution_id: str,
) -> str:
    dpi_id = datahub_guid(
        {
            "platform": MATILLION_PLATFORM,
            "instance": config.platform_instance,
            "env": config.env,
            "project_id": project_id,
            "pipeline_name": pipeline_name,
            "execution_id": execution_id,
            "entity": "execution",
        }
    )
    return make_data_process_instance_urn(dpi_id)


def build_data_job_custom_properties(
    pipeline: MatillionPipeline, project: MatillionProject
) -> Dict[str, str]:
    custom_properties = {
        "project_id": project.id,
    }

    if pipeline.published_time:
        custom_properties["published_time"] = pipeline.published_time.isoformat()

    return custom_properties


def make_dataset_urn_from_matillion_dataset(dataset: MatillionDatasetInfo) -> str:
    return make_dataset_urn_with_platform_instance(
        platform=dataset.platform,
        name=dataset.name,
        env=dataset.env,
        platform_instance=dataset.platform_instance,
    )
