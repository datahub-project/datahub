"""Container generation utilities for Dataplex source."""

import logging
from typing import Iterable, Optional

from datahub.emitter.mcp_builder import ContainerKey, ProjectIdKey
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import (
    make_bigquery_dataset_container_key,
)
from datahub.ingestion.source.sql.sql_utils import (
    gen_database_container,
    gen_schema_container,
)

logger = logging.getLogger(__name__)


def track_bigquery_container(
    project_id: str,
    dataset_id: str,
    bq_containers: dict[str, set[str]],
    config: DataplexConfig,
) -> Optional[str]:
    """Track BigQuery dataset for container creation and return container URN.

    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID (format: project.dataset)
        bq_containers: Dictionary mapping project IDs to sets of dataset IDs
        config: Dataplex configuration object

    Returns:
        Container URN if BigQuery, None otherwise
    """
    if project_id not in bq_containers:
        bq_containers[project_id] = set()
    bq_containers[project_id].add(dataset_id)

    bq_dataset_container_key = make_bigquery_dataset_container_key(
        project_id=project_id,
        dataset_id=dataset_id,
        platform="bigquery",
        env=config.env,
    )
    return bq_dataset_container_key.as_urn()


def gen_bigquery_project_container(
    project_id: str, config: DataplexConfig
) -> Iterable[MetadataWorkUnit]:
    """Generate BigQuery project container entity.

    Args:
        project_id: GCP project ID
        config: Dataplex configuration object

    Yields:
        MetadataWorkUnit for the project container
    """
    database_container_key = ProjectIdKey(
        project_id=project_id,
        platform="bigquery",
        env=config.env,
        backcompat_env_as_instance=True,
    )

    yield from gen_database_container(
        database=project_id,
        database_container_key=database_container_key,
        sub_types=[DatasetContainerSubTypes.BIGQUERY_PROJECT],
        name=project_id,
        qualified_name=project_id,
    )


def gen_bigquery_dataset_container(
    project_id: str, dataset_id: str, config: DataplexConfig
) -> Iterable[MetadataWorkUnit]:
    """Generate BigQuery dataset container entity.

    Args:
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        config: Dataplex configuration object

    Yields:
        MetadataWorkUnit for the dataset container
    """
    database_container_key: ContainerKey = ProjectIdKey(
        project_id=project_id,
        platform="bigquery",
        env=config.env,
        backcompat_env_as_instance=True,
    )

    schema_container_key = make_bigquery_dataset_container_key(
        project_id=project_id,
        dataset_id=dataset_id,
        platform="bigquery",
        env=config.env,
    )

    yield from gen_schema_container(
        database=project_id,
        schema=dataset_id,
        qualified_name=f"{project_id}.{dataset_id}",
        sub_types=[DatasetContainerSubTypes.BIGQUERY_DATASET],
        schema_container_key=schema_container_key,
        database_container_key=database_container_key,
    )


def gen_bigquery_containers(
    project_id: str, bq_containers: dict[str, set[str]], config: DataplexConfig
) -> Iterable[MetadataWorkUnit]:
    """Generate BigQuery container entities for a project.

    Creates project container and dataset containers for all datasets
    discovered from Dataplex entities.

    Args:
        project_id: GCP project ID
        bq_containers: Dictionary mapping project IDs to sets of dataset IDs
        config: Dataplex configuration object

    Yields:
        MetadataWorkUnit objects for containers
    """
    datasets = bq_containers.get(project_id, set())
    if not datasets:
        return

    logger.info(
        f"Creating BigQuery containers for project {project_id}: {len(datasets)} datasets"
    )

    # Emit project container first
    yield from gen_bigquery_project_container(project_id, config)

    # Emit dataset containers
    for dataset_id in sorted(datasets):  # Sort for deterministic order
        yield from gen_bigquery_dataset_container(project_id, dataset_id, config)
