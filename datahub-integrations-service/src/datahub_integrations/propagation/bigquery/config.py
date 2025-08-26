from typing import List, Optional

from datahub.configuration.common import AllowDenyPattern, PermissiveConfigModel
from datahub.ingestion.source.bigquery_v2.bigquery_connection import (
    BigQueryConnectionConfig,
)
from pydantic import Field


class BigqueryConnectionConfigPermissive(
    BigQueryConnectionConfig, PermissiveConfigModel
):
    taxonomy: str = Field(
        default="DataHub", description="DataHub Synced Glossary Terms' Taxonomy"
    )

    taxonomy_project_id: Optional[str] = Field(
        default=None,
        description="The project Id where the Taxonomy should be created. It defaults to the service account's project_id if not provided.",
    )

    project_ids: List[str] = Field(
        default_factory=list,
        description=(
            "Sync only specified project_ids. Use this property if you want to specify what projects to sync"
            "Overrides `project_id_pattern`."
        ),
    )

    project_id_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for project_id to filter in sync.",
    )

    dataset_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for dataset to filter in sync. Specify regex to only match the schema name. "
        "e.g. to match all tables in schema analytics, use the regex 'analytics'",
    )
