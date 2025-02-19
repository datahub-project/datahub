from typing import Optional

from datahub.configuration.common import PermissiveConfigModel
from datahub.ingestion.source.bigquery_v2.bigquery_config import (
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
