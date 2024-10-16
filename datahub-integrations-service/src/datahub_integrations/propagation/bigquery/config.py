from datahub.configuration.common import PermissiveConfigModel
from datahub.ingestion.source.bigquery_v2.bigquery_config import (
    BigQueryConnectionConfig,
)
from pydantic import Field


class BigqueryConnectionConfigPermissive(
    BigQueryConnectionConfig, PermissiveConfigModel
):

    taxonomy: str = Field(
        default="DataHub", description="DataHub Synced Glossary Terms"
    )
