from typing import Dict, List, Optional

import pydantic
from pydantic import Field

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class UnityCatalogStatefulIngestionConfig(StatefulStaleMetadataRemovalConfig):
    """
    Specialization of StatefulStaleMetadataRemovalConfig to adding custom config.
    This will be used to override the stateful_ingestion config param of StatefulIngestionConfigBase
    in the UnityCatalogConfig.
    """

    _entity_types: List[str] = Field(default=["dataset", "container"])


class UnityCatalogSourceConfig(StatefulIngestionConfigBase):
    token: str = pydantic.Field(description="Databricks personal access token")
    workspace_url: str = pydantic.Field(description="Databricks workspace url")
    workspace_name: str = pydantic.Field(
        default=None,
        description="Name of the workspace. Default to deployment name present in workspace_url",
    )

    metastore_id_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for metastore id to filter in ingestion.",
    )

    catalog_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for catalogs to filter in ingestion. Specify regex to match the catalog name",
    )

    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for schemas to filter in ingestion. Specify regex to only match the schema name. e.g. to match all tables in schema analytics, use the regex 'analytics'",
    )

    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to filter in ingestion. Specify regex to match the entire table name in catalog.schema.table format. e.g. to match all tables starting with customer in Customer catalog and public schema, use the regex 'Customer.public.customer.*'",
    )
    domain: Dict[str, AllowDenyPattern] = Field(
        default=dict(),
        description='Attach domains to catalogs, schemas or tables during ingestion using regex patterns. Domain key can be a guid like *urn:li:domain:ec428203-ce86-4db3-985d-5a8ee6df32ba* or a string like "Marketing".) If you provide strings, then datahub will attempt to resolve this name to a guid, and will error out if this fails. There can be multiple domain keys specified.',
    )

    include_table_lineage: Optional[bool] = pydantic.Field(
        default=True,
        description="Option to enable/disable lineage generation.",
    )

    include_column_lineage: Optional[bool] = pydantic.Field(
        default=True,
        description="Option to enable/disable lineage generation. Currently we have to call a rest call per column to get column level lineage due to the Databrick api which can slow down ingestion. ",
    )

    stateful_ingestion: Optional[UnityCatalogStatefulIngestionConfig] = pydantic.Field(
        default=None, description="Unity Catalog Stateful Ingestion Config."
    )
