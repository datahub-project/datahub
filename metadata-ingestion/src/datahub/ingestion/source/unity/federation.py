"""Lakehouse Federation helpers: map Unity Catalog connections to DataHub
platforms and build external dataset URN names for foreign catalogs.

A foreign catalog is a read-only mirror of an external database; its schemas and
tables map 1:1 to the remote system, so only the database prefix differs by
connector. See docs: CREATE FOREIGN CATALOG OPTIONS (database / dataProjectId / catalog).
"""

from dataclasses import dataclass
from typing import Dict, List, Optional

from databricks.sdk.service.catalog import ConnectionType

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.structured import (
    StructuredPropertyDefinition,
)
from datahub.metadata.schema_classes import ChangeTypeClass, PropertyValueClass
from datahub.metadata.urns import (
    ContainerUrn,
    DataTypeUrn,
    EntityTypeUrn,
    StructuredPropertyUrn,
)

# Unity Catalog connection type -> DataHub platform id.
CONNECTION_TYPE_TO_PLATFORM: Dict[ConnectionType, str] = {
    ConnectionType.MYSQL: "mysql",
    ConnectionType.POSTGRESQL: "postgres",
    ConnectionType.SQLSERVER: "mssql",
    ConnectionType.SQLDW: "mssql",
    ConnectionType.SNOWFLAKE: "snowflake",
    ConnectionType.REDSHIFT: "redshift",
    ConnectionType.BIGQUERY: "bigquery",
    ConnectionType.GLUE: "glue",
    ConnectionType.ORACLE: "oracle",
    ConnectionType.TERADATA: "teradata",
    ConnectionType.DATABRICKS: "databricks",
    ConnectionType.HIVE_METASTORE: "hive",
}

# Key in the foreign catalog's `options` holding the remote database/catalog name.
# None => two-tier namespace (no database segment in the external URN).
DATABASE_OPTION_KEY: Dict[ConnectionType, Optional[str]] = {
    ConnectionType.MYSQL: None,
    ConnectionType.POSTGRESQL: "database",
    ConnectionType.SQLSERVER: "database",
    ConnectionType.SQLDW: "database",
    ConnectionType.SNOWFLAKE: "database",
    ConnectionType.REDSHIFT: "database",
    ConnectionType.ORACLE: "database",
    ConnectionType.TERADATA: "database",
    ConnectionType.BIGQUERY: "dataProjectId",
    ConnectionType.DATABRICKS: "catalog",
    ConnectionType.GLUE: None,
    ConnectionType.HIVE_METASTORE: None,
}

KNOWN_FEDERATION_PLATFORMS: List[str] = sorted(
    set(CONNECTION_TYPE_TO_PLATFORM.values())
)


@dataclass
class FederationTarget:
    platform: str
    remote_database: Optional[str]  # None for two-tier platforms


def resolve_federation_target(
    connection_type: Optional[ConnectionType],
    options: Optional[Dict[str, str]],
    override_platform: Optional[str],
    override_database: Optional[str],
) -> Optional[FederationTarget]:
    """Resolve the external platform + remote database for a foreign catalog.

    Returns None when the platform cannot be determined, or when a three-tier
    connector's remote database is neither in `options` nor overridden (emitting a
    URN without it would dangle).
    """
    platform = override_platform
    if not platform and connection_type is not None:
        platform = CONNECTION_TYPE_TO_PLATFORM.get(connection_type)
    if not platform:
        return None

    if override_database:
        return FederationTarget(platform=platform, remote_database=override_database)

    # No connection type known (e.g. connections API unavailable) and no db override:
    # emit a two-tier URN rather than nothing.
    if connection_type is None:
        return FederationTarget(platform=platform, remote_database=None)

    db_key = DATABASE_OPTION_KEY.get(connection_type)
    if db_key is None:
        return FederationTarget(platform=platform, remote_database=None)

    remote_database = (options or {}).get(db_key)
    if not remote_database:
        return None
    return FederationTarget(platform=platform, remote_database=remote_database)


def external_dataset_name(target: FederationTarget, schema: str, table: str) -> str:
    if target.remote_database:
        return f"{target.remote_database}.{schema}.{table}"
    return f"{schema}.{table}"


FEDERATION_PROPERTY_SUFFIXES: List[str] = [
    "catalog_type",
    "platform",
    "connection",
    "remote_database",
]

_PROPERTY_DISPLAY: Dict[str, str] = {
    "catalog_type": "Catalog Type",
    "platform": "Federation Platform",
    "connection": "Federation Connection",
    "remote_database": "Federation Remote Database",
}

_PROPERTY_DESCRIPTION: Dict[str, str] = {
    "catalog_type": (
        "Unity Catalog catalog type (FOREIGN_CATALOG for Lakehouse Federation)."
    ),
    "platform": "DataHub platform of the external system this foreign catalog mirrors.",
    "connection": "Unity Catalog connection backing this foreign catalog.",
    "remote_database": (
        "Name of the external database/project/catalog mirrored by this catalog."
    ),
}


def structured_property_urns(namespace: str) -> Dict[str, str]:
    return {
        suffix: StructuredPropertyUrn(f"{namespace}.{suffix}").urn()
        for suffix in FEDERATION_PROPERTY_SUFFIXES
    }


def federation_property_definition_mcps(
    namespace: str,
) -> List[MetadataChangeProposalWrapper]:
    container_entity_type = EntityTypeUrn(f"datahub.{ContainerUrn.ENTITY_TYPE}").urn()
    value_type = DataTypeUrn("datahub.string").urn()
    mcps: List[MetadataChangeProposalWrapper] = []
    for suffix in FEDERATION_PROPERTY_SUFFIXES:
        qualified_name = f"{namespace}.{suffix}"
        allowed_values = None
        if suffix == "platform":
            allowed_values = [
                PropertyValueClass(value=platform)
                for platform in KNOWN_FEDERATION_PLATFORMS
            ]
        aspect = StructuredPropertyDefinition(
            qualifiedName=qualified_name,
            displayName=_PROPERTY_DISPLAY[suffix],
            description=_PROPERTY_DESCRIPTION[suffix],
            valueType=value_type,
            entityTypes=[container_entity_type],
            cardinality="SINGLE",
            allowedValues=allowed_values,
        )
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=StructuredPropertyUrn(qualified_name).urn(),
                aspect=aspect,
                changeType=ChangeTypeClass.CREATE,
                headers={"If-None-Match": "*"},
            )
        )
    return mcps
