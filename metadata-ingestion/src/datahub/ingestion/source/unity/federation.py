"""Lakehouse Federation helpers: map Unity Catalog connections to DataHub
platforms and build external dataset URN names for foreign catalogs.

A foreign catalog is a read-only mirror of an external database; its schemas and
tables mirror the remote system's names 1:1. Connectors differ only in whether a
database segment is present (two- vs three-tier) and which OPTIONS key carries it
(database / dataProjectId / catalog). See docs: CREATE FOREIGN CATALOG OPTIONS.
"""

from dataclasses import dataclass
from typing import Dict, FrozenSet, List, Optional

from databricks.sdk.service.catalog import ConnectionType

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.com.linkedin.pegasus2avro.structured import (
    StructuredPropertyDefinition,
)
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    PropertyValueClass,
)
from datahub.metadata.urns import (
    ContainerUrn,
    DataTypeUrn,
    EntityTypeUrn,
    StructuredPropertyUrn,
)


@dataclass(frozen=True)
class ConnectorMapping:
    """DataHub platform and the foreign-catalog OPTIONS key that carries the remote
    database for one Unity Catalog connection type.

    database_option_key is None for two-tier platforms (no database segment in the
    external URN); otherwise it names the OPTIONS key holding the remote database.
    """

    platform: str
    database_option_key: Optional[str]


# Single source of truth per connection type: adding a connector means adding one
# entry here, so platform and database-key can never drift out of sync.
CONNECTION_TYPE_MAP: Dict[ConnectionType, ConnectorMapping] = {
    ConnectionType.MYSQL: ConnectorMapping("mysql", None),
    ConnectionType.POSTGRESQL: ConnectorMapping("postgres", "database"),
    ConnectionType.SQLSERVER: ConnectorMapping("mssql", "database"),
    ConnectionType.SQLDW: ConnectorMapping("mssql", "database"),
    ConnectionType.SNOWFLAKE: ConnectorMapping("snowflake", "database"),
    ConnectionType.REDSHIFT: ConnectorMapping("redshift", "database"),
    ConnectionType.BIGQUERY: ConnectorMapping("bigquery", "dataProjectId"),
    ConnectionType.GLUE: ConnectorMapping("glue", None),
    # Oracle's foreign catalog OPTIONS key is `service_name`, not `database`.
    ConnectionType.ORACLE: ConnectorMapping("oracle", "service_name"),
    ConnectionType.TERADATA: ConnectorMapping("teradata", "database"),
    ConnectionType.DATABRICKS: ConnectorMapping("databricks", "catalog"),
    # Salesforce Data Cloud names its remote segment `dataspace`.
    ConnectionType.SALESFORCE_DATA_CLOUD: ConnectorMapping("salesforce", "dataspace"),
    ConnectionType.HIVE_METASTORE: ConnectorMapping("hive", None),
}

KNOWN_FEDERATION_PLATFORMS: List[str] = sorted(
    {m.platform for m in CONNECTION_TYPE_MAP.values()}
)

# Platforms whose external URN is two-tier (schema.table, no database segment).
# Used to decide the fallback when the connection type could not be determined.
PLATFORMS_WITHOUT_DATABASE_SEGMENT: FrozenSet[str] = frozenset(
    m.platform for m in CONNECTION_TYPE_MAP.values() if m.database_option_key is None
)


@dataclass(frozen=True)
class FederationTarget:
    platform: str
    remote_database: Optional[str]  # None for two-tier platforms


def resolve_federation_target(
    connection_type: Optional[ConnectionType],
    *,
    options: Optional[Dict[str, str]],
    override_platform: Optional[str],
    override_database: Optional[str],
) -> Optional[FederationTarget]:
    """Resolve the external platform + remote database for a foreign catalog.

    Returns None when the platform cannot be determined, or when a three-tier
    connector's remote database is neither in `options` nor overridden (emitting a
    URN without it would dangle).
    """
    mapping = (
        CONNECTION_TYPE_MAP.get(connection_type)
        if connection_type is not None
        else None
    )
    platform = override_platform or (mapping.platform if mapping else None)
    if not platform:
        return None

    if override_database:
        return FederationTarget(platform=platform, remote_database=override_database)

    if mapping is not None:
        if mapping.database_option_key is None:
            return FederationTarget(platform=platform, remote_database=None)
        remote_database = (options or {}).get(mapping.database_option_key)
        if not remote_database:
            return None
        return FederationTarget(platform=platform, remote_database=remote_database)

    # Connection type unknown (e.g. the connections API was unavailable) and no
    # database override. Only emit a two-tier URN when the (overridden) platform is
    # inherently two-tier; for a three-tier or unrecognized platform the URN would
    # be missing its database segment and dangle, so return None and let the caller
    # warn and skip.
    if platform in PLATFORMS_WITHOUT_DATABASE_SEGMENT:
        return FederationTarget(platform=platform, remote_database=None)
    return None


def external_dataset_name(target: FederationTarget, schema: str, table: str) -> str:
    if target.remote_database:
        return f"{target.remote_database}.{schema}.{table}"
    return f"{schema}.{table}"


# Structured-property suffixes (the last segment of each qualified name). Named
# constants so producers (source.py) and definitions below share one spelling.
FEDERATION_PROP_CATALOG_TYPE = "catalog_type"
FEDERATION_PROP_PLATFORM = "platform"
FEDERATION_PROP_CONNECTION = "connection"
FEDERATION_PROP_REMOTE_DATABASE = "remote_database"


@dataclass(frozen=True)
class FederationProperty:
    suffix: str
    display_name: str
    description: str
    # Only the platform property constrains its values to the known platform list.
    platform_allowed_values: bool = False


# One record per property; the definition builder iterates this so display name and
# description can never desync from the suffix list.
FEDERATION_PROPERTIES: List[FederationProperty] = [
    FederationProperty(
        FEDERATION_PROP_CATALOG_TYPE,
        "Catalog Type",
        "Unity Catalog catalog type (FOREIGN_CATALOG for Lakehouse Federation).",
    ),
    FederationProperty(
        FEDERATION_PROP_PLATFORM,
        "Federation Platform",
        "DataHub platform of the external system this foreign catalog mirrors.",
        platform_allowed_values=True,
    ),
    FederationProperty(
        FEDERATION_PROP_CONNECTION,
        "Federation Connection",
        "Unity Catalog connection backing this foreign catalog.",
    ),
    FederationProperty(
        FEDERATION_PROP_REMOTE_DATABASE,
        "Federation Remote Database",
        "Name of the external database/project/catalog mirrored by this catalog.",
    ),
]


def federation_property_urn(namespace: str, suffix: str) -> StructuredPropertyUrn:
    return StructuredPropertyUrn(f"{namespace}.{suffix}")


def federation_property_definition_mcps(
    namespace: str,
) -> List[MetadataChangeProposalWrapper]:
    container_entity_type = EntityTypeUrn(f"datahub.{ContainerUrn.ENTITY_TYPE}").urn()
    value_type = DataTypeUrn("datahub.string").urn()
    mcps: List[MetadataChangeProposalWrapper] = []
    for prop in FEDERATION_PROPERTIES:
        allowed_values = (
            [
                PropertyValueClass(value=platform)
                for platform in KNOWN_FEDERATION_PLATFORMS
            ]
            if prop.platform_allowed_values
            else None
        )
        aspect = StructuredPropertyDefinition(
            qualifiedName=f"{namespace}.{prop.suffix}",
            displayName=prop.display_name,
            description=prop.description,
            valueType=value_type,
            entityTypes=[container_entity_type],
            cardinality="SINGLE",
            allowedValues=allowed_values,
        )
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=federation_property_urn(namespace, prop.suffix).urn(),
                aspect=aspect,
                changeType=ChangeTypeClass.CREATE,
                headers={"If-None-Match": "*"},
            )
        )
    return mcps


def identity_column_lineage(
    dataset_urn: str,
    external_urn: str,
    downstream_field_paths: List[str],
    upstream_field_paths: List[str],
) -> List[FineGrainedLineageClass]:
    """1:1 identity column-level lineage from an external dataset's fields to a
    foreign-catalog mirror's fields. A foreign catalog is a read-only copy, so
    each column maps to the same-named external column. Matched case-insensitively
    because the mirror and the external source may differ only in field-name case
    (e.g. Databricks CUSTOMER_ID vs a lowercased Snowflake customer_id). Downstream
    fields with no external match are skipped."""
    upstream_by_casefold = {p.casefold(): p for p in upstream_field_paths}
    result: List[FineGrainedLineageClass] = []
    for downstream_path in downstream_field_paths:
        upstream_path = upstream_by_casefold.get(downstream_path.casefold())
        if upstream_path is None:
            continue
        result.append(
            FineGrainedLineageClass(
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[make_schema_field_urn(external_urn, upstream_path)],
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[make_schema_field_urn(dataset_urn, downstream_path)],
            )
        )
    return result
