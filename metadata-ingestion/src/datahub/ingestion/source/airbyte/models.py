from typing import Any, Dict, List, Optional, Sequence, Union

from pydantic import AliasChoices, BaseModel, ConfigDict, Field

from datahub.ingestion.source.airbyte.config import PlatformDetail
from datahub.utilities.str_enum import StrEnum

# Schema-name keys seen across Airbyte connector configurations. Order
# matters — more-specific keys first so the generic `"schema"` doesn't
# shadow MSSQL/Oracle's `"default_schema"`.
_SCHEMA_FIELDS: Sequence[str] = (
    "schema",
    "default_schema",
    "schema_name",
)

# Database-name keys. Destinations additionally treat BigQuery's `"dataset"`
# as the database tier — see `_DESTINATION_DATABASE_FIELDS`.
_SOURCE_DATABASE_FIELDS: Sequence[str] = (
    "database",
    "db",
    "db_name",
    "database_name",
    "dbname",
    "service_name",  # Oracle
    "sid",  # Oracle SID
    "project",  # BigQuery
    "project_id",
    "catalog",  # Databricks, Trino
    "keyspace",  # Cassandra
)

_DESTINATION_DATABASE_FIELDS: Sequence[str] = (
    *_SOURCE_DATABASE_FIELDS,
    "dataset",  # BigQuery destinations
)


def _lookup_config_field(
    config: Optional[Dict[str, Any]], fields: Sequence[str]
) -> Optional[str]:
    if not config:
        return None
    for field in fields:
        value = config.get(field)
        if value and isinstance(value, str):
            return value
    return None


class StreamIdentifier(BaseModel):
    """Hashable `(namespace, stream_name)` key used to look up streams in
    dicts keyed by stream identity (e.g. propertyFields lookup)."""

    stream_name: str
    namespace: str

    model_config = ConfigDict(frozen=True)

    def __str__(self) -> str:
        return (
            f"{self.namespace}.{self.stream_name}"
            if self.namespace
            else self.stream_name
        )

    def __hash__(self) -> int:
        return hash((self.stream_name, self.namespace))


class PropertyFieldPath(BaseModel):
    """A dotted path to a (possibly nested) Airbyte field, e.g.
    `["address", "city"]` for `address.city`. We only consume the leaf
    name for column-level lineage; the rest is preserved for callers that
    care about nesting."""

    path: List[str]

    @property
    def field_name(self) -> str:
        return self.path[-1] if self.path else ""

    def __str__(self) -> str:
        return ".".join(self.path)


class AirbyteStream(BaseModel):
    name: str
    namespace: Optional[str] = Field(None, alias="namespace")
    json_schema: Dict[str, Any] = Field({}, alias="jsonSchema")

    model_config = ConfigDict(populate_by_name=True)


class AirbyteStreamConfig(BaseModel):
    stream: AirbyteStream
    config: Dict[str, Any] = {}

    def is_enabled(self) -> bool:
        if not self.config:
            return True
        if self.config.get("syncMode") == "null":
            return False
        if self.config.get("selected") is False:
            return False
        return True

    def is_field_selected(self, field_name: str) -> bool:
        # Default to selected when no fieldSelection mapping is supplied.
        if not self.config:
            return True
        field_selection = self.config.get("fieldSelection", {})
        return field_selection.get(field_name) is not False

    def get_destination_namespace(self) -> Optional[str]:
        if not self.config:
            return None
        return self.config.get("destinationNamespace")

    model_config = ConfigDict(populate_by_name=True)


class AirbyteSyncCatalog(BaseModel):
    streams: List[AirbyteStreamConfig] = []

    model_config = ConfigDict(populate_by_name=True)


class AirbyteSourceConfiguration(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    schemas: List[str] = Field(default_factory=list)
    schema_name: Optional[str] = Field(None, alias="schema")
    username: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True, extra="allow")


class AirbyteDestinationConfiguration(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    schema_name: Optional[str] = Field(None, alias="schema")
    username: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True, extra="allow")


class AirbyteSourcePartial(BaseModel):
    """Airbyte source representation tolerant of missing optional fields —
    used for both list and get endpoints across OSS / Cloud / Public API
    versions, which return slightly different field sets."""

    source_id: str = Field(alias="sourceId")
    name: Optional[str] = None
    source_type: Optional[str] = Field(None, alias="sourceType")
    source_definition_id: Optional[str] = Field(
        None,
        validation_alias=AliasChoices("definitionId", "sourceDefinitionId"),
    )
    configuration: Optional[Dict[str, Any]] = None
    workspace_id: Optional[str] = Field(None, alias="workspaceId")
    created_at: Optional[int] = Field(None, alias="createdAt")

    model_config = ConfigDict(populate_by_name=True)

    def get_schema_for_table(self, table_name: str) -> Optional[str]:
        # Some connectors (notably MSSQL) carry per-table schema overrides
        # in `configuration.tables`; this exposes the lookup for callers
        # that need to honour the override.
        if not self.configuration:
            return None
        tables = self.configuration.get("tables")
        if tables and isinstance(tables, list):
            for table in tables:
                if isinstance(table, dict) and table.get("name") == table_name:
                    return table.get("schema")
        return None

    @property
    def get_schema(self) -> Optional[str]:
        schema = _lookup_config_field(self.configuration, _SCHEMA_FIELDS)
        if schema:
            return schema

        # Snowflake / BigQuery expose schemas as a list; take the first
        # entry (single-schema sources are the common case).
        if self.configuration:
            schemas = self.configuration.get("schemas")
            if schemas and isinstance(schemas, list) and len(schemas) > 0:
                first_schema = schemas[0]
                if isinstance(first_schema, str):
                    return first_schema
                if isinstance(first_schema, dict):
                    return first_schema.get("name") or first_schema.get("schema")
        return None

    @property
    def get_database(self) -> Optional[str]:
        return _lookup_config_field(self.configuration, _SOURCE_DATABASE_FIELDS)


class AirbyteDestinationPartial(BaseModel):
    destination_id: str = Field(alias="destinationId")
    name: Optional[str] = None
    destination_type: Optional[str] = Field(None, alias="destinationType")
    destination_definition_id: Optional[str] = Field(
        None,
        validation_alias=AliasChoices("definitionId", "destinationDefinitionId"),
    )
    configuration: Optional[Dict[str, Any]] = None
    workspace_id: Optional[str] = Field(None, alias="workspaceId")
    created_at: Optional[int] = Field(None, alias="createdAt")

    model_config = ConfigDict(populate_by_name=True)

    @property
    def get_schema(self) -> Optional[str]:
        return _lookup_config_field(self.configuration, _SCHEMA_FIELDS)

    @property
    def get_database(self) -> Optional[str]:
        return _lookup_config_field(self.configuration, _DESTINATION_DATABASE_FIELDS)


class AirbyteConnectionPartial(BaseModel):
    connection_id: str = Field(alias="connectionId")
    name: Optional[str] = None
    source_id: str = Field(alias="sourceId")
    destination_id: str = Field(alias="destinationId")
    status: Optional[str] = None
    sync_catalog: Optional[AirbyteSyncCatalog] = Field(None, alias="syncCatalog")
    configuration: Optional[Dict[str, Any]] = None
    schedule_type: Optional[str] = Field(None, alias="scheduleType")
    schedule_data: Optional[Dict[str, Any]] = Field(None, alias="scheduleData")
    namespace_definition: Optional[str] = Field(None, alias="namespaceDefinition")
    namespace_format: Optional[str] = Field(None, alias="namespaceFormat")
    prefix: Optional[str] = None
    created_at: Optional[int] = Field(None, alias="createdAt")
    tags: List[Dict[str, Any]] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True)

    # Airbyte's OSS API surfaces these as top-level fields; the Public API
    # nests them under `configuration`. We accept both shapes.
    @property
    def get_namespace_definition(self) -> Optional[str]:
        if self.namespace_definition:
            return self.namespace_definition
        if self.configuration:
            return self.configuration.get("namespaceDefinition")
        return None

    @property
    def get_namespace_format(self) -> Optional[str]:
        if self.namespace_format:
            return self.namespace_format
        if self.configuration:
            return self.configuration.get("namespaceFormat")
        return None

    @property
    def get_prefix(self) -> Optional[str]:
        if self.prefix:
            return self.prefix
        if self.configuration:
            return self.configuration.get("prefix")
        return None


class AirbyteWorkspacePartial(BaseModel):
    workspace_id: str = Field(alias="workspaceId")
    name: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)


class AirbytePipelineInfo(BaseModel):
    workspace: AirbyteWorkspacePartial
    connection: AirbyteConnectionPartial
    source: AirbyteSourcePartial
    destination: AirbyteDestinationPartial


class AirbyteStreamDetails(BaseModel):
    stream_name: str = Field(alias="streamName")
    namespace: str = Field(
        default="",
        validation_alias=AliasChoices("namespace", "streamnamespace"),
    )
    property_fields: List[PropertyFieldPath] = Field(
        default_factory=list, alias="propertyFields"
    )
    default_cursor_field: List[str] = Field(
        default_factory=list, alias="defaultCursorField"
    )
    source_defined_cursor_field: bool = Field(False, alias="sourceDefinedCursorField")
    source_defined_primary_key: List[List[str]] = Field(
        default_factory=list, alias="sourceDefinedPrimaryKey"
    )

    model_config = ConfigDict(populate_by_name=True)

    def get_column_names(self) -> List[str]:
        return [field.field_name for field in self.property_fields]


class AirbyteTagInfo(BaseModel):
    tag_id: Optional[str] = Field(None, alias="id")
    name: str
    resource_id: Optional[str] = Field(None, alias="resourceId")
    resource_type: Optional[str] = Field(None, alias="resourceType")

    model_config = ConfigDict(populate_by_name=True)


class AirbyteDatasetUrns(BaseModel):
    source_urn: str
    destination_urn: str

    model_config = ConfigDict(populate_by_name=True)


class AirbyteStreamInfo(BaseModel):
    config: AirbyteStreamConfig
    details: AirbyteStreamDetails

    model_config = ConfigDict(populate_by_name=True)


class PlatformInfo(BaseModel):
    platform: str
    platform_instance: Optional[str] = None
    env: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)


class PlatformKind(StrEnum):
    SOURCE = "source"
    DESTINATION = "destination"


class PlatformResolutionRequest(BaseModel):
    """Inputs to `AirbyteSource._resolve_platform`. Lets sources and
    destinations share a single resolution helper instead of two near-
    identical methods."""

    entity_id: str
    entity_type: Optional[str] = None
    name: Optional[str] = None
    definition_id: Optional[str] = None
    overrides: PlatformDetail
    kind: PlatformKind

    model_config = ConfigDict(populate_by_name=True)


class AirbyteTestResult(BaseModel):
    success: bool
    error_message: Optional[str] = None
    data: Optional[
        Union[
            AirbyteWorkspacePartial,
            AirbyteConnectionPartial,
            AirbyteSourcePartial,
            AirbyteDestinationPartial,
        ]
    ] = None
