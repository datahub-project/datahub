from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from datahub.ingestion.source.airbyte.config import PlatformDetail
from datahub.ingestion.source.airbyte.constants import (
    API_FIELD_ALIAS_NAME,
    API_FIELD_CONNECTION_ID,
    API_FIELD_CURSOR_FIELD,
    API_FIELD_DESTINATION_ID,
    API_FIELD_DESTINATION_NAMESPACE,
    API_FIELD_FIELD_SELECTION,
    API_FIELD_FIELD_SELECTION_ENABLED,
    API_FIELD_JSON_SCHEMA,
    API_FIELD_JSON_SCHEMA_SNAKE,
    API_FIELD_NAME,
    API_FIELD_NAMESPACE,
    API_FIELD_NAMESPACE_DEFINITION,
    API_FIELD_NAMESPACE_FORMAT,
    API_FIELD_PREFIX,
    API_FIELD_PRIMARY_KEY,
    API_FIELD_PROPERTY_FIELDS,
    API_FIELD_SCHEMA,
    API_FIELD_SCHEMAS,
    API_FIELD_SELECTED_FIELDS,
    API_FIELD_SOURCE_ID,
    API_FIELD_STATUS,
    API_FIELD_STREAM_NAME,
    API_FIELD_STREAM_NAMESPACE_CAMEL,
    API_FIELD_STREAM_NAMESPACE_LOWER,
    API_FIELD_SYNC_CATALOG,
    API_FIELD_SYNC_MODE,
    API_FIELD_TABLES,
    DESTINATION_DATABASE_CONFIG_FIELDS,
    SCHEMA_CONFIG_FIELDS,
    SOURCE_DATABASE_CONFIG_FIELDS,
    SYNC_MODE_NULL,
)
from datahub.utilities.str_enum import StrEnum

StreamNamespacesByName = Dict[str, List[str]]


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


def _coerce_optional_str(value: object) -> Optional[str]:
    if value is None or isinstance(value, str):
        return value
    return str(value)


def _first_truthy_str(data: Mapping[str, Any], keys: Sequence[str]) -> Optional[str]:
    # Airbyte writes these keys inconsistently across versions, and a key that
    # is present but empty must fall through to the next alias. Pydantic's
    # AliasChoices resolves on key *presence*, so it cannot express this.
    for key in keys:
        value = data.get(key)
        if value:
            return _coerce_optional_str(value)
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


class PropertyFieldPath(BaseModel):
    """A dotted path to a (possibly nested) Airbyte field, e.g.
    `["address", "city"]` for `address.city`. We only consume the leaf name for
    column-level lineage; the rest is preserved for callers that care about
    nesting."""

    path: List[str]

    @property
    def field_name(self) -> str:
        return self.path[-1] if self.path else ""

    def __str__(self) -> str:
        return ".".join(self.path)


class AirbyteConfigStreamRef(BaseModel):
    """One `configurations.streams` entry from the Public API, covering both the
    namespace-backfill queue accounting and the sync settings the catalog
    builder needs. Extra keys are preserved, and values are coerced rather than
    rejected — a malformed entry should degrade to a best-effort stream instead
    of aborting the whole connection fetch."""

    name: Optional[str] = None
    namespace: Optional[str] = None
    # The Public API packs both sync modes into one string, e.g.
    # "incremental_append"; it is split when building the stream config.
    sync_mode: Optional[str] = Field(None, alias=API_FIELD_SYNC_MODE)
    primary_key: List[List[str]] = Field(
        default_factory=list, alias=API_FIELD_PRIMARY_KEY
    )
    cursor_field: List[str] = Field(default_factory=list, alias=API_FIELD_CURSOR_FIELD)
    destination_namespace: Optional[str] = Field(
        None, alias=API_FIELD_DESTINATION_NAMESPACE
    )
    alias_name: Optional[str] = Field(None, alias=API_FIELD_ALIAS_NAME)
    selected_fields: Optional[List[object]] = Field(
        None, alias=API_FIELD_SELECTED_FIELDS
    )
    field_selection_enabled: Optional[bool] = Field(
        None, alias=API_FIELD_FIELD_SELECTION_ENABLED
    )
    json_schema: Dict[str, Any] = Field(
        default_factory=dict, alias=API_FIELD_JSON_SCHEMA
    )

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    @model_validator(mode="before")
    @classmethod
    def _resolve_json_schema(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        resolved = dict(data)
        resolved[API_FIELD_JSON_SCHEMA] = (
            resolved.pop(API_FIELD_JSON_SCHEMA, None)
            or resolved.pop(API_FIELD_JSON_SCHEMA_SNAKE, None)
            or {}
        )
        return resolved

    @field_validator(
        "name",
        "namespace",
        "sync_mode",
        "destination_namespace",
        "alias_name",
        mode="before",
    )
    @classmethod
    def _stringify(cls, value: object) -> Optional[str]:
        return _coerce_optional_str(value)

    @field_validator("primary_key", "cursor_field", mode="before")
    @classmethod
    def _empty_list_when_missing(cls, value: object) -> object:
        return value or []


class AirbyteStreamsApiRow(BaseModel):
    """One row from Airbyte `/streams` (1.8+). Field names vary by version."""

    stream_name: Optional[str] = None
    namespace: str = ""
    property_fields: List[object] = Field(default_factory=list)

    model_config = ConfigDict(extra="allow")

    @model_validator(mode="before")
    @classmethod
    def _resolve_aliases(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        resolved = dict(data)
        resolved["stream_name"] = _first_truthy_str(
            data, (API_FIELD_STREAM_NAME, API_FIELD_NAME)
        )
        resolved["namespace"] = (
            _first_truthy_str(
                data,
                (
                    API_FIELD_NAMESPACE,
                    API_FIELD_STREAM_NAMESPACE_LOWER,
                    API_FIELD_STREAM_NAMESPACE_CAMEL,
                ),
            )
            or ""
        )
        resolved["property_fields"] = data.get(API_FIELD_PROPERTY_FIELDS) or []
        return resolved


class AirbyteStreamSyncSettings(BaseModel):
    selected: bool = True
    sync_mode: str = Field(default="full_refresh", alias=API_FIELD_SYNC_MODE)
    destination_sync_mode: str = Field(default="overwrite", alias="destinationSyncMode")
    primary_key: List[List[str]] = Field(
        default_factory=list, alias=API_FIELD_PRIMARY_KEY
    )
    cursor_field: List[str] = Field(default_factory=list, alias=API_FIELD_CURSOR_FIELD)
    destination_namespace: Optional[str] = Field(
        None, alias=API_FIELD_DESTINATION_NAMESPACE
    )
    alias_name: Optional[str] = Field(None, alias=API_FIELD_ALIAS_NAME)
    # Airbyte versions disagree on the element shape (bare names vs.
    # `{"fieldPath": [...]}`); nothing downstream reads it, so pass it through.
    selected_fields: Optional[List[object]] = Field(
        None, alias=API_FIELD_SELECTED_FIELDS
    )
    field_selection_enabled: Optional[bool] = Field(
        None, alias=API_FIELD_FIELD_SELECTION_ENABLED
    )
    field_selection: Dict[str, bool] = Field(
        default_factory=dict, alias=API_FIELD_FIELD_SELECTION
    )

    model_config = ConfigDict(populate_by_name=True, extra="allow")


class AirbyteStream(BaseModel):
    name: str
    namespace: Optional[str] = Field(None, alias=API_FIELD_NAMESPACE)
    json_schema: Dict[str, Any] = Field(
        default_factory=dict, alias=API_FIELD_JSON_SCHEMA
    )

    model_config = ConfigDict(populate_by_name=True)


class AirbyteStreamConfig(BaseModel):
    stream: AirbyteStream
    config: AirbyteStreamSyncSettings = Field(default_factory=AirbyteStreamSyncSettings)

    def is_enabled(self) -> bool:
        if self.config.sync_mode == SYNC_MODE_NULL:
            return False
        if self.config.selected is False:
            return False
        return True

    def is_field_selected(self, field_name: str) -> bool:
        # Default to selected when no fieldSelection mapping is supplied.
        if not self.config.field_selection:
            return True
        return self.config.field_selection.get(field_name) is not False

    def get_destination_namespace(self) -> Optional[str]:
        return self.config.destination_namespace

    model_config = ConfigDict(populate_by_name=True)


class AirbyteSyncCatalog(BaseModel):
    streams: List[AirbyteStreamConfig] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True)


class NamespaceQueueResult(BaseModel):
    queues: StreamNamespacesByName = Field(default_factory=dict)
    ambiguous: StreamNamespacesByName = Field(default_factory=dict)
    positional: StreamNamespacesByName = Field(default_factory=dict)

    model_config = ConfigDict(frozen=True)


class SyncCatalogBuildResult(BaseModel):
    catalog: AirbyteSyncCatalog
    ambiguous: StreamNamespacesByName = Field(default_factory=dict)
    positional: StreamNamespacesByName = Field(default_factory=dict)

    model_config = ConfigDict(frozen=True)


class AirbyteSourceConfiguration(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    schemas: List[str] = Field(default_factory=list)
    schema_name: Optional[str] = Field(None, alias=API_FIELD_SCHEMA)
    username: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True, extra="allow")


class AirbyteDestinationConfiguration(BaseModel):
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    schema_name: Optional[str] = Field(None, alias=API_FIELD_SCHEMA)
    username: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True, extra="allow")


class AirbyteSourcePartial(BaseModel):
    """Airbyte source representation tolerant of missing optional fields — used
    for both list and get endpoints across OSS / Cloud / Public API versions,
    which return slightly different field sets."""

    source_id: str = Field(alias=API_FIELD_SOURCE_ID)
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
        # MSSQL connectors carry per-table schema overrides in configuration.tables.
        if not self.configuration:
            return None
        tables = self.configuration.get(API_FIELD_TABLES)
        if tables and isinstance(tables, list):
            for table in tables:
                if isinstance(table, dict) and table.get("name") == table_name:
                    return table.get(API_FIELD_SCHEMA)
        return None

    @property
    def get_schema(self) -> Optional[str]:
        schema = _lookup_config_field(self.configuration, SCHEMA_CONFIG_FIELDS)
        if schema:
            return schema

        # Snowflake / BigQuery expose schemas as a list; take the first entry.
        if self.configuration:
            schemas = self.configuration.get(API_FIELD_SCHEMAS)
            if schemas and isinstance(schemas, list) and len(schemas) > 0:
                first_schema = schemas[0]
                if isinstance(first_schema, str):
                    return first_schema
                if isinstance(first_schema, dict):
                    return first_schema.get("name") or first_schema.get(
                        API_FIELD_SCHEMA
                    )
        return None

    @property
    def get_database(self) -> Optional[str]:
        return _lookup_config_field(self.configuration, SOURCE_DATABASE_CONFIG_FIELDS)


class AirbyteDestinationPartial(BaseModel):
    destination_id: str = Field(alias=API_FIELD_DESTINATION_ID)
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
        return _lookup_config_field(self.configuration, SCHEMA_CONFIG_FIELDS)

    @property
    def get_database(self) -> Optional[str]:
        return _lookup_config_field(
            self.configuration, DESTINATION_DATABASE_CONFIG_FIELDS
        )


class AirbyteConnectionPartial(BaseModel):
    connection_id: str = Field(alias=API_FIELD_CONNECTION_ID)
    name: Optional[str] = None
    source_id: str = Field(alias=API_FIELD_SOURCE_ID)
    destination_id: str = Field(alias=API_FIELD_DESTINATION_ID)
    status: Optional[str] = Field(None, alias=API_FIELD_STATUS)
    sync_catalog: Optional[AirbyteSyncCatalog] = Field(
        None, alias=API_FIELD_SYNC_CATALOG
    )
    ambiguous_stream_namespaces: StreamNamespacesByName = Field(default_factory=dict)
    positional_stream_namespaces: StreamNamespacesByName = Field(default_factory=dict)
    configuration: Optional[Dict[str, Any]] = None
    schedule_type: Optional[str] = Field(None, alias="scheduleType")
    schedule_data: Optional[Dict[str, Any]] = Field(None, alias="scheduleData")
    namespace_definition: Optional[str] = Field(
        None, alias=API_FIELD_NAMESPACE_DEFINITION
    )
    namespace_format: Optional[str] = Field(None, alias=API_FIELD_NAMESPACE_FORMAT)
    prefix: Optional[str] = Field(None, alias=API_FIELD_PREFIX)
    created_at: Optional[int] = Field(None, alias="createdAt")
    tags: List[Dict[str, Any]] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True)

    # OSS API uses top-level fields; Public API nests them under configuration.
    @property
    def get_namespace_definition(self) -> Optional[str]:
        if self.namespace_definition:
            return self.namespace_definition
        if self.configuration:
            return self.configuration.get(API_FIELD_NAMESPACE_DEFINITION)
        return None

    @property
    def get_namespace_format(self) -> Optional[str]:
        if self.namespace_format:
            return self.namespace_format
        if self.configuration:
            return self.configuration.get(API_FIELD_NAMESPACE_FORMAT)
        return None

    @property
    def get_prefix(self) -> Optional[str]:
        if self.prefix:
            return self.prefix
        if self.configuration:
            return self.configuration.get(API_FIELD_PREFIX)
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
    stream_name: str = Field(alias=API_FIELD_STREAM_NAME)
    namespace: str = Field(
        default="",
        validation_alias=AliasChoices(
            API_FIELD_NAMESPACE, API_FIELD_STREAM_NAMESPACE_LOWER
        ),
    )
    property_fields: List[PropertyFieldPath] = Field(
        default_factory=list, alias=API_FIELD_PROPERTY_FIELDS
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


class AirbyteStreamApiMetadata(BaseModel):
    property_fields_by_stream: Dict[StreamIdentifier, List[PropertyFieldPath]] = Field(
        default_factory=dict
    )
    namespaces_by_name: StreamNamespacesByName = Field(default_factory=dict)


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
    """Inputs to `AirbyteSource._resolve_platform`. Lets sources and destinations
    share a single resolution helper instead of two near-identical methods."""

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
