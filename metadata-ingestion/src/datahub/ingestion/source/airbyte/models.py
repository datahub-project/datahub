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
    API_FIELD_DESTINATION_SYNC_MODE,
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
    SYNC_MODE_DESTINATION_OVERWRITE,
    SYNC_MODE_FULL_REFRESH,
    SYNC_MODE_INCREMENTAL,
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


def _as_str_list(value: object) -> List[str]:
    """Pydantic v2 will not widen a scalar into a list, so off-spec payloads
    such as `"cursorField": "updated_at"` would otherwise fail validation."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if item is not None]
    return [str(value)]


def _as_field_paths(value: object) -> List[List[str]]:
    """Coerce to Airbyte's `string[][]` field-path shape. A flat
    `["id", "tenant"]` is read as two single-segment paths, matching how
    Airbyte expresses a composite key."""
    if value is None:
        return []
    if isinstance(value, str):
        return [[value]]
    if not isinstance(value, (list, tuple)):
        return [[str(value)]]
    paths: List[List[str]] = []
    for item in value:
        if item is None:
            continue
        path = _as_str_list(item)
        if path:
            paths.append(path)
    return paths


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
        schema = (
            resolved.pop(API_FIELD_JSON_SCHEMA, None)
            or resolved.pop(API_FIELD_JSON_SCHEMA_SNAKE, None)
            or {}
        )
        # A non-dict schema carries nothing we can read; an empty one just means
        # columns come from /streams propertyFields instead.
        resolved[API_FIELD_JSON_SCHEMA] = schema if isinstance(schema, dict) else {}
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

    @field_validator("primary_key", mode="before")
    @classmethod
    def _normalize_primary_key(cls, value: object) -> List[List[str]]:
        return _as_field_paths(value)

    @field_validator("cursor_field", mode="before")
    @classmethod
    def _normalize_cursor_field(cls, value: object) -> List[str]:
        return _as_str_list(value)

    @field_validator("selected_fields", mode="before")
    @classmethod
    def _normalize_selected_fields(cls, value: object) -> Optional[List[object]]:
        if value is None:
            return None
        return list(value) if isinstance(value, (list, tuple)) else [value]


class AirbyteStreamsApiRow(BaseModel):
    """One row from Airbyte `/streams`. Field names vary by version, and the
    namespace only appears from 1.7.0 onwards."""

    stream_name: Optional[str] = None
    namespace: str = ""
    property_fields: List[List[str]] = Field(default_factory=list)

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
        resolved["property_fields"] = _as_field_paths(
            data.get(API_FIELD_PROPERTY_FIELDS)
        )
        return resolved


class SyncModeSplit(BaseModel):
    """The Public API's single `syncMode` string split back into the two modes
    the sync catalog carries separately, e.g. `full_refresh_overwrite` ->
    (`full_refresh`, `overwrite`). Source modes are matched as prefixes because
    splitting on the first `_` mis-reads `full_refresh_*` as `full`."""

    source_mode: str = SYNC_MODE_FULL_REFRESH
    destination_mode: str = SYNC_MODE_DESTINATION_OVERWRITE

    model_config = ConfigDict(frozen=True)

    @classmethod
    def from_api_value(cls, value: Optional[str]) -> "SyncModeSplit":
        if not value:
            return cls()
        for source_mode in (SYNC_MODE_FULL_REFRESH, SYNC_MODE_INCREMENTAL):
            prefix = f"{source_mode}_"
            if value.startswith(prefix):
                return cls(
                    source_mode=source_mode,
                    destination_mode=value[len(prefix) :],
                )
        # Unrecognized values (including the "null" marker Airbyte uses for a
        # disabled stream) pass through so `is_enabled` can still see them.
        return cls(source_mode=value)


class AirbyteStreamSyncSettings(BaseModel):
    selected: bool = True
    sync_mode: str = Field(default=SYNC_MODE_FULL_REFRESH, alias=API_FIELD_SYNC_MODE)
    destination_sync_mode: str = Field(
        default=SYNC_MODE_DESTINATION_OVERWRITE, alias=API_FIELD_DESTINATION_SYNC_MODE
    )
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
    """Namespaces to hand out to config streams that carry none. `ambiguous`
    keeps the unclaimed candidates so the report can list them."""

    queues: StreamNamespacesByName = Field(default_factory=dict)
    ambiguous: StreamNamespacesByName = Field(default_factory=dict)

    model_config = ConfigDict(frozen=True)


class ResolvedSchema(BaseModel):
    """A stream's schema tier. `guessed` marks a name resolution had to pick
    rather than read."""

    name: str = ""
    guessed: bool = False

    model_config = ConfigDict(frozen=True)


class SyncCatalogBuildResult(BaseModel):
    catalog: AirbyteSyncCatalog
    ambiguous: StreamNamespacesByName = Field(default_factory=dict)
    skipped_stream_payloads: List[str] = Field(default_factory=list)
    streams_api_unavailable: bool = False
    streams_api_unavailable_status_code: Optional[int] = None
    streams_api_unavailable_message: Optional[str] = None
    streams_api_namespaces_absent: bool = False

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
    def declared_schema(self) -> Optional[str]:
        return _lookup_config_field(self.configuration, SCHEMA_CONFIG_FIELDS)

    @property
    def configured_schemas(self) -> List[str]:
        # Postgres / Snowflake / BigQuery list the schemas they replicate instead
        # of naming one.
        if not self.configuration:
            return []
        schemas = self.configuration.get(API_FIELD_SCHEMAS)
        if not isinstance(schemas, list):
            return []

        names: List[str] = []
        for schema in schemas:
            if isinstance(schema, str):
                name: Optional[str] = schema
            elif isinstance(schema, dict):
                name = schema.get(API_FIELD_NAME) or schema.get(API_FIELD_SCHEMA)
            else:
                name = None
            if name:
                names.append(name)
        return names

    @property
    def schema_is_guess(self) -> bool:
        """True when the configuration only offers a list of several schemas.
        Nothing says which stream belongs to which, so picking one is a guess
        that is wrong for the rest."""
        return self.declared_schema is None and len(self.configured_schemas) > 1

    @property
    def get_schema(self) -> Optional[str]:
        if self.declared_schema:
            return self.declared_schema
        schemas = self.configured_schemas
        return schemas[0] if schemas else None

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
    # Not API fields: set by the client when it rebuilds the sync catalog from
    # `configurations.streams`, so the source can report what the rebuild lost.
    ambiguous_stream_namespaces: StreamNamespacesByName = Field(default_factory=dict)
    skipped_stream_payloads: List[str] = Field(default_factory=list)
    streams_api_unavailable: bool = False
    streams_api_unavailable_status_code: Optional[int] = None
    streams_api_unavailable_message: Optional[str] = None
    streams_api_namespaces_absent: bool = False
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
    # Holds the resolved schema tier, not just Airbyte's namespace.
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
    unavailable: bool = False
    # Populated when unavailable=True so the source warning can report what
    # Airbyte actually returned (HTTP status vs network/connection error).
    unavailable_status_code: Optional[int] = None
    unavailable_message: Optional[str] = None
    # `/streams` answered but no row carried a namespace, so there is nothing to
    # back-fill from.
    namespaces_absent: bool = False
    skipped_rows: List[str] = Field(default_factory=list)


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
