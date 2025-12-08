from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.utilities.urns.data_flow_urn import DataFlowUrn
from datahub.utilities.urns.data_job_urn import DataJobUrn


class AirbyteStream(BaseModel):
    """Model for Airbyte stream details in syncCatalog."""

    name: str
    namespace: Optional[str] = Field(None, alias="namespace")
    json_schema: Dict[str, Any] = Field({}, alias="jsonSchema")

    model_config = ConfigDict(populate_by_name=True)


class AirbyteStreamConfig(BaseModel):
    """Model for Airbyte stream configuration in syncCatalog."""

    stream: AirbyteStream
    config: Dict[str, Any] = {}  # Default to empty dict instead of Optional

    def is_enabled(self) -> bool:
        """Check if stream is enabled for syncing."""
        if not self.config:
            return True

        if self.config.get("syncMode") == "null":
            return False

        if self.config.get("selected") is False:
            return False

        return True

    def is_field_selected(self, field_name: str) -> bool:
        """Check if a field is selected for syncing."""
        # By default, assume all fields are selected
        if not self.config:
            return True

        # Check for field-specific selection
        field_selection = self.config.get("fieldSelection", {})
        if field_name in field_selection and field_selection[field_name] is False:
            return False

        return True

    model_config = ConfigDict(populate_by_name=True)


class AirbyteSyncCatalog(BaseModel):
    """Model for Airbyte sync catalog."""

    streams: List[AirbyteStreamConfig] = []

    model_config = ConfigDict(populate_by_name=True)


class AirbyteSourceConfiguration(BaseModel):
    """Model for Airbyte source specific configuration."""

    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    schemas: List[str] = Field(default_factory=list)
    schema_name: Optional[str] = Field(None, alias="schema")
    username: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True, extra="allow")


class AirbyteDestinationConfiguration(BaseModel):
    """Model for Airbyte destination specific configuration."""

    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    schema_name: Optional[str] = Field(None, alias="schema")
    username: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True, extra="allow")


class AirbyteDataSource(BaseModel):
    """Model for Airbyte source configuration."""

    source_id: str = Field(alias="sourceId")
    name: str
    source_type: str = Field("", alias="sourceType")  # Default to empty string
    source_definition_id: str = Field(
        "", alias="sourceDefinitionId"
    )  # Default to empty string
    configuration: AirbyteSourceConfiguration = Field(
        default_factory=AirbyteSourceConfiguration
    )
    workspace_id: str = Field("", alias="workspaceId")  # Default to empty string

    model_config = ConfigDict(populate_by_name=True)


class AirbyteDestination(BaseModel):
    """Model for Airbyte destination configuration."""

    destination_id: str = Field(alias="destinationId")
    name: str
    destination_type: str = Field(
        "", alias="destinationType"
    )  # Default to empty string
    destination_definition_id: str = Field(
        "", alias="destinationDefinitionId"
    )  # Default to empty string
    configuration: AirbyteDestinationConfiguration = Field(
        default_factory=AirbyteDestinationConfiguration
    )
    workspace_id: str = Field("", alias="workspaceId")  # Default to empty string

    model_config = ConfigDict(populate_by_name=True)


class AirbyteConnection(BaseModel):
    """Model for Airbyte connection configuration."""

    connection_id: str = Field(alias="connectionId")
    name: str
    source_id: str = Field(alias="sourceId")
    destination_id: str = Field(alias="destinationId")
    status: str
    sync_catalog: AirbyteSyncCatalog = Field(
        default_factory=AirbyteSyncCatalog, alias="syncCatalog"
    )
    schedule_type: str = Field("", alias="scheduleType")  # Default to empty string
    schedule_data: Dict[str, Any] = Field(default_factory=dict, alias="scheduleData")

    model_config = ConfigDict(populate_by_name=True)


class AirbyteJobAttempt(BaseModel):
    """Model for Airbyte job attempt."""

    id: int
    status: str
    created_at: int = Field(alias="createdAt")
    ended_at: Optional[int] = Field(
        None, alias="endedAt"
    )  # This can legitimately be null for running jobs

    model_config = ConfigDict(populate_by_name=True)


class AirbyteJob(BaseModel):
    """Model for Airbyte job details."""

    id: str
    config_id: str = Field(alias="configId")
    config_type: str = Field(alias="configType")
    status: str
    created_at: int = Field(alias="createdAt")
    updated_at: int = Field(alias="updatedAt")
    attempts: List[AirbyteJobAttempt] = []

    model_config = ConfigDict(populate_by_name=True)


class AirbyteWorkspace(BaseModel):
    """Model for Airbyte workspace."""

    workspace_id: str = Field(alias="workspaceId")
    name: str

    model_config = ConfigDict(populate_by_name=True)


class AirbyteSourcePartial(BaseModel):
    """Model for potentially incomplete Airbyte source configuration."""

    source_id: str = Field(alias="sourceId")  # Make mandatory
    name: Optional[str] = None
    source_type: Optional[str] = Field(None, alias="sourceType")
    source_definition_id: Optional[str] = Field(None, alias="sourceDefinitionId")
    configuration: Optional[Dict[str, Any]] = None
    workspace_id: Optional[str] = Field(None, alias="workspaceId")

    model_config = ConfigDict(populate_by_name=True)

    @property
    def get_schema(self) -> Optional[str]:
        """
        Get schema from the source configuration

        Returns:
            Schema name or None if not found
        """
        if not self.configuration:
            return None

        # Try common schema field names across different connectors
        schema_fields = [
            "schema",  # Generic, Postgres, Snowflake
            "default_schema",  # MSSQL, Oracle
            "schema_name",  # Alternative name
        ]

        for field in schema_fields:
            schema = self.configuration.get(field)
            if schema and isinstance(schema, str):
                return schema

        # Then look for schemas array (Snowflake, BigQuery)
        schemas = self.configuration.get("schemas")
        if schemas and isinstance(schemas, list) and len(schemas) > 0:
            first_schema = schemas[0]
            # schemas might be array of strings or objects
            if isinstance(first_schema, str):
                return first_schema
            elif isinstance(first_schema, dict):
                return first_schema.get("name") or first_schema.get("schema")

        return None

    @property
    def get_database(self) -> Optional[str]:
        """
        Get database from the source configuration

        Returns:
            Database name or None if not found
        """
        if not self.configuration:
            return None

        # Try common database field names across different connectors
        database_fields = [
            "database",  # Generic, Postgres, MySQL, MSSQL, Snowflake
            "db",  # Short form
            "db_name",  # Alternative
            "database_name",  # Alternative
            "dbname",  # Alternative
            "service_name",  # Oracle
            "sid",  # Oracle SID
            "project",  # BigQuery (project = database equivalent)
            "project_id",  # BigQuery alternative
            "catalog",  # Databricks, Trino
        ]

        for field in database_fields:
            database = self.configuration.get(field)
            if database and isinstance(database, str):
                return database

        return None


class AirbyteDestinationPartial(BaseModel):
    """Model for potentially incomplete Airbyte destination configuration."""

    destination_id: str = Field(alias="destinationId")  # Make mandatory
    name: Optional[str] = None
    destination_type: Optional[str] = Field(None, alias="destinationType")
    destination_definition_id: Optional[str] = Field(
        None, alias="destinationDefinitionId"
    )
    configuration: Optional[Dict[str, Any]] = None
    workspace_id: Optional[str] = Field(None, alias="workspaceId")

    model_config = ConfigDict(populate_by_name=True)

    @property
    def get_schema(self) -> Optional[str]:
        """
        Get schema from the destination configuration

        Returns:
            Schema name or None if not found
        """
        if not self.configuration:
            return None

        # Try common schema field names across different connectors
        schema_fields = [
            "schema",  # Generic, Postgres, Snowflake
            "default_schema",  # MSSQL, Oracle
            "schema_name",  # Alternative name
        ]

        for field in schema_fields:
            schema = self.configuration.get(field)
            if schema and isinstance(schema, str):
                return schema

        return None

    @property
    def get_database(self) -> Optional[str]:
        """
        Get database from the destination configuration

        Returns:
            Database name or None if not found
        """
        if not self.configuration:
            return None

        # Try common database field names across different connectors
        database_fields = [
            "database",  # Generic, Postgres, MySQL, MSSQL, Snowflake
            "db",  # Short form
            "db_name",  # Alternative
            "database_name",  # Alternative
            "dbname",  # Alternative
            "service_name",  # Oracle
            "sid",  # Oracle SID
            "project",  # BigQuery (project = database equivalent)
            "project_id",  # BigQuery alternative
            "catalog",  # Databricks, Trino
            "dataset",  # BigQuery (dataset = schema equivalent, but some use it as database)
        ]

        for field in database_fields:
            database = self.configuration.get(field)
            if database and isinstance(database, str):
                return database

        return None


class AirbyteConnectionPartial(BaseModel):
    """Model for potentially incomplete Airbyte connection configuration."""

    connection_id: str = Field(alias="connectionId")  # Make mandatory
    name: Optional[str] = None
    source_id: str = Field(alias="sourceId")  # Make mandatory
    destination_id: str = Field(alias="destinationId")  # Make mandatory
    status: Optional[str] = None
    sync_catalog: Optional[AirbyteSyncCatalog] = Field(None, alias="syncCatalog")
    configuration: Optional[Dict[str, Any]] = None  # May contain catalog info
    schedule_type: Optional[str] = Field(None, alias="scheduleType")
    schedule_data: Optional[Dict[str, Any]] = Field(None, alias="scheduleData")
    namespace_definition: Optional[str] = Field(None, alias="namespaceDefinition")
    namespace_format: Optional[str] = Field(None, alias="namespaceFormat")
    prefix: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)

    @property
    def get_namespace_definition(self) -> Optional[str]:
        """Get namespace definition from connection config.

        Returns:
            'source', 'destination', or 'customformat', or None if not set
        """
        # First check top-level field
        if self.namespace_definition:
            return self.namespace_definition

        # Fall back to configuration dict
        if self.configuration:
            return self.configuration.get("namespaceDefinition")

        return None

    @property
    def get_namespace_format(self) -> Optional[str]:
        """Get namespace format string from connection config.

        Returns:
            Format string like "${SOURCE_NAMESPACE}" or None
        """
        # First check top-level field
        if self.namespace_format:
            return self.namespace_format

        # Fall back to configuration dict
        if self.configuration:
            return self.configuration.get("namespaceFormat")

        return None

    @property
    def get_prefix(self) -> Optional[str]:
        """Get table name prefix from connection config.

        Returns:
            Prefix string or None
        """
        # First check top-level field
        if self.prefix:
            return self.prefix

        # Fall back to configuration dict
        if self.configuration:
            return self.configuration.get("prefix")

        return None


# Updated AirbyteWorkspacePartial model with workspace_id as mandatory
class AirbyteWorkspacePartial(BaseModel):
    """Model for potentially incomplete Airbyte workspace."""

    workspace_id: str = Field(alias="workspaceId")  # Make mandatory
    name: Optional[str] = None

    model_config = ConfigDict(populate_by_name=True)


class AirbytePipelineInfo(BaseModel):
    """Model for storing Airbyte pipeline (connection) details."""

    workspace: AirbyteWorkspacePartial
    connection: AirbyteConnectionPartial
    source: AirbyteSourcePartial
    destination: AirbyteDestinationPartial


class AirbyteJobExecution(BaseModel):
    """Model for Airbyte job execution details."""

    id: str
    name: str
    status: str
    start_time: int
    end_time: Optional[int] = None  # Can be None for running jobs

    model_config = ConfigDict(populate_by_name=True)


# New models for better type safety


class AirbyteStreamField(BaseModel):
    """Model for stream field/column metadata."""

    name: str
    field_type: str = ""  # Default to empty string
    description: str = ""  # Default to empty string

    model_config = ConfigDict(populate_by_name=True)


class AirbyteStreamDetails(BaseModel):
    """Model for Airbyte stream details from API."""

    stream_name: str = Field(alias="streamName")
    namespace: str = ""  # Default to empty string instead of Optional
    property_fields: List[Union[List[str], str]] = Field(
        default_factory=list, alias="propertyFields"
    )

    model_config = ConfigDict(populate_by_name=True)

    def get_column_names(self) -> List[str]:
        """Extract column names from property fields."""
        columns = []
        for prop_field in self.property_fields:
            if isinstance(prop_field, list) and prop_field:
                columns.append(prop_field[0])
            elif isinstance(prop_field, str):
                columns.append(prop_field)
        return columns


class AirbyteTagInfo(BaseModel):
    """Model for Airbyte tag information."""

    tag_id: Optional[str] = Field(None, alias="id")  # May be missing in some responses
    name: str
    resource_id: Optional[str] = Field(
        None, alias="resourceId"
    )  # May be missing for global tags
    resource_type: Optional[str] = Field(
        None, alias="resourceType"
    )  # May be missing for global tags

    model_config = ConfigDict(populate_by_name=True)


class ValidatedPipelineIds(BaseModel):
    """Model for validated workspace and connection IDs."""

    workspace_id: str
    connection_id: str

    model_config = ConfigDict(populate_by_name=True)


class AirbyteDatasetUrns(BaseModel):
    """Model for paired source and destination dataset URNs."""

    source_urn: str
    destination_urn: str

    model_config = ConfigDict(populate_by_name=True)


class AirbyteInputOutputDatasets(BaseModel):
    """Model for input and output dataset URNs."""

    input_urns: List[str] = Field(default_factory=list)
    output_urns: List[str] = Field(default_factory=list)

    model_config = ConfigDict(populate_by_name=True)


@dataclass
class DataFlowResult:
    """Container for DataFlow creation results."""

    dataflow_urn: DataFlowUrn
    work_units: Iterable[MetadataWorkUnit]


@dataclass
class DataJobResult:
    """Container for DataJob creation results."""

    datajob_urn: DataJobUrn
    work_units: Iterable[MetadataWorkUnit]


@dataclass
class AirbyteTestResult:
    """Container for Airbyte connection test results."""

    success: bool
    error_message: Optional[str] = None
    data: Optional[
        Union[
            "AirbyteWorkspacePartial",
            "AirbyteConnectionPartial",
            "AirbyteSourcePartial",
            "AirbyteDestinationPartial",
        ]
    ] = None
