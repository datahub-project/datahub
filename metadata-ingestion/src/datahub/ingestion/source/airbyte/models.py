from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field

from datahub.emitter.mce_builder import DEFAULT_ENV


class AirbyteStream(BaseModel):
    """Model for Airbyte stream details in syncCatalog."""

    name: str
    namespace: Optional[str] = Field(None, alias="namespace")
    json_schema: Dict[str, Any] = Field({}, alias="jsonSchema")

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True


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

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True


class AirbyteSyncCatalog(BaseModel):
    """Model for Airbyte sync catalog."""

    streams: List[AirbyteStreamConfig] = []

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True


class AirbyteSourceConfiguration(BaseModel):
    """Model for Airbyte source specific configuration."""

    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    schemas: List[str] = Field(default_factory=list)
    schema_name: Optional[str] = Field(None, alias="schema")
    username: Optional[str] = None

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True
        extra = "allow"  # Allow extra fields


class AirbyteDestinationConfiguration(BaseModel):
    """Model for Airbyte destination specific configuration."""

    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    schema_name: Optional[str] = Field(None, alias="schema")
    username: Optional[str] = None

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True
        extra = "allow"  # Allow extra fields


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

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True


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

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True


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

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True


class AirbyteJobAttempt(BaseModel):
    """Model for Airbyte job attempt."""

    id: int
    status: str
    created_at: int = Field(alias="createdAt")
    ended_at: Optional[int] = Field(
        None, alias="endedAt"
    )  # This can legitimately be null for running jobs

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True


class AirbyteJob(BaseModel):
    """Model for Airbyte job details."""

    id: str
    config_id: str = Field(alias="configId")
    config_type: str = Field(alias="configType")
    status: str
    created_at: int = Field(alias="createdAt")
    updated_at: int = Field(alias="updatedAt")
    attempts: List[AirbyteJobAttempt] = []

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True


class AirbyteWorkspace(BaseModel):
    """Model for Airbyte workspace."""

    workspace_id: str = Field(alias="workspaceId")
    name: str

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True


class AirbyteSourcePartial(BaseModel):
    """Model for potentially incomplete Airbyte source configuration."""

    source_id: str = Field(alias="sourceId")  # Make mandatory
    name: Optional[str] = None
    source_type: Optional[str] = Field(None, alias="sourceType")
    source_definition_id: Optional[str] = Field(None, alias="sourceDefinitionId")
    configuration: Optional[Dict[str, Any]] = None
    workspace_id: Optional[str] = Field(None, alias="workspaceId")

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True

    @property
    def get_schema(self) -> Optional[str]:
        """
        Get schema from the source configuration

        Returns:
            Schema name or None if not found
        """
        if not self.configuration:
            return None

        # First look for a single schema field
        schema = self.configuration.get("schema")
        if schema:
            return schema

        # Then look for schemas array
        schemas = self.configuration.get("schemas")
        if schemas and isinstance(schemas, list) and len(schemas) > 0:
            return schemas[0]  # Return the first schema in the list

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

        return self.configuration.get("database")


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

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True

    @property
    def get_schema(self) -> Optional[str]:
        """
        Get schema from the destination configuration

        Returns:
            Schema name or None if not found
        """
        if not self.configuration:
            return None

        return self.configuration.get("schema")

    @property
    def get_database(self) -> Optional[str]:
        """
        Get database from the destination configuration

        Returns:
            Database name or None if not found
        """
        if not self.configuration:
            return None

        return self.configuration.get("database")


class AirbyteConnectionPartial(BaseModel):
    """Model for potentially incomplete Airbyte connection configuration."""

    connection_id: str = Field(alias="connectionId")  # Make mandatory
    name: Optional[str] = None
    source_id: str = Field(alias="sourceId")  # Make mandatory
    destination_id: str = Field(alias="destinationId")  # Make mandatory
    status: Optional[str] = None
    sync_catalog: Optional[AirbyteSyncCatalog] = Field(None, alias="syncCatalog")
    schedule_type: Optional[str] = Field(None, alias="scheduleType")
    schedule_data: Optional[Dict[str, Any]] = Field(None, alias="scheduleData")

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True


# Updated AirbyteWorkspacePartial model with workspace_id as mandatory
class AirbyteWorkspacePartial(BaseModel):
    """Model for potentially incomplete Airbyte workspace."""

    workspace_id: str = Field(alias="workspaceId")  # Make mandatory
    name: Optional[str] = None

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True


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

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True


# New models for better type safety


class AirbyteStreamField(BaseModel):
    """Model for stream field/column metadata."""

    name: str
    field_type: str = ""  # Default to empty string
    description: str = ""  # Default to empty string

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True


class AirbyteStreamDetails(BaseModel):
    """Model for Airbyte stream details from API."""

    stream_name: str = Field(alias="streamName")
    namespace: str = ""  # Default to empty string instead of Optional
    property_fields: List[Union[List[str], str]] = Field(
        default_factory=list, alias="propertyFields"
    )

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True

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

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True


class AirbyteDatasetMapping(BaseModel):
    """Model for mapping Airbyte dataset components to DataHub URNs."""

    platform: str
    name: str
    env: str = DEFAULT_ENV
    platform_instance: Optional[str] = None  # This can legitimately be None

    class Config:
        """Pydantic configuration."""

        allow_population_by_field_name = True
