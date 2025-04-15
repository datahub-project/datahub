import unittest
from enum import Enum
from typing import Dict, List, Optional, Union

import pytest
from pydantic import BaseModel, Field, ValidationError

from datahub.ingestion.source.airbyte.models import (
    AirbyteConnection,
    AirbyteDataSource,
    AirbyteDestination,
    AirbyteWorkspace,
)


# Mock classes for testing purposes that don't exist in the models.py
class AirbyteFieldType(str, Enum):
    """Mock enum for field types in tests."""

    STRING = "string"
    INTEGER = "integer"
    NUMBER = "number"
    BOOLEAN = "boolean"
    OBJECT = "object"
    ARRAY = "array"
    TIMESTAMP = "timestamp"


class AirbyteSchemaField(BaseModel):
    """Mock class for schema fields in tests."""

    name: str
    type: AirbyteFieldType
    description: Optional[str] = None
    nullable: bool = True
    is_primary_key: bool = False


class AirbyteStreamSchema(BaseModel):
    """Mock class for stream schema in tests."""

    properties: Dict[str, Dict[str, Union[str, List[str]]]]
    required: List[str] = []

    def to_schema_fields(
        self, primary_keys: Optional[List[str]] = None
    ) -> List[AirbyteSchemaField]:
        """Convert JSON schema to schema fields."""
        fields = []
        for name, prop in self.properties.items():
            field_type_str = prop.get("type", "string")

            # Add check to ensure field_type_str is a string before calling upper()
            if isinstance(field_type_str, str):
                field_type = getattr(
                    AirbyteFieldType, field_type_str.upper(), AirbyteFieldType.STRING
                )
            else:
                field_type = AirbyteFieldType.STRING

            # Special handling for date-time format
            if (
                field_type == AirbyteFieldType.STRING
                and prop.get("format") == "date-time"
            ):
                field_type = AirbyteFieldType.TIMESTAMP

            is_required = name in self.required
            is_primary_key = primary_keys and name in primary_keys

            field = AirbyteSchemaField(
                name=name,
                type=field_type,
                description=prop.get("description"),
                nullable=not is_required,
                is_primary_key=is_primary_key,
            )
            fields.append(field)
        return fields


class AirbyteSyncMode(str, Enum):
    """Mock enum for sync modes in tests."""

    FULL_REFRESH = "full_refresh"
    INCREMENTAL = "incremental"
    APPEND = "append"
    OVERWRITE = "overwrite"


class AirbyteStatus(str, Enum):
    """Mock enum for connection status in tests."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    DEPRECATED = "deprecated"


class AirbyteConnectionScheduleType(str, Enum):
    """Mock enum for connection schedule types in tests."""

    MANUAL = "manual"
    BASIC = "basic"
    CRON = "cron"


class AirbyteConnectionStream(BaseModel):
    """Mock class for connection stream in tests."""

    name: str
    namespace: Optional[str] = None
    sync_mode: AirbyteSyncMode = AirbyteSyncMode.FULL_REFRESH
    cursor_field: List[str] = Field(default_factory=list)
    primary_key: List[List[str]] = Field(default_factory=list)
    destination_sync_mode: AirbyteSyncMode = AirbyteSyncMode.OVERWRITE
    selected: bool = True
    stream_schema: Optional[AirbyteStreamSchema] = Field(None, alias="schema")

    class Config:
        """Pydantic configuration."""

        fields = {
            "name": {"alias": "name"},
            "namespace": {"alias": "namespace"},
            "sync_mode": {"alias": "syncMode"},
            "destination_sync_mode": {"alias": "destinationSyncMode"},
            "cursor_field": {"alias": "cursorField"},
            "primary_key": {"alias": "primaryKey"},
        }


class FieldSelection(BaseModel):
    """Mock class for field selection in tests."""

    field_path: List[str]
    selected: bool = True
    additional_fields: List[str] = Field(default_factory=list)


class TestAirbyteWorkspace:
    def test_valid_workspace(self):
        workspace = AirbyteWorkspace(
            workspaceId="test-workspace-id",
            name="Test Workspace",
            workspace_id="test-workspace-id",
        )
        assert workspace.workspace_id == "test-workspace-id"
        assert workspace.name == "Test Workspace"

    def test_from_dict(self):
        data = {
            "workspaceId": "test-workspace-id",
            "name": "Test Workspace",
            "slug": "test-workspace",
            "initialSetupComplete": True,
            "anonymousDataCollection": False,
            "news": False,
            "securityUpdates": True,
            "displaySetupWizard": False,
        }
        workspace = AirbyteWorkspace.parse_obj(data)
        assert workspace.workspace_id == "test-workspace-id"
        assert workspace.name == "Test Workspace"
        assert not hasattr(workspace, "slug")
        assert not hasattr(workspace, "initialSetupComplete")
        assert not hasattr(workspace, "anonymousDataCollection")
        assert not hasattr(workspace, "news")
        assert not hasattr(workspace, "securityUpdates")
        assert not hasattr(workspace, "displaySetupWizard")

    def test_missing_required_fields(self):
        with pytest.raises(ValidationError):
            AirbyteWorkspace.parse_obj({})

    def test_extra_fields_ignored(self):
        data = {
            "workspaceId": "test-workspace-id",
            "name": "Test Workspace",
            "slug": "test-workspace",
            "extra_field": "extra value",
        }
        workspace = AirbyteWorkspace.parse_obj(data)
        assert not hasattr(workspace, "extra_field")


class TestAirbyteSource:
    def test_valid_source(self):
        source = AirbyteDataSource(
            sourceId="test-source-id",
            name="Test Source",
            sourceType="postgres",
            sourceDefinitionId="test-source-definition-id",
            workspaceId="test-workspace-id",
            configuration={"host": "localhost", "port": 5432},
            source_id="test-source-id",
        )
        assert source.source_id == "test-source-id"
        assert source.name == "Test Source"
        assert source.source_type == "postgres"
        assert source.source_definition_id == "test-source-definition-id"
        assert source.workspace_id == "test-workspace-id"
        assert source.configuration.host == "localhost"
        assert source.configuration.port == 5432

    def test_from_dict(self):
        data = {
            "sourceId": "test-source-id",
            "name": "Test Source",
            "sourceType": "postgres",
            "sourceDefinitionId": "test-source-definition-id",
            "workspaceId": "test-workspace-id",
            "configuration": {"host": "localhost", "port": 5432},
        }
        source = AirbyteDataSource.parse_obj(data)
        assert source.source_id == "test-source-id"
        assert source.name == "Test Source"
        assert source.source_type == "postgres"
        assert source.source_definition_id == "test-source-definition-id"
        assert source.workspace_id == "test-workspace-id"
        assert source.configuration.host == "localhost"
        assert source.configuration.port == 5432

    def test_missing_required_fields(self):
        with pytest.raises(ValidationError):
            AirbyteDataSource.parse_obj({})


class TestAirbyteDestination:
    def test_valid_destination(self):
        destination = AirbyteDestination(
            destinationId="test-destination-id",
            name="Test Destination",
            destinationType="postgres",
            destinationDefinitionId="test-destination-definition-id",
            workspaceId="test-workspace-id",
            configuration={"host": "localhost", "port": 5432},
            destination_id="test-destination-id",
        )
        assert destination.destination_id == "test-destination-id"
        assert destination.name == "Test Destination"
        assert destination.destination_type == "postgres"
        assert destination.destination_definition_id == "test-destination-definition-id"
        assert destination.workspace_id == "test-workspace-id"
        assert destination.configuration.host == "localhost"
        assert destination.configuration.port == 5432

    def test_from_dict(self):
        data = {
            "destinationId": "test-destination-id",
            "name": "Test Destination",
            "destinationType": "postgres",
            "destinationDefinitionId": "test-destination-definition-id",
            "workspaceId": "test-workspace-id",
            "configuration": {"host": "localhost", "port": 5432},
        }
        destination = AirbyteDestination.parse_obj(data)
        assert destination.destination_id == "test-destination-id"
        assert destination.name == "Test Destination"
        assert destination.destination_type == "postgres"
        assert destination.destination_definition_id == "test-destination-definition-id"
        assert destination.workspace_id == "test-workspace-id"
        assert destination.configuration.host == "localhost"
        assert destination.configuration.port == 5432

    def test_missing_required_fields(self):
        with pytest.raises(ValidationError):
            AirbyteDestination.parse_obj({})


class TestAirbyteConnection:
    def test_valid_connection(self):
        connection = AirbyteConnection(
            connectionId="test-connection-id",
            name="Test Connection",
            sourceId="test-source-id",
            destinationId="test-destination-id",
            status="active",
            syncCatalog={
                "streams": [
                    {
                        "stream": {
                            "name": "customers",
                            "namespace": "public",
                            "jsonSchema": {
                                "properties": {
                                    "id": {"type": "integer"},
                                    "name": {"type": "string"},
                                }
                            },
                            "supportedSyncModes": ["full_refresh", "incremental"],
                            "sourceDefinedCursor": True,
                            "sourceDefinedPrimaryKey": [["id"]],
                        },
                        "config": {
                            "syncMode": "incremental",
                            "cursorField": ["updated_at"],
                            "destinationSyncMode": "append",
                            "primaryKey": [["id"]],
                            "selected": True,
                        },
                    }
                ]
            },
            scheduleType="basic",
            scheduleData={
                "basicSchedule": {
                    "timeUnit": "hours",
                    "units": 24,
                }
            },
            connection_id="test-connection-id",
            source_id="test-source-id",
            destination_id="test-destination-id",
        )
        assert connection.connection_id == "test-connection-id"
        assert connection.name == "Test Connection"
        assert connection.source_id == "test-source-id"
        assert connection.destination_id == "test-destination-id"
        assert connection.status == "active"
        assert connection.schedule_type == "basic"
        assert connection.schedule_data["basicSchedule"]["timeUnit"] == "hours"
        assert connection.schedule_data["basicSchedule"]["units"] == 24
        assert len(connection.sync_catalog.streams) == 1

    def test_from_dict(self):
        data = {
            "connectionId": "test-connection-id",
            "name": "Test Connection",
            "sourceId": "test-source-id",
            "destinationId": "test-destination-id",
            "status": "active",
            "syncCatalog": {
                "streams": [
                    {
                        "stream": {
                            "name": "customers",
                            "namespace": "public",
                            "jsonSchema": {
                                "properties": {
                                    "id": {"type": "integer"},
                                    "name": {"type": "string"},
                                }
                            },
                            "supportedSyncModes": ["full_refresh", "incremental"],
                            "sourceDefinedCursor": True,
                            "sourceDefinedPrimaryKey": [["id"]],
                        },
                        "config": {
                            "syncMode": "incremental",
                            "cursorField": ["updated_at"],
                            "destinationSyncMode": "append",
                            "primaryKey": [["id"]],
                            "selected": True,
                        },
                    }
                ]
            },
            "scheduleType": "manual",
            "namespaceDefinition": "source",
            "namespaceFormat": "${SOURCE_NAMESPACE}",
            "prefix": "",
            "operationIds": [],
            "resourceRequirements": {
                "cpu_request": "",
                "cpu_limit": "",
                "memory_request": "",
                "memory_limit": "",
            },
        }
        connection = AirbyteConnection.parse_obj(data)
        assert connection.connection_id == "test-connection-id"
        assert connection.name == "Test Connection"
        assert connection.source_id == "test-source-id"
        assert connection.destination_id == "test-destination-id"
        assert connection.status == "active"
        assert connection.schedule_type == "manual"
        assert len(connection.sync_catalog.streams) == 1

    def test_missing_required_fields(self):
        with pytest.raises(ValidationError):
            AirbyteConnection.parse_obj({})


if __name__ == "__main__":
    unittest.main()
