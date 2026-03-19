import pytest
from pydantic import ValidationError

from datahub.ingestion.source.airbyte.models import (
    AirbyteConnection,
    AirbyteDataSource,
    AirbyteDestination,
    AirbyteWorkspace,
)


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
        workspace = AirbyteWorkspace.model_validate(data)
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
            AirbyteWorkspace.model_validate({})

    def test_extra_fields_ignored(self):
        data = {
            "workspaceId": "test-workspace-id",
            "name": "Test Workspace",
            "slug": "test-workspace",
            "extra_field": "extra value",
        }
        workspace = AirbyteWorkspace.model_validate(data)
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
        source = AirbyteDataSource.model_validate(data)
        assert source.source_id == "test-source-id"
        assert source.name == "Test Source"
        assert source.source_type == "postgres"
        assert source.source_definition_id == "test-source-definition-id"
        assert source.workspace_id == "test-workspace-id"
        assert source.configuration.host == "localhost"
        assert source.configuration.port == 5432

    def test_missing_required_fields(self):
        with pytest.raises(ValidationError):
            AirbyteDataSource.model_validate({})


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
        destination = AirbyteDestination.model_validate(data)
        assert destination.destination_id == "test-destination-id"
        assert destination.name == "Test Destination"
        assert destination.destination_type == "postgres"
        assert destination.destination_definition_id == "test-destination-definition-id"
        assert destination.workspace_id == "test-workspace-id"
        assert destination.configuration.host == "localhost"
        assert destination.configuration.port == 5432

    def test_missing_required_fields(self):
        with pytest.raises(ValidationError):
            AirbyteDestination.model_validate({})


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
        connection = AirbyteConnection.model_validate(data)
        assert connection.connection_id == "test-connection-id"
        assert connection.name == "Test Connection"
        assert connection.source_id == "test-source-id"
        assert connection.destination_id == "test-destination-id"
        assert connection.status == "active"
        assert connection.schedule_type == "manual"
        assert len(connection.sync_catalog.streams) == 1

    def test_missing_required_fields(self):
        with pytest.raises(ValidationError):
            AirbyteConnection.model_validate({})
