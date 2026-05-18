import logging
from typing import Optional

from datahub.ingestion.source.airbyte.client import (
    AirbyteBaseClient,
    create_airbyte_client,
)
from datahub.ingestion.source.airbyte.config import (
    AirbyteClientConfig,
    AirbyteDeploymentType,
)
from datahub.ingestion.source.airbyte.models import (
    AirbyteConnectionPartial,
    AirbyteTestResult,
    AirbyteWorkspacePartial,
)

logger = logging.getLogger(__name__)


def _test_workspaces(
    client: AirbyteBaseClient, config: AirbyteClientConfig
) -> AirbyteTestResult:
    try:
        workspaces = client.list_workspaces()
        if not isinstance(workspaces, list):
            return AirbyteTestResult(
                success=False,
                error_message="Unable to retrieve workspaces from Airbyte API: expected a list response",
            )

        if not workspaces:
            if config.deployment_type == AirbyteDeploymentType.CLOUD:
                return AirbyteTestResult(
                    success=False,
                    error_message=f"No workspaces found with ID {config.cloud_workspace_id}. Please check your configuration.",
                )
            else:
                return AirbyteTestResult(
                    success=False,
                    error_message="No workspaces found in Airbyte instance. Please check your configuration.",
                )

        return AirbyteTestResult(success=True, data=workspaces[0])
    except Exception as e:
        return AirbyteTestResult(
            success=False, error_message=f"Error retrieving workspaces: {str(e)}"
        )


def _test_connections(
    client: AirbyteBaseClient, workspace_id: str
) -> AirbyteTestResult:
    try:
        connections = client.list_connections(workspace_id)
        if not isinstance(connections, list):
            return AirbyteTestResult(
                success=False,
                error_message="Unable to retrieve connections from Airbyte API: expected a list response",
            )

        if not connections:
            return AirbyteTestResult(success=True)

        return AirbyteTestResult(success=True, data=connections[0])
    except Exception as e:
        return AirbyteTestResult(
            success=False,
            error_message=f"Failed to list connections for workspace {workspace_id}: {str(e)}",
        )


def _test_source(client: AirbyteBaseClient, source_id: str) -> AirbyteTestResult:
    try:
        client.get_source(source_id)
        return AirbyteTestResult(success=True)
    except Exception as e:
        return AirbyteTestResult(
            success=False,
            error_message=f"Failed to retrieve source {source_id}: {str(e)}",
        )


def _test_destination(
    client: AirbyteBaseClient, destination_id: str
) -> AirbyteTestResult:
    try:
        client.get_destination(destination_id)
        return AirbyteTestResult(success=True)
    except Exception as e:
        return AirbyteTestResult(
            success=False,
            error_message=f"Failed to retrieve destination {destination_id}: {str(e)}",
        )


def _test_jobs(client: AirbyteBaseClient, connection_id: str) -> AirbyteTestResult:
    try:
        client.list_jobs(connection_id, limit=5)
        return AirbyteTestResult(success=True)
    except Exception as e:
        return AirbyteTestResult(
            success=False,
            error_message=f"Failed to retrieve jobs for connection {connection_id}: {str(e)}",
        )


def test_connection(config: AirbyteClientConfig) -> Optional[str]:
    """Test the connection to the Airbyte API; returns None on success, or an
    error message describing the first failure encountered."""
    try:
        if not config.verify_ssl:
            logger.warning("SSL certificate verification is disabled")

        client = create_airbyte_client(config)

        workspace_result = _test_workspaces(client, config)
        if not workspace_result.success:
            return workspace_result.error_message

        if workspace_result.data is None:
            return "Unexpected error: workspace data is missing"

        if not isinstance(workspace_result.data, AirbyteWorkspacePartial):
            return f"Unexpected workspace data type: {type(workspace_result.data)}"

        workspace_id = workspace_result.data.workspace_id
        logger.info("Testing connection using workspace: %s", workspace_id)

        connection_result = _test_connections(client, workspace_id)
        if not connection_result.success:
            return connection_result.error_message

        if connection_result.data is not None:
            if not isinstance(connection_result.data, AirbyteConnectionPartial):
                return (
                    f"Unexpected connection data type: {type(connection_result.data)}"
                )

            connection_id = connection_result.data.connection_id

            source_id = connection_result.data.source_id
            source_result = _test_source(client, source_id)
            if not source_result.success:
                return source_result.error_message

            dest_id = connection_result.data.destination_id
            dest_result = _test_destination(client, dest_id)
            if not dest_result.success:
                return dest_result.error_message

            jobs_result = _test_jobs(client, connection_id)
            if not jobs_result.success:
                return jobs_result.error_message

        logger.info("Successfully connected to Airbyte %s API", config.deployment_type)
        return None
    except Exception as e:
        error_message = (
            f"Failed to connect to Airbyte {config.deployment_type}: {str(e)}"
        )
        logger.error(error_message)
        return error_message
