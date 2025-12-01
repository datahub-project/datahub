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
from datahub.ingestion.source.airbyte.models import AirbyteTestResult

logger = logging.getLogger(__name__)


def _log_authentication_info(config: AirbyteClientConfig) -> None:
    """
    Log information about the authentication method being used

    Args:
        config: The Airbyte client configuration
    """
    if config.deployment_type == AirbyteDeploymentType.OPEN_SOURCE:
        if config.api_key:
            logger.info("Using API key/token for authentication")
        elif config.username:
            logger.info("Using basic authentication with username and password")
        else:
            logger.info(
                "No authentication credentials provided for Open Source deployment"
            )
    else:  # Cloud deployment
        if config.oauth2_client_id and config.oauth2_refresh_token:
            logger.info("Using OAuth2 authentication for Airbyte Cloud")
        else:
            logger.info("OAuth2 credentials incomplete for Airbyte Cloud")


def _log_ssl_settings(config: AirbyteClientConfig) -> None:
    """
    Log information about the SSL verification settings

    Args:
        config: The Airbyte client configuration
    """
    if not config.verify_ssl:
        logger.warning("SSL certificate verification is disabled")
    elif config.ssl_ca_cert:
        logger.info(f"Using custom CA certificate: {config.ssl_ca_cert}")


def _test_workspaces(
    client: AirbyteBaseClient, config: AirbyteClientConfig
) -> AirbyteTestResult:
    """
    Test listing workspaces and validate the response

    Args:
        client: The Airbyte client
        config: The Airbyte client configuration

    Returns:
        AirbyteTestResult with success flag, error message (if any), and first workspace data
    """
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
    """
    Test listing connections for a workspace and validate the response

    Args:
        client: The Airbyte client
        workspace_id: The workspace ID

    Returns:
        AirbyteTestResult with success flag, error message (if any), and first connection data
    """
    try:
        connections = client.list_connections(workspace_id)
        if not isinstance(connections, list):
            return AirbyteTestResult(
                success=False,
                error_message="Unable to retrieve connections from Airbyte API: expected a list response",
            )

        logger.info(
            f"Successfully retrieved {len(connections)} connections from workspace {workspace_id}"
        )

        if not connections:
            # Not having connections is not an error, just return None for the connection
            return AirbyteTestResult(success=True)

        return AirbyteTestResult(success=True, data=connections[0])
    except Exception as e:
        return AirbyteTestResult(
            success=False,
            error_message=f"Failed to list connections for workspace {workspace_id}: {str(e)}",
        )


def _test_source(client: AirbyteBaseClient, source_id: str) -> AirbyteTestResult:
    """
    Test retrieving a source and validate the response

    Args:
        client: The Airbyte client
        source_id: The source ID

    Returns:
        AirbyteTestResult with success flag and error message (if any)
    """
    try:
        client.get_source(source_id)
        logger.info(f"Successfully retrieved source {source_id}")
        return AirbyteTestResult(success=True)
    except Exception as e:
        return AirbyteTestResult(
            success=False,
            error_message=f"Failed to retrieve source {source_id}: {str(e)}",
        )


def _test_destination(
    client: AirbyteBaseClient, destination_id: str
) -> AirbyteTestResult:
    """
    Test retrieving a destination and validate the response

    Args:
        client: The Airbyte client
        destination_id: The destination ID

    Returns:
        AirbyteTestResult with success flag and error message (if any)
    """
    try:
        client.get_destination(destination_id)
        logger.info(f"Successfully retrieved destination {destination_id}")
        return AirbyteTestResult(success=True)
    except Exception as e:
        return AirbyteTestResult(
            success=False,
            error_message=f"Failed to retrieve destination {destination_id}: {str(e)}",
        )


def _test_jobs(client: AirbyteBaseClient, connection_id: str) -> AirbyteTestResult:
    """
    Test listing jobs for a connection and validate the response

    Args:
        client: The Airbyte client
        connection_id: The connection ID

    Returns:
        AirbyteTestResult with success flag and error message (if any)
    """
    try:
        jobs = client.list_jobs(connection_id, limit=5)
        logger.info(
            f"Successfully retrieved {len(jobs)} jobs for connection {connection_id}"
        )
        return AirbyteTestResult(success=True)
    except Exception as e:
        return AirbyteTestResult(
            success=False,
            error_message=f"Failed to retrieve jobs for connection {connection_id}: {str(e)}",
        )


def test_connection(config: AirbyteClientConfig) -> Optional[str]:
    """
    Test the connection to the Airbyte API using the provided configuration.
    Works with both Open Source and Cloud deployments.

    Args:
        config: The Airbyte client configuration

    Returns:
        Optional[str]: None if successful, or an error message if connection fails
    """
    try:
        logger.info(f"Testing connection to Airbyte {config.deployment_type} API")

        # Log configuration information
        _log_authentication_info(config)
        _log_ssl_settings(config)

        # Create the appropriate client
        client = create_airbyte_client(config)

        # Test listing workspaces
        workspace_result = _test_workspaces(client, config)
        if not workspace_result.success:
            return workspace_result.error_message

        # Check if workspace is None (should never happen but avoids type error)
        if workspace_result.data is None:
            return "Unexpected error: workspace data is missing"

        workspace_id = workspace_result.data.get("workspaceId")
        if not workspace_id:
            return "No workspace ID found in the first workspace"

        logger.info(f"Testing connection using workspace: {workspace_id}")

        # Test listing connections
        connection_result = _test_connections(client, workspace_id)
        if not connection_result.success:
            return connection_result.error_message

        # If we have a connection, test its details
        if connection_result.data is not None:
            connection_id = connection_result.data.get("connectionId")

            if connection_id:
                # Test getting source details
                source_id = connection_result.data.get("sourceId")
                if source_id:
                    source_result = _test_source(client, source_id)
                    if not source_result.success:
                        return source_result.error_message

                # Test getting destination details
                dest_id = connection_result.data.get("destinationId")
                if dest_id:
                    dest_result = _test_destination(client, dest_id)
                    if not dest_result.success:
                        return dest_result.error_message

                # Test getting jobs
                jobs_result = _test_jobs(client, connection_id)
                if not jobs_result.success:
                    return jobs_result.error_message

        logger.info(f"Successfully connected to Airbyte {config.deployment_type} API")
        return None
    except Exception as e:
        error_message = (
            f"Failed to connect to Airbyte {config.deployment_type}: {str(e)}"
        )
        logger.error(error_message)
        return error_message
