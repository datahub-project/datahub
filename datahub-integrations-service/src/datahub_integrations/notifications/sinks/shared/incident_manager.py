"""
Shared incident management service for handling incident resolution and reopening.

This service provides common functionality for both Teams and Slack integrations
to resolve and reopen incidents through DataHub's GraphQL API.
"""

import logging
from dataclasses import dataclass
from typing import Optional

from datahub.ingestion.graph.client import DataHubGraph

from datahub_integrations.graphql.incidents import UPDATE_INCIDENT_STATUS_MUTATION

logger = logging.getLogger(__name__)


@dataclass
class IncidentActionResult:
    """Result of an incident action (resolve/reopen)."""

    success: bool
    message: str
    incident_urn: str
    new_status: Optional[str] = None


class IncidentManager:
    """
    Shared service for managing incident lifecycle operations.

    This class provides common functionality for resolving and reopening incidents
    that can be used by both Teams and Slack integrations.
    """

    def __init__(self, graph: DataHubGraph):
        """
        Initialize the incident manager.

        Args:
            graph: DataHub GraphQL client for making API calls
        """
        self.graph = graph

    def _get_impersonation_graph(self, user_urn: Optional[str] = None) -> DataHubGraph:
        """
        Get a DataHub graph client with optional user impersonation.

        Args:
            user_urn: Optional user URN to impersonate. If None, uses system user.

        Returns:
            DataHubGraph client with appropriate impersonation headers
        """
        if user_urn:
            # Create a graph client with user impersonation (same as Slack implementation)
            return DataHubGraph(
                self.graph.config.model_copy(
                    update={"extra_headers": {"X-DataHub-Impersonated-Urn": user_urn}},
                    deep=True,
                )
            )
        else:
            # Use system user (same as Slack implementation)
            return self.graph

    async def resolve_incident(
        self,
        incident_urn: str,
        actor_urn: Optional[str] = None,
        message: str = "",
        stage: Optional[str] = None,
    ) -> IncidentActionResult:
        """
        Resolve an incident by updating its status to RESOLVED.

        Args:
            incident_urn: The URN of the incident to resolve
            actor_urn: Optional URN of the user performing the action
            message: Optional resolution message
            stage: Optional incident stage

        Returns:
            IncidentActionResult with the result of the operation
        """
        try:
            logger.info(f"Resolving incident {incident_urn} by {actor_urn or 'system'}")

            # Get impersonation graph (same as Slack implementation)
            impersonation_graph = self._get_impersonation_graph(actor_urn)

            # Use the same GraphQL mutation as Slack
            input_data = {"state": "RESOLVED", "message": message, "stage": stage}

            result = impersonation_graph.execute_graphql(
                UPDATE_INCIDENT_STATUS_MUTATION,
                variables={"urn": incident_urn, "input": input_data},
            )

            if result:
                logger.info(f"Successfully resolved incident {incident_urn}")
                return IncidentActionResult(
                    success=True,
                    message="Incident has been resolved successfully",
                    incident_urn=incident_urn,
                    new_status="RESOLVED",
                )
            else:
                logger.error(
                    f"Failed to resolve incident {incident_urn}: GraphQL mutation returned no result"
                )
                return IncidentActionResult(
                    success=False,
                    message="Failed to resolve incident - GraphQL mutation returned no result",
                    incident_urn=incident_urn,
                )

        except Exception as e:
            logger.error(f"Error resolving incident {incident_urn}: {str(e)}")
            return IncidentActionResult(
                success=False,
                message=f"Error resolving incident: {str(e)}",
                incident_urn=incident_urn,
            )

    async def reopen_incident(
        self, incident_urn: str, actor_urn: Optional[str] = None
    ) -> IncidentActionResult:
        """
        Reopen an incident by updating its status to ACTIVE.

        Args:
            incident_urn: The URN of the incident to reopen
            actor_urn: Optional URN of the user performing the action

        Returns:
            IncidentActionResult with the result of the operation
        """
        try:
            logger.info(f"Reopening incident {incident_urn} by {actor_urn or 'system'}")

            # Get impersonation graph (same as Slack implementation)
            impersonation_graph = self._get_impersonation_graph(actor_urn)

            # Use the same GraphQL mutation as Slack
            input_data = {
                "state": "ACTIVE",
            }

            result = impersonation_graph.execute_graphql(
                UPDATE_INCIDENT_STATUS_MUTATION,
                variables={"urn": incident_urn, "input": input_data},
            )

            if result:
                logger.info(f"Successfully reopened incident {incident_urn}")
                return IncidentActionResult(
                    success=True,
                    message="Incident has been reopened successfully",
                    incident_urn=incident_urn,
                    new_status="ACTIVE",
                )
            else:
                logger.error(
                    f"Failed to reopen incident {incident_urn}: GraphQL mutation returned no result"
                )
                return IncidentActionResult(
                    success=False,
                    message="Failed to reopen incident - GraphQL mutation returned no result",
                    incident_urn=incident_urn,
                )

        except Exception as e:
            logger.error(f"Error reopening incident {incident_urn}: {str(e)}")
            return IncidentActionResult(
                success=False,
                message=f"Error reopening incident: {str(e)}",
                incident_urn=incident_urn,
            )
