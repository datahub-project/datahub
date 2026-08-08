"""Incident management tools for DataHub MCP server.

These tools let agents close the operational loop on data incidents:
list the incidents on an asset, raise a new incident when a problem is
found, and resolve an incident once it has been remediated.

Configuration via environment variables:
- INCIDENT_TOOLS_ENABLED: Set to "false" to disable the mutation tools
  (raise_incident, resolve_incident). Default: enabled. Also requires
  TOOLS_IS_MUTATION_ENABLED enabled. list_incidents is read-only and is
  not affected.
"""

import logging
import os
from typing import Any, Literal, Optional, get_args

from datahub_agent_context.context import get_graph
from datahub_agent_context.mcp_tools.base import clean_gql_response, execute_graphql
from datahub_agent_context.mcp_tools.helpers import (
    _select_results_within_budget,
    truncate_descriptions,
)

logger = logging.getLogger(__name__)

DEFAULT_PAGE_SIZE = 20
MAX_PAGE_SIZE = 50

# Entity types that expose the `incidents` field in the GraphQL schema.
SUPPORTED_ENTITY_TYPES = ("Dataset", "DataJob", "DataFlow", "Dashboard", "Chart")

IncidentState = Literal["ACTIVE", "RESOLVED"]
# CUSTOM is excluded because it additionally requires a customType, which this
# tool does not expose.
IncidentType = Literal[
    "OPERATIONAL", "FRESHNESS", "VOLUME", "FIELD", "SQL", "DATA_SCHEMA"
]
IncidentPriority = Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"]

# The five inline fragments below mirror the exact set of entity types that
# expose the `incidents` field in the GraphQL schema (incident.graphql extends
# Dataset, DataJob, DataFlow, Dashboard, and Chart); there is no shared
# interface for the field, so each type is spelled out.
LIST_INCIDENTS_QUERY = """
query listEntityIncidents(
    $urn: String!,
    $state: IncidentState,
    $start: Int!,
    $count: Int!
) {
    entity(urn: $urn) {
        urn
        type
        ... on Dataset {
            incidents(state: $state, start: $start, count: $count) {
                ...entityIncidentsResultFields
            }
        }
        ... on DataJob {
            incidents(state: $state, start: $start, count: $count) {
                ...entityIncidentsResultFields
            }
        }
        ... on DataFlow {
            incidents(state: $state, start: $start, count: $count) {
                ...entityIncidentsResultFields
            }
        }
        ... on Dashboard {
            incidents(state: $state, start: $start, count: $count) {
                ...entityIncidentsResultFields
            }
        }
        ... on Chart {
            incidents(state: $state, start: $start, count: $count) {
                ...entityIncidentsResultFields
            }
        }
    }
}

fragment entityIncidentsResultFields on EntityIncidentsResult {
    start
    count
    total
    incidents {
        urn
        incidentType
        customType
        title
        description
        priority
        incidentStatus {
            state
            stage
            message
            lastUpdated {
                time
                actor
            }
        }
        source {
            type
        }
        created {
            time
            actor
        }
    }
}
"""

RAISE_INCIDENT_MUTATION = """
mutation raiseIncident($input: RaiseIncidentInput!) {
    raiseIncident(input: $input)
}
"""

UPDATE_INCIDENT_STATUS_MUTATION = """
mutation updateIncidentStatus($urn: String!, $input: IncidentStatusInput!) {
    updateIncidentStatus(urn: $urn, input: $input)
}
"""


def _is_incident_tools_enabled() -> bool:
    """Check if the incident mutation tools are enabled (default: True)."""
    value = os.environ.get("INCIDENT_TOOLS_ENABLED", "true")
    return value.lower() in ("true", "1", "yes")


def _build_incident_summary(incident: dict[str, Any]) -> dict[str, Any]:
    """Extract a concise summary from a raw incident."""
    status = incident.get("incidentStatus") or {}
    last_updated = status.get("lastUpdated") or {}
    source = incident.get("source") or {}
    created = incident.get("created") or {}

    summary: dict[str, Any] = {
        "urn": incident.get("urn"),
        "type": incident.get("incidentType"),
        "customType": incident.get("customType"),
        "title": incident.get("title"),
        "description": incident.get("description"),
        "priority": incident.get("priority"),
        "state": status.get("state"),
        "stage": status.get("stage"),
        "statusMessage": status.get("message"),
        "statusLastUpdatedMillis": last_updated.get("time"),
        "source": source.get("type"),
        "createdMillis": created.get("time"),
        "createdBy": created.get("actor"),
    }
    return summary


def list_incidents(
    urn: str,
    state: Optional[IncidentState] = "ACTIVE",
    start: int = 0,
    count: int = DEFAULT_PAGE_SIZE,
) -> dict[str, Any]:
    """List incidents associated with a data asset.

    Fetches incidents (data quality or operational issues) raised on an asset,
    including their type, state, priority, and status history. Use this to
    understand whether an asset currently has known problems before relying on
    it, or to find an incident to update after remediating an issue.

    Supported entity types: datasets, data jobs, data flows, dashboards, charts.

    Args:
        urn: Entity URN (e.g. urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD))
        state: Optional incident state to filter by:
              - "ACTIVE": Only ongoing incidents (default)
              - "RESOLVED": Only resolved incidents
              - None: Incidents in any state
        start: Pagination offset (default 0)
        count: Number of incidents to return per page (default 20, max 50)

    Returns:
        Dictionary with:
        - success: Whether the request was successful
        - data: Contains start, count, total, and incidents list
        - message: Summary message

    Each incident includes: urn, type, customType, title, description, priority,
    state, stage, statusMessage, statusLastUpdatedMillis, source, createdMillis,
    createdBy.

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client):
            result = list_incidents(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)"
            )
    """
    graph = get_graph()

    if not urn:
        raise ValueError("urn cannot be empty")

    start = max(0, start)
    count = max(1, min(count, MAX_PAGE_SIZE))

    try:
        result = execute_graphql(
            graph,
            query=LIST_INCIDENTS_QUERY,
            variables={
                "urn": urn,
                "state": state,
                "start": start,
                "count": count,
            },
            operation_name="listEntityIncidents",
        )
    except Exception as e:
        raise RuntimeError(f"Error fetching incidents for {urn}: {e}") from e

    entity_data = result.get("entity")
    if not entity_data:
        return {
            "success": False,
            "data": None,
            "message": f"Entity not found: {urn}",
        }

    if "incidents" not in entity_data:
        return {
            "success": False,
            "data": None,
            "message": (
                f"Entity type '{entity_data.get('type')}' does not support incidents. "
                f"Supported types: {', '.join(SUPPORTED_ENTITY_TYPES)}"
            ),
        }

    incidents_result = entity_data.get("incidents") or {}
    raw_incidents = incidents_result.get("incidents") or []
    summaries = [_build_incident_summary(i) for i in raw_incidents if i.get("urn")]

    summaries = [clean_gql_response(s) for s in summaries]
    truncate_descriptions(summaries)
    selected = list(
        _select_results_within_budget(
            results=iter(summaries),
            fetch_entity=lambda s: s,
            max_results=len(summaries),
        )
    )

    total = incidents_result.get("total", 0)
    state_label = state.lower() if state else "any-state"
    response: dict[str, Any] = {
        "success": True,
        "data": {
            "start": incidents_result.get("start", start),
            "count": len(selected),
            "total": total,
            "incidents": selected,
        },
        "message": f"Found {total} {state_label} incidents for entity",
    }
    if len(selected) < len(summaries):
        response["data"]["truncatedDueToTokenBudget"] = True
    return response


def raise_incident(
    urn: str,
    title: str,
    description: str,
    incident_type: IncidentType = "OPERATIONAL",
    priority: Optional[IncidentPriority] = None,
) -> dict[str, Any]:
    """Raise a new incident on a data asset.

    Creates an incident in the ACTIVE state on the given asset. The incident
    appears in the asset's Incidents tab in the DataHub UI and can be used to
    warn consumers of the asset about known problems. Use this after diagnosing
    an issue (e.g. a failing assertion, stale data, or a broken pipeline) to
    make the problem visible to the asset's owners and consumers.

    ⚠️  IMPORTANT: Before calling this tool, you SHOULD confirm with the user
    that they want to raise this incident. Present the title, description, and
    affected asset, and ask for their approval before proceeding.

    Args:
        urn: URN of the asset the incident applies to
            (e.g. urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD))
        title: A short, descriptive title for the incident
            (e.g. "Stale data: table not updated since 2024-01-01")
        description: A longer description of the problem, its impact, and any
            relevant context (supports markdown formatting)
        incident_type: The type of incident (default "OPERATIONAL"):
            - "OPERATIONAL": Failure to materialize a dataset or execute a pipeline
            - "FRESHNESS": The asset is not up to date
            - "VOLUME": The asset has too few or too many rows
            - "FIELD": A column contains invalid values
            - "SQL": A SQL-based check has failed
            - "DATA_SCHEMA": The schema has changed incompatibly
        priority: Optional priority ("LOW", "MEDIUM", "HIGH", "CRITICAL")

    Returns:
        Dictionary with:
        - success: Whether the incident was raised
        - incidentUrn: URN of the newly created incident
        - urn: The asset URN the incident was raised on
        - message: Success message

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client):
            result = raise_incident(
                urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.table,PROD)",
                title="Null spike in customer_email",
                description="23% of rows have null customer_email since the 06-01 load.",
                incident_type="FIELD",
                priority="HIGH",
            )
    """
    if not _is_incident_tools_enabled():
        return {
            "success": False,
            "incidentUrn": None,
            "urn": urn,
            "message": "Incident tools are disabled via INCIDENT_TOOLS_ENABLED",
        }

    graph = get_graph()

    if not urn:
        raise ValueError("urn cannot be empty")
    if not title or not title.strip():
        raise ValueError("title cannot be empty")
    if not description or not description.strip():
        raise ValueError("description cannot be empty")

    valid_incident_types = get_args(IncidentType)
    if incident_type not in valid_incident_types:
        raise ValueError(
            f"Invalid incident_type '{incident_type}'. "
            f"Must be one of: {', '.join(valid_incident_types)}"
        )

    valid_priorities = get_args(IncidentPriority)
    if priority is not None and priority not in valid_priorities:
        raise ValueError(
            f"Invalid priority '{priority}'. "
            f"Must be one of: {', '.join(valid_priorities)}"
        )

    incident_input: dict[str, Any] = {
        "type": incident_type,
        "title": title,
        "description": description,
        "resourceUrn": urn,
    }
    if priority is not None:
        incident_input["priority"] = priority

    try:
        result = execute_graphql(
            graph,
            query=RAISE_INCIDENT_MUTATION,
            variables={"input": incident_input},
            operation_name="raiseIncident",
        )

        incident_urn = result.get("raiseIncident")
        if not incident_urn:
            raise RuntimeError(
                f"Failed to raise incident on {urn} - no incident urn returned"
            )

        return {
            "success": True,
            "incidentUrn": incident_urn,
            "urn": urn,
            "message": f"Successfully raised {incident_type} incident: {title}",
        }

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(f"Error raising incident on {urn}: {str(e)}") from e


def resolve_incident(
    incident_urn: str,
    message: str,
) -> dict[str, Any]:
    """Resolve an existing incident on a data asset.

    Transitions an incident to the RESOLVED state with a resolution message
    explaining what was done. Use this after remediating the underlying
    problem, so that the asset's owners and consumers can see it has been
    addressed. Use list_incidents first to find the incident URN.

    Args:
        incident_urn: URN of the incident to resolve
            (e.g. "urn:li:incident:9caa8a45-42c4-45ec-91d5-9cbbb5f2be40").
            Obtain this from list_incidents or from the response of raise_incident.
        message: A resolution message describing what was done to fix the
            problem (e.g. "Backfilled missing partitions for 2024-06-01..03")

    Returns:
        Dictionary with:
        - success: Whether the incident was resolved
        - incidentUrn: The incident URN
        - message: Success message

    Example:
        from datahub_agent_context.context import DataHubContext

        with DataHubContext(client):
            result = resolve_incident(
                incident_urn="urn:li:incident:9caa8a45-42c4-45ec-91d5-9cbbb5f2be40",
                message="Upstream pipeline fixed and data backfilled.",
            )
    """
    if not _is_incident_tools_enabled():
        return {
            "success": False,
            "incidentUrn": incident_urn,
            "message": "Incident tools are disabled via INCIDENT_TOOLS_ENABLED",
        }

    graph = get_graph()

    if not incident_urn:
        raise ValueError("incident_urn cannot be empty")
    # Strict prefix check (same convention as save_document's urn validation):
    # catches the likely agent mistake of passing the asset urn instead of the
    # incident urn, since both appear in list_incidents output.
    if not incident_urn.startswith("urn:li:incident:"):
        raise ValueError(
            f"Invalid incident_urn format '{incident_urn}'. "
            "Must start with 'urn:li:incident:'"
        )
    if not message or not message.strip():
        raise ValueError("message cannot be empty")

    try:
        result = execute_graphql(
            graph,
            query=UPDATE_INCIDENT_STATUS_MUTATION,
            variables={
                "urn": incident_urn,
                "input": {
                    "state": "RESOLVED",
                    "message": message,
                },
            },
            operation_name="updateIncidentStatus",
        )

        if result.get("updateIncidentStatus", False):
            return {
                "success": True,
                "incidentUrn": incident_urn,
                "message": f"Successfully resolved incident {incident_urn}",
            }
        else:
            raise RuntimeError(
                f"Failed to resolve incident {incident_urn} - operation returned false"
            )

    except Exception as e:
        if isinstance(e, RuntimeError):
            raise
        raise RuntimeError(f"Error resolving incident {incident_urn}: {str(e)}") from e
