# ABOUTME: Helper functions for common metadata operations like adding/removing tags and terms.
# ABOUTME: Reduces boilerplate GraphQL code in smoke tests.

import logging
import time
from typing import Any, Dict, Optional

from datahub.cli.cli_utils import get_entity as get_entity_cli
from tests.utils import execute_graphql, with_test_retry

logger = logging.getLogger(__name__)


def get_entity(
    auth_session,
    urn: str,
) -> Dict[str, Any]:
    return get_entity_cli(
        session=auth_session,
        gms_host=auth_session.gms_url(),
        urn=urn,
    )


def add_tag(
    auth_session,
    resource_urn: str,
    tag_urn: str,
    sub_resource: Optional[str] = None,
    sub_resource_type: Optional[str] = None,
) -> bool:
    """Add a tag to a resource."""
    variables: Dict[str, Any] = {
        "input": {"tagUrn": tag_urn, "resourceUrn": resource_urn}
    }
    if sub_resource:
        variables["input"]["subResource"] = sub_resource
    if sub_resource_type:
        variables["input"]["subResourceType"] = sub_resource_type

    query = """mutation addTag($input: TagAssociationInput!) {
        addTag(input: $input)
    }"""

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["addTag"]


def remove_tag(
    auth_session,
    resource_urn: str,
    tag_urn: str,
    sub_resource: Optional[str] = None,
    sub_resource_type: Optional[str] = None,
) -> bool:
    """Remove a tag from a resource."""
    variables: Dict[str, Any] = {
        "input": {"tagUrn": tag_urn, "resourceUrn": resource_urn}
    }
    if sub_resource:
        variables["input"]["subResource"] = sub_resource
    if sub_resource_type:
        variables["input"]["subResourceType"] = sub_resource_type

    query = """mutation removeTag($input: TagAssociationInput!) {
        removeTag(input: $input)
    }"""

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["removeTag"]


def add_term(
    auth_session,
    resource_urn: str,
    term_urn: str,
    sub_resource: Optional[str] = None,
    sub_resource_type: Optional[str] = None,
) -> bool:
    """Add a glossary term to a resource."""
    variables: Dict[str, Any] = {
        "input": {"termUrn": term_urn, "resourceUrn": resource_urn}
    }
    if sub_resource:
        variables["input"]["subResource"] = sub_resource
    if sub_resource_type:
        variables["input"]["subResourceType"] = sub_resource_type

    query = """mutation addTerm($input: TermAssociationInput!) {
        addTerm(input: $input)
    }"""

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["addTerm"]


def remove_term(
    auth_session,
    resource_urn: str,
    term_urn: str,
    sub_resource: Optional[str] = None,
    sub_resource_type: Optional[str] = None,
) -> bool:
    """Remove a glossary term from a resource."""
    variables: Dict[str, Any] = {
        "input": {"termUrn": term_urn, "resourceUrn": resource_urn}
    }
    if sub_resource:
        variables["input"]["subResource"] = sub_resource
    if sub_resource_type:
        variables["input"]["subResourceType"] = sub_resource_type

    query = """mutation removeTerm($input: TermAssociationInput!) {
        removeTerm(input: $input)
    }"""

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["removeTerm"]


def update_description(
    auth_session,
    resource_urn: str,
    description: str,
    sub_resource: Optional[str] = None,
    sub_resource_type: Optional[str] = None,
) -> bool:
    """Update resource description."""
    variables: Dict[str, Any] = {
        "input": {
            "description": description,
            "resourceUrn": resource_urn,
        }
    }
    if sub_resource:
        variables["input"]["subResource"] = sub_resource
    if sub_resource_type:
        variables["input"]["subResourceType"] = sub_resource_type

    query = """mutation updateDescription($input: DescriptionUpdateInput!) {
        updateDescription(input: $input)
    }"""

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["updateDescription"]


# Read-only operations with retry logic


@with_test_retry()
def get_search_results(auth_session, entity_type: str) -> Dict[str, Any]:
    """Search for entities by type."""
    entity_type_map = {
        "chart": "CHART",
        "dataset": "DATASET",
        "dashboard": "DASHBOARD",
        "dataJob": "DATA_JOB",
        "dataFlow": "DATA_FLOW",
        "dataProduct": "DATA_PRODUCT",
        "container": "CONTAINER",
        "tag": "TAG",
        "corpUser": "CORP_USER",
        "mlFeature": "MLFEATURE",
        "glossaryTerm": "GLOSSARY_TERM",
        "domain": "DOMAIN",
        "mlPrimaryKey": "MLPRIMARY_KEY",
        "corpGroup": "CORP_GROUP",
        "mlFeatureTable": "MLFEATURE_TABLE",
        "glossaryNode": "GLOSSARY_NODE",
        "mlModel": "MLMODEL",
    }

    query = """
        query search($input: SearchInput!) {
            search(input: $input) {
                total
                searchResults {
                    entity {
                        urn
                    }
                }
            }
        }
    """
    variables: Dict[str, Any] = {
        "input": {"type": entity_type_map.get(entity_type), "query": "*"}
    }

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["search"]


@with_test_retry()
def list_ingestion_sources(auth_session) -> Dict[str, Any]:
    """List all ingestion sources."""
    query = """
        query listIngestionSources($input: ListIngestionSourcesInput!) {
            listIngestionSources(input: $input) {
                total
                ingestionSources {
                    urn
                    name
                    type
                    config {
                        version
                    }
                }
            }
        }
    """
    variables: Dict[str, Any] = {"input": {"query": "*"}}

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["listIngestionSources"]


@with_test_retry()
def list_ingestion_sources_with_filter(
    auth_session,
    filters: Optional[list] = None,
    include_executions: bool = False,
    start: int = 0,
    count: int = 100,
) -> Dict[str, Any]:
    """List ingestion sources with optional filtering and execution status.

    Args:
        auth_session: The authenticated session
        filters: Optional list of filter objects, e.g.:
                 [{"field": "sourceType", "values": ["SYSTEM"], "negated": False}]
        include_executions: Whether to include the latest execution status for each source
        start: Starting offset for pagination
        count: Number of sources to retrieve

    Returns:
        Dictionary containing total count and list of ingestion sources with their details
    """
    executions_field = ""
    if include_executions:
        executions_field = """
            executions(start: 0, count: 1) {
                executionRequests {
                    urn
                    result {
                        status
                        startTimeMs
                        durationMs
                    }
                }
            }
        """

    query = f"""
        query listIngestionSources($input: ListIngestionSourcesInput!) {{
            listIngestionSources(input: $input) {{
                total
                ingestionSources {{
                    urn
                    name
                    type
                    {executions_field}
                }}
            }}
        }}
    """

    variables: Dict[str, Any] = {"input": {"start": start, "count": count}}
    if filters:
        variables["input"]["filters"] = filters

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["listIngestionSources"]


@with_test_retry()
def list_scheduled_ingestion_sources(
    auth_session,
    start: int = 0,
    count: int = 100,
) -> Dict[str, Any]:
    """List ingestion sources with schedules and execution history.

    This helper fetches ingestion sources that have cron schedules along with:
    - Schedule configuration (cron interval and timezone)
    - All execution history (up to 1000 executions)
    - Source metadata (URN, name, type)

    Args:
        auth_session: The authenticated session
        start: Starting offset for pagination
        count: Number of sources to retrieve

    Returns:
        Dictionary containing:
        - total: Total count of scheduled ingestion sources
        - ingestionSources: List of sources with schedules and executions
    """
    query = """
        query listIngestionSources($input: ListIngestionSourcesInput!) {
            listIngestionSources(input: $input) {
                total
                ingestionSources {
                    urn
                    name
                    type
                    schedule {
                        interval
                        timezone
                    }
                    executions(start: 0, count: 1000) {
                        executionRequests {
                            urn
                            input {
                                requestedAt
                                source {
                                    type
                                }
                            }
                            result {
                                status
                                startTimeMs
                                durationMs
                            }
                        }
                    }
                }
            }
        }
    """

    variables: Dict[str, Any] = {"input": {"start": start, "count": count}}

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["listIngestionSources"]


@with_test_retry()
def list_policies(auth_session) -> Dict[str, Any]:
    """List all policies."""
    query = """
        query listPolicies($input: ListPoliciesInput!) {
            listPolicies(input: $input) {
                total
                policies {
                    urn
                    name
                    state
                }
            }
        }
    """
    variables: Dict[str, Any] = {"input": {"query": "*"}}

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["listPolicies"]


@with_test_retry()
def get_highlights(auth_session) -> Dict[str, Any]:
    """Get highlights."""
    query = """
        query getHighlights {
            getHighlights {
                value
                title
                body
            }
        }
    """

    res_data = execute_graphql(auth_session, query)
    return res_data["data"]["getHighlights"]


@with_test_retry()
def get_analytics_charts(auth_session) -> Dict[str, Any]:
    """Get analytics charts."""
    query = """
        query getAnalyticsCharts {
            getAnalyticsCharts {
                groupId
                title
            }
        }
    """

    res_data = execute_graphql(auth_session, query)
    return res_data["data"]["getAnalyticsCharts"]


@with_test_retry()
def get_metadata_analytics_charts(auth_session) -> Dict[str, Any]:
    """Get metadata analytics charts."""
    query = """
        query getMetadataAnalyticsCharts($input: MetadataAnalyticsInput!) {
            getMetadataAnalyticsCharts(input: $input) {
                groupId
                title
            }
        }
    """
    variables: Dict[str, Any] = {"input": {"query": "*"}}

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["getMetadataAnalyticsCharts"]


def get_global_settings(auth_session) -> Optional[Dict[str, Any]]:
    """Get global settings including integration and notification settings."""
    query = """
        query getGlobalSettings {
            globalSettings {
                integrationSettings {
                    slackSettings {
                        defaultChannelName
                        botToken
                        datahubAtMentionEnabled
                        __typename
                    }
                    emailSettings {
                        defaultEmail
                        __typename
                    }
                    teamsSettings {
                        defaultChannel {
                            id
                            name
                            __typename
                        }
                        __typename
                    }
                    __typename
                }
                notificationSettings {
                    settings {
                        type
                        value
                        params {
                            key
                            value
                            __typename
                        }
                        __typename
                    }
                    __typename
                }
                visualSettings {
                    helpLink {
                        isEnabled
                        label
                        link
                        __typename
                    }
                    customLogoUrl
                    customOrgName
                    __typename
                }
                documentationAi {
                    enabled
                    instructions {
                        id
                        type
                        state
                        instruction
                        created {
                            time
                            actor
                            __typename
                        }
                        lastModified {
                            time
                            actor
                            __typename
                        }
                        __typename
                    }
                    __typename
                }
                aiAssistant {
                    instructions {
                        id
                        type
                        state
                        instruction
                        created {
                            time
                            actor
                            __typename
                        }
                        lastModified {
                            time
                            actor
                            __typename
                        }
                        __typename
                    }
                    __typename
                }
                __typename
            }
        }
    """

    try:
        res_data = execute_graphql(auth_session, query)
        return res_data["data"]["globalSettings"]
    except Exception as e:
        logger.warning(f"Failed to fetch global settings: {e}")
        return None


def verify_auth_session(auth_session, context: str = "") -> bool:
    """Verify auth session by querying current user info.

    Args:
        auth_session: The authenticated session to verify
        context: Optional context string for logging (e.g., "after login", "before token generation")

    Returns:
        True if auth is valid, False otherwise
    """
    query = """
        query me {
            me {
                corpUser {
                    urn
                    status
                }
                platformPrivileges {
                    managePolicies
                    manageIdentities
                    generatePersonalAccessTokens
                }
            }
        }
    """

    context_str = f" ({context})" if context else ""
    try:
        logger.info(f"Verifying auth session{context_str}")
        res_data = execute_graphql(auth_session, query)
        me_data = res_data["data"]["me"]
        logger.info(
            f"Auth session verified{context_str}: "
            f"user={me_data['corpUser']['urn']}, "
            f"status={me_data['corpUser']['status']}, "
            f"privileges={me_data['platformPrivileges']}"
        )
        return True
    except Exception as e:
        logger.error(
            f"Auth session verification failed{context_str}: {type(e).__name__}: {e}"
        )
        return False


def get_default_channel_name(auth_session) -> str | None:
    """Extract defaultChannelName from global Slack settings."""
    global_settings = get_global_settings(auth_session)
    if global_settings is None:
        return None

    integration_settings = global_settings.get("integrationSettings")
    if integration_settings is None:
        return None

    slack_settings = integration_settings.get("slackSettings")
    if slack_settings is None:
        return None

    return slack_settings.get("defaultChannelName")


@with_test_retry()
def list_incidents(
    auth_session,
    resource_urn: str,
    state: str = "ACTIVE",
    start: int = 0,
    count: int = 10,
) -> Dict[str, Any]:
    """List incidents for a resource.

    Args:
        auth_session: The authenticated session
        resource_urn: URN of the resource (e.g., dataset URN)
        state: Incident state filter (ACTIVE, RESOLVED, or ALL)
        start: Starting offset for pagination
        count: Number of incidents to retrieve

    Returns:
        Dictionary containing incidents data with keys: start, count, total, incidents
    """
    state_filter = f"state: {state}, " if state != "ALL" else ""
    query = f"""query dataset($urn: String!) {{
        dataset(urn: $urn) {{
          incidents({state_filter}start: {start}, count: {count}) {{
            start
            count
            total
            incidents {{
              urn
              type
              incidentType
              title
              description
              priority
              incidentStatus {{
                state
                message
                lastUpdated {{
                  time
                  actor
                }}
              }}
              source {{
                type
                source {{
                  ... on Assertion {{
                    urn
                    info {{
                      type
                    }}
                  }}
                }}
              }}
              entity {{
                urn
              }}
              created {{
                time
                actor
              }}
            }}
          }}
        }}
    }}"""
    variables: Dict[str, Any] = {"urn": resource_urn}

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["dataset"]["incidents"]


def raise_incident(
    auth_session,
    resource_urn: str,
    incident_type: str,
    title: str,
    description: str,
    priority: str = "CRITICAL",
) -> str:
    """Raise a new incident on a resource.

    Args:
        auth_session: The authenticated session
        resource_urn: URN of the resource to raise incident on
        incident_type: Type of incident (e.g., OPERATIONAL, CUSTOM)
        title: Incident title
        description: Incident description
        priority: Incident priority (CRITICAL, HIGH, MEDIUM, LOW)

    Returns:
        URN of the newly created incident
    """
    variables: Dict[str, Any] = {
        "input": {
            "type": incident_type,
            "title": title,
            "description": description,
            "resourceUrn": resource_urn,
            "priority": priority,
        }
    }

    query = """mutation raiseIncident($input: RaiseIncidentInput!) {
        raiseIncident(input: $input)
    }"""

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["raiseIncident"]


def update_incident_status(
    auth_session,
    incident_urn: str,
    state: str,
    message: str = "",
) -> bool:
    """Update the status of an incident.

    Args:
        auth_session: The authenticated session
        incident_urn: URN of the incident to update
        state: New state (ACTIVE, RESOLVED)
        message: Optional status message

    Returns:
        True if the update was successful
    """
    variables: Dict[str, Any] = {
        "urn": incident_urn,
        "input": {
            "state": state,
            "message": message,
        },
    }

    query = """mutation updateIncidentStatus($urn: String!, $input: IncidentStatusInput!) {
        updateIncidentStatus(urn: $urn, input: $input)
    }"""

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["updateIncidentStatus"]


@with_test_retry()
def search_datasets_by_assertions(auth_session) -> Dict[str, Any]:
    """Search for datasets filtered by assertion status.

    This query mirrors the observe/datasets UI filtering logic.
    It searches for datasets with any of:
    - hasFailingAssertions
    - hasPassingAssertions
    - hasErroredAssertions
    - hasInitializingAssertions

    Returns:
        Dictionary containing search results with keys: start, count, total, searchResults
    """
    query = """
        query searchAcrossEntities($input: SearchAcrossEntitiesInput!) {
            searchAcrossEntities(input: $input) {
                start
                count
                total
                searchResults {
                    entity {
                        urn
                        type
                    }
                }
            }
        }
    """

    variables: Dict[str, Any] = {
        "input": {
            "types": ["DATASET"],
            "query": "*",
            "start": 0,
            "count": 25,
            "orFilters": [
                {"and": [{"field": "hasFailingAssertions", "values": ["true"]}]},
                {"and": [{"field": "hasPassingAssertions", "values": ["true"]}]},
                {"and": [{"field": "hasErroredAssertions", "values": ["true"]}]},
                {"and": [{"field": "hasInitializingAssertions", "values": ["true"]}]},
            ],
            "sortInput": {
                "sortCriterion": {
                    "field": "lastAssertionResultAt",
                    "sortOrder": "DESCENDING",
                }
            },
            "searchFlags": {"skipCache": True},
        }
    }

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["searchAcrossEntities"]


@with_test_retry()
def get_data_product(auth_session, urn: str) -> Optional[Dict[str, Any]]:
    """Get a data product by URN."""
    query = """
        query getDataProduct($urn: String!) {
            dataProduct(urn: $urn) {
                urn
                type
                properties {
                    name
                    description
                }
            }
        }
    """
    variables: Dict[str, Any] = {"urn": urn}

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["dataProduct"]


@with_test_retry()
def search_across_lineage(
    auth_session,
    urn: str,
    direction: str = "UPSTREAM",
    query: str = "*",
    count: int = 10,
) -> Dict[str, Any]:
    """Search across lineage from a given entity."""
    graphql_query = """
        query searchAcrossLineage($input: SearchAcrossLineageInput!) {
            searchAcrossLineage(input: $input) {
                start
                count
                total
                searchResults {
                    entity {
                        urn
                    }
                    paths {
                        path {
                            urn
                        }
                    }
                    degree
                }
            }
        }
    """
    variables: Dict[str, Any] = {
        "input": {
            "urn": urn,
            "direction": direction,
            "query": query,
            "count": count,
        }
    }

    res_data = execute_graphql(auth_session, graphql_query, variables)
    return res_data["data"]["searchAcrossLineage"]


@with_test_retry()
def scroll_across_lineage(
    auth_session,
    urn: str,
    direction: str = "UPSTREAM",
    scroll_id: Optional[str] = None,
    query: str = "*",
    keep_alive: str = "5m",
    count: int = 10,
) -> Dict[str, Any]:
    """Scroll across lineage from a given entity."""
    graphql_query = """
        query scrollAcrossLineage($input: ScrollAcrossLineageInput!) {
            scrollAcrossLineage(input: $input) {
                nextScrollId
                count
                total
                searchResults {
                    entity {
                        urn
                    }
                    paths {
                        path {
                            urn
                        }
                    }
                    degree
                }
            }
        }
    """
    variables: Dict[str, Any] = {
        "input": {
            "urn": urn,
            "direction": direction,
            "query": query,
            "count": count,
            "keepAlive": keep_alive,
        }
    }
    if scroll_id:
        variables["input"]["scrollId"] = scroll_id

    res_data = execute_graphql(auth_session, graphql_query, variables)
    return res_data["data"]["scrollAcrossLineage"]


def create_tag_proposal(
    auth_session,
    resource_urn: str,
    tag_urns: list[str],
    description: str,
) -> str:
    """Create a tag proposal on a resource.

    Args:
        auth_session: The authenticated session
        resource_urn: URN of the resource to propose tags for
        tag_urns: List of tag URNs to propose
        description: Proposal description/context

    Returns:
        URN of the newly created proposal (ActionRequest URN)
    """
    variables: Dict[str, Any] = {
        "input": {
            "resourceUrn": resource_urn,
            "tagUrns": tag_urns,
            "description": description,
        }
    }

    query = """mutation proposeTags($input: ProposeTagsInput!) {
        proposeTags(input: $input)
    }"""

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["proposeTags"]


@with_test_retry()
def list_proposals(
    auth_session,
    resource_urn: str,
    proposal_type: str = "TAG_ASSOCIATION",
    status: str = "PENDING",
    start: int = 0,
    count: int = 10,
) -> Dict[str, Any]:
    """List proposals (action requests) for a resource.

    Args:
        auth_session: The authenticated session
        resource_urn: URN of the resource (e.g., dataset URN)
        proposal_type: Type of proposal (TAG_ASSOCIATION, TERM_ASSOCIATION, etc.)
        status: Proposal status filter (PENDING, COMPLETED)
        start: Starting offset for pagination
        count: Number of proposals to retrieve

    Returns:
        Dictionary containing proposals data with keys: start, count, total, actionRequests
    """
    query = """query listActionRequests($input: ListActionRequestsInput!) {
        listActionRequests(input: $input) {
            start
            count
            total
            actionRequests {
                urn
                type
                description
                status
                result
                entity {
                    urn
                }
                params {
                    tagProposal {
                        tags {
                            urn
                            properties {
                                name
                            }
                        }
                    }
                }
            }
        }
    }"""

    variables: Dict[str, Any] = {
        "input": {
            "resourceUrn": resource_urn,
            "type": proposal_type,
            "status": status,
            "start": start,
            "count": count,
        }
    }

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["listActionRequests"]


def accept_proposal(
    auth_session,
    proposal_urn: str,
    note: str = "",
) -> bool:
    """Accept a proposal.

    Args:
        auth_session: The authenticated session
        proposal_urn: URN of the proposal to accept
        note: Optional note explaining the acceptance

    Returns:
        True if the acceptance was successful
    """
    variables: Dict[str, Any] = {
        "urns": [proposal_urn],
        "note": note,
    }

    query = """mutation acceptProposals($urns: [String!]!, $note: String) {
        acceptProposals(urns: $urns, note: $note)
    }"""

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["acceptProposals"]


def reject_proposal(
    auth_session,
    proposal_urn: str,
    note: str = "",
) -> bool:
    """Reject a proposal.

    Args:
        auth_session: The authenticated session
        proposal_urn: URN of the proposal to reject
        note: Optional note explaining the rejection

    Returns:
        True if the rejection was successful
    """
    variables: Dict[str, Any] = {
        "urns": [proposal_urn],
        "note": note,
    }

    query = """mutation rejectProposals($urns: [String!]!, $note: String) {
        rejectProposals(urns: $urns, note: $note)
    }"""

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["rejectProposals"]


def add_owner(
    auth_session,
    resource_urn: str,
    owner_urn: str,
    owner_entity_type: str = "CORP_USER",
    ownership_type: str = "TECHNICAL_OWNER",
    ownership_type_urn: Optional[str] = None,
) -> bool:
    """Add an owner to a resource.

    Args:
        auth_session: Authenticated session
        resource_urn: URN of the resource (e.g., dataset)
        owner_urn: URN of the owner (user or group)
        owner_entity_type: Either CORP_USER or CORP_GROUP
        ownership_type: Type of ownership (TECHNICAL_OWNER, BUSINESS_OWNER, etc.)
        ownership_type_urn: Optional custom ownership type URN

    Returns:
        True if successful
    """
    variables: Dict[str, Any] = {
        "input": {
            "ownerUrn": owner_urn,
            "ownerEntityType": owner_entity_type,
            "resourceUrn": resource_urn,
        }
    }

    if ownership_type_urn:
        variables["input"]["ownershipTypeUrn"] = ownership_type_urn
        variables["input"]["type"] = "CUSTOM"
    else:
        variables["input"]["type"] = ownership_type

    query = """mutation addOwner($input: AddOwnerInput!) {
        addOwner(input: $input)
    }"""

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["addOwner"]


def remove_owner(
    auth_session,
    resource_urn: str,
    owner_urn: str,
    ownership_type_urn: Optional[str] = None,
) -> bool:
    """Remove an owner from a resource.

    Args:
        auth_session: Authenticated session
        resource_urn: URN of the resource (e.g., dataset)
        owner_urn: URN of the owner to remove
        ownership_type_urn: Optional - specific ownership type to remove

    Returns:
        True if successful
    """
    variables: Dict[str, Any] = {
        "input": {
            "ownerUrn": owner_urn,
            "resourceUrn": resource_urn,
        }
    }

    if ownership_type_urn:
        variables["input"]["ownershipTypeUrn"] = ownership_type_urn

    query = """mutation removeOwner($input: RemoveOwnerInput!) {
        removeOwner(input: $input)
    }"""

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["removeOwner"]


@with_test_retry()
def get_dataset_owners(auth_session, dataset_urn: str) -> Dict[str, Any]:
    """Get ownership information for a dataset.

    Args:
        auth_session: Authenticated session
        dataset_urn: URN of the dataset

    Returns:
        Dictionary containing ownership data with 'owners' array
    """
    query = """
        query getDatasetOwners($urn: String!) {
            dataset(urn: $urn) {
                urn
                ownership {
                    owners {
                        owner {
                            ... on CorpUser {
                                urn
                                properties {
                                    email
                                }
                            }
                            ... on CorpGroup {
                                urn
                                properties {
                                    displayName
                                }
                            }
                        }
                        type
                        ownershipType {
                            urn
                            info {
                                name
                            }
                        }
                    }
                }
            }
        }
    """
    variables: Dict[str, Any] = {"urn": dataset_urn}

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["dataset"]["ownership"]


@with_test_retry()
def get_dataset_tags(auth_session, dataset_urn: str) -> Dict[str, Any]:
    """Get tag information for a dataset.

    Args:
        auth_session: Authenticated session
        dataset_urn: URN of the dataset

    Returns:
        Dictionary containing tags data with 'tags' array
    """
    query = """
        query getDatasetTags($urn: String!) {
            dataset(urn: $urn) {
                urn
                globalTags {
                    tags {
                        tag {
                            urn
                            name
                            description
                        }
                    }
                }
            }
        }
    """
    variables: Dict[str, Any] = {"urn": dataset_urn}

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["dataset"]["globalTags"]


@with_test_retry()
def get_dataset_terms(auth_session, dataset_urn: str) -> Dict[str, Any]:
    """Get glossary term information for a dataset.

    Args:
        auth_session: Authenticated session
        dataset_urn: URN of the dataset

    Returns:
        Dictionary containing terms data with 'terms' array
    """
    query = """
        query getDatasetTerms($urn: String!) {
            dataset(urn: $urn) {
                urn
                glossaryTerms {
                    terms {
                        term {
                            urn
                            name
                        }
                    }
                }
            }
        }
    """
    variables: Dict[str, Any] = {"urn": dataset_urn}

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["dataset"]["glossaryTerms"]


def update_deprecation(
    auth_session,
    resource_urn: str,
    deprecated: bool,
    note: str = "",
    decommission_time: Optional[int] = None,
) -> bool:
    """Update deprecation status of a resource."""
    variables: Dict[str, Any] = {
        "input": {"urn": resource_urn, "deprecated": deprecated, "note": note}
    }
    if decommission_time is not None:
        variables["input"]["decommissionTime"] = decommission_time

    query = """mutation updateDeprecation($input: UpdateDeprecationInput!) {
        updateDeprecation(input: $input)
    }"""

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["updateDeprecation"]


@with_test_retry()
def get_dataset_deprecation(auth_session, dataset_urn: str) -> Optional[Dict[str, Any]]:
    """Get deprecation information for a dataset.

    Args:
        auth_session: Authenticated session
        dataset_urn: URN of the dataset

    Returns:
        Dictionary containing deprecation data with 'deprecated', 'note', etc., or None if not deprecated
    """
    query = """
        query getDatasetDeprecation($urn: String!) {
            dataset(urn: $urn) {
                urn
                deprecation {
                    deprecated
                    note
                    decommissionTime
                    actor
                }
            }
        }
    """
    variables: Dict[str, Any] = {"urn": dataset_urn}

    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["dataset"]["deprecation"]


def create_ingestion_execution_request(auth_session, ingestion_source_urn: str) -> str:
    """Submit execution request for an ingestion source.

    Returns: execution request URN
    """
    query = """
        mutation createIngestionExecutionRequest($input: CreateIngestionExecutionRequestInput!) {
            createIngestionExecutionRequest(input: $input)
        }
    """
    variables: Dict[str, Any] = {"input": {"ingestionSourceUrn": ingestion_source_urn}}
    res_data = execute_graphql(auth_session, query, variables)
    return res_data["data"]["createIngestionExecutionRequest"]


def get_execution_request_status(
    auth_session, execution_request_urn: str
) -> Optional[Dict[str, Any]]:
    """Get execution request result.

    Returns: None if not completed yet, or dict with:
        - status: "SUCCESS", "FAILURE", "CANCELLED", "DUPLICATE", "ABORTED"
        - startTimeMs: timestamp
        - durationMs: duration in milliseconds
    """
    # Terminal statuses that indicate execution is complete
    TERMINAL_STATUSES = {"SUCCESS", "FAILURE", "CANCELLED", "DUPLICATE", "ABORTED"}

    query = """
        query executionRequest($urn: String!) {
            executionRequest(urn: $urn) {
                result {
                    status
                    startTimeMs
                    durationMs
                }
            }
        }
    """
    variables: Dict[str, Any] = {"urn": execution_request_urn}
    res_data = execute_graphql(auth_session, query, variables)
    result = res_data["data"]["executionRequest"]["result"]

    # Only return result if status is terminal, otherwise return None to continue polling
    if result and result.get("status") in TERMINAL_STATUSES:
        return result
    return None


def poll_execution_requests(
    auth_session,
    execution_tracking: Dict[str, Dict[str, str]],
    timeout_minutes: int,
    poll_interval_seconds: int = 5,
) -> Dict[str, Dict[str, Any]]:
    """Poll multiple execution requests until all complete or timeout.

    Args:
        auth_session: Authenticated session
        execution_tracking: Dict mapping source_name to {
            "source_urn": str,
            "execution_urn": str
        }
        timeout_minutes: Maximum time to wait
        poll_interval_seconds: Seconds between polls

    Returns: Dict mapping source_name to {
        "source_urn": str,
        "execution_urn": str,
        "status": str,  # "SUCCESS", "FAILURE", "TIMEOUT", etc.
        "startTimeMs": int,
        "durationMs": int
    }
    """
    timeout_seconds = timeout_minutes * 60
    start_time = time.time()
    results = {}

    pending = set(execution_tracking.keys())

    while pending and (time.time() - start_time) < timeout_seconds:
        elapsed_minutes = (time.time() - start_time) / 60
        logger.info(
            f"Polling {len(pending)} pending executions "
            f"({elapsed_minutes:.1f}/{timeout_minutes} min elapsed)"
        )

        for source_name in list(pending):
            tracking_info = execution_tracking[source_name]
            execution_urn = tracking_info["execution_urn"]

            result = get_execution_request_status(auth_session, execution_urn)

            if result is not None:
                status = result["status"]
                logger.info(f"Source '{source_name}' completed with status: {status}")

                results[source_name] = {
                    "source_urn": tracking_info["source_urn"],
                    "execution_urn": execution_urn,
                    "status": status,
                    "startTimeMs": result.get("startTimeMs"),
                    "durationMs": result.get("durationMs"),
                }
                pending.remove(source_name)

        if pending:
            time.sleep(poll_interval_seconds)

    for source_name in pending:
        tracking_info = execution_tracking[source_name]
        logger.warning(
            f"Source '{source_name}' timed out after {timeout_minutes} minutes"
        )
        results[source_name] = {
            "source_urn": tracking_info["source_urn"],
            "execution_urn": tracking_info["execution_urn"],
            "status": "TIMEOUT",
            "startTimeMs": None,
            "durationMs": None,
        }

    return results
