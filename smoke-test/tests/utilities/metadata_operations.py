# ABOUTME: Helper functions for common metadata operations like adding/removing tags and terms.
# ABOUTME: Reduces boilerplate GraphQL code in smoke tests.

import logging
from typing import Any, Dict, Optional

from tests.utils import execute_graphql, with_test_retry

logger = logging.getLogger(__name__)


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
