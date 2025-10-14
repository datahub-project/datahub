# ABOUTME: Helper functions for common metadata operations like adding/removing tags and terms.
# ABOUTME: Reduces boilerplate GraphQL code in smoke tests.

from typing import Any, Dict, Optional

from tests.utils import execute_graphql


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
