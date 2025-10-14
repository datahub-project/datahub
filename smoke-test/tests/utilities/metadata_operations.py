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
    """Add a tag to a resource.

    Args:
        auth_session: Authenticated session for making requests
        resource_urn: URN of the resource to tag (dataset, chart, etc.)
        tag_urn: URN of the tag to add
        sub_resource: Optional sub-resource identifier (e.g., schema field path)
        sub_resource_type: Optional sub-resource type (e.g., "DATASET_FIELD")

    Returns:
        True if the tag was added successfully

    Example:
        >>> add_tag(auth_session, "urn:li:dataset:(...)", "urn:li:tag:Legacy")
        >>> add_tag(auth_session, dataset_urn, tag_urn,
        ...         sub_resource="[version=2.0].field_name",
        ...         sub_resource_type="DATASET_FIELD")
    """
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
    """Remove a tag from a resource.

    Args:
        auth_session: Authenticated session for making requests
        resource_urn: URN of the resource (dataset, chart, etc.)
        tag_urn: URN of the tag to remove
        sub_resource: Optional sub-resource identifier (e.g., schema field path)
        sub_resource_type: Optional sub-resource type (e.g., "DATASET_FIELD")

    Returns:
        True if the tag was removed successfully

    Example:
        >>> remove_tag(auth_session, "urn:li:dataset:(...)", "urn:li:tag:Legacy")
    """
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
    """Add a glossary term to a resource.

    Args:
        auth_session: Authenticated session for making requests
        resource_urn: URN of the resource (dataset, chart, etc.)
        term_urn: URN of the glossary term to add
        sub_resource: Optional sub-resource identifier (e.g., schema field path)
        sub_resource_type: Optional sub-resource type (e.g., "DATASET_FIELD")

    Returns:
        True if the term was added successfully

    Example:
        >>> add_term(auth_session, dataset_urn, "urn:li:glossaryTerm:SavingAccount")
    """
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
    """Remove a glossary term from a resource.

    Args:
        auth_session: Authenticated session for making requests
        resource_urn: URN of the resource (dataset, chart, etc.)
        term_urn: URN of the glossary term to remove
        sub_resource: Optional sub-resource identifier (e.g., schema field path)
        sub_resource_type: Optional sub-resource type (e.g., "DATASET_FIELD")

    Returns:
        True if the term was removed successfully

    Example:
        >>> remove_term(auth_session, dataset_urn, "urn:li:glossaryTerm:SavingAccount")
    """
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
    """Update resource description.

    Args:
        auth_session: Authenticated session for making requests
        resource_urn: URN of the resource (dataset, chart, etc.)
        description: New description text
        sub_resource: Optional sub-resource identifier (e.g., schema field path)
        sub_resource_type: Optional sub-resource type (e.g., "DATASET_FIELD")

    Returns:
        True if the description was updated successfully

    Example:
        >>> update_description(auth_session, dataset_urn, "Updated description")
    """
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
