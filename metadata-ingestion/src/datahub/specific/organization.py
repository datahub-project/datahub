"""
Organization helper for building organization metadata using the Python SDK.

This module provides convenient functions for creating organization metadata
and adding entity-to-organization relationships.
"""

from typing import List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    OrganizationPropertiesClass,
    OrganizationsClass,
    StatusClass,
    UserOrganizationsClass,
)
from datahub.utilities.urns.organization_urn import OrganizationUrn


def make_organization_urn(organization_id: str) -> str:
    """
    Create an organization URN from an organization ID.
    
    Args:
        organization_id: The unique identifier for the organization
        
    Returns:
        A properly formatted organization URN
        
    Examples:
        >>> make_organization_urn("engineering")
        'urn:li:organization:engineering'
    """
    if organization_id.startswith("urn:li:organization:"):
        return organization_id
    return OrganizationUrn(organization_id).urn()


def create_organization(
    organization_id: str,
    name: str,
    description: Optional[str] = None,
    logo_url: Optional[str] = None,
    external_url: Optional[str] = None,
) -> MetadataChangeProposalWrapper:
    """
    Create a new organization with properties.
    
    Args:
        organization_id: Unique identifier for the organization
        name: Display name of the organization
        description: Optional description of the organization
        logo_url: Optional URL to the organization's logo
        external_url: Optional URL to the organization's external page
        
    Returns:
        MetadataChangeProposalWrapper ready to be emitted
        
    Examples:
        >>> mcp = create_organization(
        ...     organization_id="engineering",
        ...     name="Engineering Team",
        ...     description="Core engineering organization",
        ...     external_url="https://example.com/engineering"
        ... )
    """
    organization_urn = make_organization_urn(organization_id)
    
    properties = OrganizationPropertiesClass(
        name=name,
        description=description if description else "",
        logoUrl=logo_url,
        externalUrl=external_url,
    )
    
    return MetadataChangeProposalWrapper(
        entityUrn=organization_urn,
        aspect=properties,
        changeType=ChangeTypeClass.UPSERT,
    )


def add_entity_to_organizations(
    entity_urn: str,
    organization_ids: List[str],
) -> MetadataChangeProposalWrapper:
    """
    Add an entity to one or more organizations.
    
    Args:
        entity_urn: URN of the entity to add to organizations
        organization_ids: List of organization IDs to add the entity to
        
    Returns:
        MetadataChangeProposalWrapper ready to be emitted
        
    Examples:
        >>> mcp = add_entity_to_organizations(
        ...     entity_urn="urn:li:dataset:(urn:li:dataPlatform:kafka,my_dataset,PROD)",
        ...     organization_ids=["engineering", "data-platform"]
        ... )
    """
    organization_urns = [make_organization_urn(org_id) for org_id in organization_ids]
    
    organizations = OrganizationsClass(
        organizations=organization_urns
    )
    
    return MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        aspect=organizations,
        changeType=ChangeTypeClass.UPSERT,
    )


def add_user_to_organizations(
    user_urn: str,
    organization_ids: List[str],
) -> MetadataChangeProposalWrapper:
    """
    Add a user to one or more organizations.
    
    Args:
        user_urn: URN of the user (corpUser) to add to organizations
        organization_ids: List of organization IDs to add the user to
        
    Returns:
        MetadataChangeProposalWrapper ready to be emitted
        
    Examples:
        >>> mcp = add_user_to_organizations(
        ...     user_urn="urn:li:corpuser:john.doe",
        ...     organization_ids=["engineering"]
        ... )
    """
    if not user_urn.startswith("urn:li:corpuser:"):
        raise ValueError(f"Expected corpuser URN, got: {user_urn}")
    
    organization_urns = [make_organization_urn(org_id) for org_id in organization_ids]
    
    user_organizations = UserOrganizationsClass(
        organizations=organization_urns
    )
    
    return MetadataChangeProposalWrapper(
        entityUrn=user_urn,
        aspect=user_organizations,
        changeType=ChangeTypeClass.UPSERT,
    )


def delete_organization(organization_id: str) -> MetadataChangeProposalWrapper:
    """
    Soft-delete an organization by setting its status to removed.
    
    Args:
        organization_id: The organization ID to delete
        
    Returns:
        MetadataChangeProposalWrapper ready to be emitted
        
    Examples:
        >>> mcp = delete_organization("engineering")
    """
    organization_urn = make_organization_urn(organization_id)
    
    status = StatusClass(removed=True)
    
    return MetadataChangeProposalWrapper(
        entityUrn=organization_urn,
        aspect=status,
        changeType=ChangeTypeClass.UPSERT,
    )
