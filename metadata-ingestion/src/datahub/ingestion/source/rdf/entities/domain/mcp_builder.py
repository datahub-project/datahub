"""
Domain MCP Builder

Creates DataHub MCPs for domains.
"""

import logging
from typing import Any, Dict, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.rdf.entities.base import EntityMCPBuilder
from datahub.ingestion.source.rdf.entities.domain.ast import DataHubDomain
from datahub.metadata.schema_classes import (
    CorpGroupInfoClass,
    DomainPropertiesClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)

logger = logging.getLogger(__name__)


class DomainMCPBuilder(EntityMCPBuilder[DataHubDomain]):
    """
    Creates MCPs for domains.

    Creates DomainProperties MCP for each domain.
    Only creates MCPs for domains with datasets in their hierarchy.
    """

    @property
    def entity_type(self) -> str:
        return "domain"

    def build_mcps(
        self, domain: DataHubDomain, context: Dict[str, Any] = None
    ) -> List[MetadataChangeProposalWrapper]:
        """Build MCPs for a single domain."""
        mcps = []

        # Skip domains without datasets
        if not self._domain_has_datasets(domain):
            return mcps

        try:
            mcp = self._create_domain_properties_mcp(domain)
            if mcp:
                mcps.append(mcp)
        except Exception as e:
            logger.error(f"Failed to create MCP for domain {domain.name}: {e}")

        return mcps

    def build_all_mcps(
        self, domains: List[DataHubDomain], context: Dict[str, Any] = None
    ) -> List[MetadataChangeProposalWrapper]:
        """Build MCPs for all domains."""
        mcps = []

        for domain in domains:
            domain_mcps = self.build_mcps(domain, context)
            mcps.extend(domain_mcps)

        logger.info(f"Built {len(mcps)} domain MCPs")
        return mcps

    def _create_domain_properties_mcp(
        self, domain: DataHubDomain
    ) -> Optional[MetadataChangeProposalWrapper]:
        """Create DomainProperties MCP."""
        # Use domain description if available, otherwise generate from path
        description = (
            domain.description
            if domain.description
            else f"Domain for {tuple(domain.path_segments)}"
        )

        properties = DomainPropertiesClass(
            name=domain.name,
            description=description,
            parentDomain=str(domain.parent_domain_urn)
            if domain.parent_domain_urn
            else None,
        )

        return MetadataChangeProposalWrapper(
            entityUrn=str(domain.urn), aspect=properties
        )

    def _domain_has_datasets(self, domain: DataHubDomain) -> bool:
        """Check if domain or any subdomain has datasets."""
        if domain.datasets:
            return True

        for subdomain in domain.subdomains:
            if self._domain_has_datasets(subdomain):
                return True

        return False

    @staticmethod
    def create_corpgroup_mcp(
        group_urn: str,
        group_name: str,
        group_description: str = None,
        group_email: str = None,
    ) -> MetadataChangeProposalWrapper:
        """Create MCP for a corpGroup (owner group) per specification Section 8.2 and 8.8."""
        group_info = CorpGroupInfoClass(
            displayName=group_name,
            description=group_description or f"Owner group: {group_name}",
            email=group_email,
        )

        return MetadataChangeProposalWrapper(entityUrn=group_urn, aspect=group_info)

    @staticmethod
    def create_domain_ownership_mcp(
        domain_urn: str, owner_urns: List[str], owner_types: List[str] = None
    ) -> MetadataChangeProposalWrapper:
        """Create MCP for domain ownership assignment per specification Section 8.3 and 8.8."""
        if not owner_urns:
            raise ValueError(
                "Cannot create domain ownership MCP with empty owner_urns list"
            )

        if not owner_types:
            raise ValueError(
                f"Owner types must be provided for {len(owner_urns)} owners. "
                f"Each owner must have dh:hasOwnerType property in RDF (supports custom owner types)."
            )

        if len(owner_types) != len(owner_urns):
            raise ValueError(
                f"Owner types count ({len(owner_types)}) must match owner_urns count ({len(owner_urns)}). "
                f"Each owner must have a corresponding owner type."
            )

        # Map standard owner type strings to enum for compatibility, but support any custom string
        type_mapping = {
            "BUSINESS_OWNER": OwnershipTypeClass.BUSINESS_OWNER,
            "DATA_STEWARD": OwnershipTypeClass.DATA_STEWARD,
            "TECHNICAL_OWNER": OwnershipTypeClass.TECHNICAL_OWNER,
        }

        # Create owner objects
        owners = []
        for owner_urn, owner_type_str in zip(owner_urns, owner_types):
            # Try to use enum for standard types, but fall back to string for custom types
            if isinstance(owner_type_str, str):
                # Use enum if it's a standard type, otherwise use the string directly (supports custom types)
                owner_type = type_mapping.get(owner_type_str.upper(), owner_type_str)
            else:
                # Already an enum or other type
                owner_type = owner_type_str

            owners.append(OwnerClass(owner=owner_urn, type=owner_type))

        ownership_aspect = OwnershipClass(owners=owners)

        return MetadataChangeProposalWrapper(
            entityUrn=domain_urn, aspect=ownership_aspect
        )
