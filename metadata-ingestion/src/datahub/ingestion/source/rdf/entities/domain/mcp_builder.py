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
    Creates MCPs for domains with glossary terms in their hierarchy.
    """

    @property
    def entity_type(self) -> str:
        return "domain"

    def build_mcps(
        self, domain: DataHubDomain, context: Dict[str, Any] = None
    ) -> List[MetadataChangeProposalWrapper]:
        """Build MCPs for a single domain."""
        mcps = []

        # Skip domains without glossary terms
        if not self._domain_has_glossary_terms(domain):
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

    def _domain_has_glossary_terms(self, domain: DataHubDomain) -> bool:
        """Check if domain or any subdomain has glossary terms."""
        if domain.glossary_terms:
            return True

        for subdomain in domain.subdomains:
            if self._domain_has_glossary_terms(subdomain):
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

    def build_post_processing_mcps(
        self, datahub_graph: Any, context: Dict[str, Any] = None
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Build post-processing MCPs for domains.

        Handles:
        - Owner group creation (corpGroups)
        - Domain ownership assignment

        Args:
            datahub_graph: The complete DataHub AST
            context: Optional context with shared state (e.g., report)

        Returns:
            List of MetadataChangeProposalWrapper objects
        """
        mcps = []
        report = context.get("report") if context else None

        # Build owner IRI to URN mapping (needed for domain ownership)
        owner_iri_to_urn = {}
        owner_iri_to_type = {}

        # Process owner groups first (must exist before domain ownership)
        if hasattr(datahub_graph, "owner_groups") and datahub_graph.owner_groups:
            logger.info(
                f"Processing {len(datahub_graph.owner_groups)} owner groups (before domain ownership)"
            )
            for owner_group in datahub_graph.owner_groups:
                try:
                    group_mcp = self.create_corpgroup_mcp(
                        group_urn=owner_group.urn,
                        group_name=owner_group.name,
                        group_description=owner_group.description,
                    )
                    mcps.append(group_mcp)
                    owner_iri_to_urn[owner_group.iri] = owner_group.urn
                    owner_iri_to_type[owner_group.iri] = owner_group.owner_type
                    if report:
                        report.report_entity_emitted()
                    logger.debug(
                        f"Created corpGroup MCP for owner group: {owner_group.name} ({owner_group.urn})"
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to create corpGroup MCP for owner group {owner_group.iri}: {e}"
                    )

        # Process domain ownership MCPs
        for domain in datahub_graph.domains:
            if hasattr(domain, "owners") and domain.owners:
                owner_urns = []
                owner_types = []
                for owner_iri in domain.owners:
                    if owner_iri in owner_iri_to_urn:
                        owner_urn = owner_iri_to_urn[owner_iri]
                        owner_urns.append(owner_urn)
                        owner_type = owner_iri_to_type.get(owner_iri)
                        if not owner_type:
                            logger.warning(
                                f"Cannot determine owner type for {owner_iri}. "
                                f"Owner must have dh:hasOwnerType property in RDF. Skipping ownership for domain {domain.urn}."
                            )
                            continue
                        owner_types.append(owner_type)

                if owner_urns:
                    try:
                        ownership_mcp = self.create_domain_ownership_mcp(
                            domain_urn=str(domain.urn),
                            owner_urns=owner_urns,
                            owner_types=owner_types,
                        )
                        mcps.append(ownership_mcp)
                        if report:
                            report.report_entity_emitted()
                        logger.debug(
                            f"Created ownership MCP for domain {domain.name} with {len(owner_urns)} owners"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to create ownership MCP for domain {domain.urn}: {e}"
                        )

        return mcps
