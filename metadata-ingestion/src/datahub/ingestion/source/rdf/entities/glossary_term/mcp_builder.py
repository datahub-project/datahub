"""
Glossary Term MCP Builder

Creates DataHub MCPs (Metadata Change Proposals) for glossary terms.
"""

import logging
from typing import Any, Dict, List

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.rdf.entities.base import EntityMCPBuilder
from datahub.ingestion.source.rdf.entities.glossary_term.ast import DataHubGlossaryTerm

# Lazy import to avoid circular dependency with relationship module
from datahub.metadata.schema_classes import (
    GlossaryNodeInfoClass,
    GlossaryRelatedTermsClass,
    GlossaryTermInfoClass,
)

logger = logging.getLogger(__name__)


class GlossaryTermMCPBuilder(EntityMCPBuilder[DataHubGlossaryTerm]):
    """
    Creates MCPs for glossary terms.

    Creates:
    - GlossaryTermInfo MCP for term metadata
    - GlossaryRelatedTerms MCP for relationships (isRelatedTerms only)

    Note: Only creates isRelatedTerms (inherits) for broader relationships.
    Does NOT create hasRelatedTerms (contains).
    """

    @property
    def entity_type(self) -> str:
        return "glossary_term"

    def build_mcps(
        self, term: DataHubGlossaryTerm, context: Dict[str, Any] | None = None
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Build MCPs for a single glossary term.

        Args:
            term: The DataHub glossary term
            context: Optional context with 'parent_node_urn' for hierarchy
        """
        mcps = []
        parent_node_urn: str | None = None
        if context:
            parent_node_urn = context.get("parent_node_urn")  # type: ignore[assignment]

        try:
            # Create term info MCP
            term_info_mcp = self._create_term_info_mcp(term, parent_node_urn)
            mcps.append(term_info_mcp)

        except Exception as e:
            logger.error(f"Failed to create MCP for glossary term {term.name}: {e}")

        return mcps

    def build_all_mcps(
        self, terms: List[DataHubGlossaryTerm], context: Dict[str, Any] | None = None
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Build MCPs for glossary terms.

        Terms that are in dependent entities (entities this entity depends on)
        are skipped here and will be created in post-processing after their
        parent entities are created. Only terms NOT in dependent entities are
        created here (without parent nodes).
        """
        mcps = []
        datahub_graph = context.get("datahub_graph") if context else None

        # Collect terms that are in dependent entities (these will be handled in post-processing)
        # Use dependency metadata to determine which entity types to check
        terms_in_dependent_entities = set()
        dependent_entity_types = []

        # Get metadata for glossary_term to find its dependencies
        from datahub.ingestion.source.rdf.entities.glossary_term import ENTITY_METADATA

        if ENTITY_METADATA.dependencies:
            dependent_entity_types = ENTITY_METADATA.dependencies

        # Check each dependent entity type for terms
        if datahub_graph and dependent_entity_types:
            # Import the helper function to convert entity types to field names
            from datahub.ingestion.source.rdf.core.utils import (
                entity_type_to_field_name,
            )

            for dep_entity_type in dependent_entity_types:
                # Get the field name for this entity type (pluralized)
                field_name = entity_type_to_field_name(dep_entity_type)

                if hasattr(datahub_graph, field_name):
                    dependent_entities = getattr(datahub_graph, field_name, [])
                    for entity in dependent_entities:
                        # Check if this entity type has a glossary_terms attribute
                        if hasattr(entity, "glossary_terms"):
                            for term in entity.glossary_terms:
                                terms_in_dependent_entities.add(term.urn)

        # Only create MCPs for terms NOT in dependent entities
        # Terms in dependent entities will be created in post-processing with correct parent nodes
        for term in terms:
            if term.urn not in terms_in_dependent_entities:
                term_mcps = self.build_mcps(term, context)
                mcps.extend(term_mcps)

        skipped_count = len(terms) - len(mcps)
        if skipped_count > 0:
            logger.debug(
                f"Skipped {skipped_count} terms that are in dependent entities {dependent_entity_types} "
                f"(will be created in post-processing)"
            )
        logger.info(
            f"Built {len(mcps)} MCPs for {len(terms) - skipped_count} glossary terms "
            f"(skipped {skipped_count} in dependent entities)"
        )
        return mcps

    def build_relationship_mcps(
        self, relationships, context: Dict[str, Any] | None = None
    ) -> List[MetadataChangeProposalWrapper]:
        # Lazy import to avoid circular dependency
        from datahub.ingestion.source.rdf.entities.relationship.ast import (
            RelationshipType,
        )

        """
        Build MCPs for glossary term relationships.

        Only creates isRelatedTerms (inherits) for broader relationships.
        Does NOT create hasRelatedTerms (contains).

        Args:
            relationships: List of DataHub relationships
            context: Optional context

        Returns:
            List of MCPs for relationship aspects
        """
        mcps = []

        # Aggregate relationships by source term
        # Only track broader relationships for isRelatedTerms
        broader_terms_map: Dict[str, List[str]] = {}  # child_urn -> [broader_term_urns]

        for relationship in relationships:
            if relationship.relationship_type == RelationshipType.BROADER:
                source_urn = str(relationship.source_urn)
                target_urn = str(relationship.target_urn)

                if source_urn not in broader_terms_map:
                    broader_terms_map[source_urn] = []
                broader_terms_map[source_urn].append(target_urn)

        # Create isRelatedTerms MCPs (child points to broader parent = inherits)
        created_count = 0
        failed_count = 0

        for child_urn, broader_urns in broader_terms_map.items():
            try:
                unique_broader = list(set(broader_urns))  # Deduplicate
                broader_mcp = MetadataChangeProposalWrapper(
                    entityUrn=child_urn,
                    aspect=GlossaryRelatedTermsClass(isRelatedTerms=unique_broader),
                )
                mcps.append(broader_mcp)
                created_count += 1
                logger.debug(
                    f"Created isRelatedTerms MCP for {child_urn} with {len(unique_broader)} broader terms"
                )
            except Exception as e:
                failed_count += 1
                logger.error(
                    f"Failed to create isRelatedTerms MCP for {child_urn}: {e}"
                )

        logger.info(f"Built {created_count} relationship MCPs ({failed_count} failed)")
        return mcps

    def _create_term_info_mcp(
        self, term: DataHubGlossaryTerm, parent_node_urn: str | None = None
    ) -> MetadataChangeProposalWrapper:
        """Create the GlossaryTermInfo MCP."""
        term_info = GlossaryTermInfoClass(
            name=term.name,
            definition=term.definition or f"Glossary term: {term.name}",
            termSource="EXTERNAL",
            parentNode=parent_node_urn,
            sourceRef=term.source,
            sourceUrl=term.source,
            customProperties=term.custom_properties or {},
        )

        return MetadataChangeProposalWrapper(entityUrn=term.urn, aspect=term_info)

    @staticmethod
    def create_glossary_node_mcp(
        node_urn: str, node_name: str, parent_urn: str | None = None
    ) -> MetadataChangeProposalWrapper:
        """Create MCP for a glossary node."""
        node_info = GlossaryNodeInfoClass(
            name=node_name,
            definition=f"Glossary node: {node_name}",
            parentNode=parent_urn,
        )

        return MetadataChangeProposalWrapper(
            entityUrn=node_urn,
            aspect=node_info,
        )

    def build_post_processing_mcps(
        self, datahub_graph: Any, context: Dict[str, Any] | None = None
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Build MCPs for glossary nodes and terms from domain hierarchy.

        This is the ONLY place where glossary MCPs are created. It:
        1. Consults the domain hierarchy (built from glossary term path_segments)
        2. Creates glossary nodes (term groups) from the domain hierarchy
        3. Creates glossary terms under their parent glossary nodes

        Domains are used ONLY as a data structure - they are NOT ingested as
        DataHub domain entities. The glossary module is responsible for creating
        all glossary-related MCPs (nodes and terms).

        Args:
            datahub_graph: The complete DataHubGraph AST (contains domains as data structure)
            context: Optional context (should include 'report' for entity counting)

        Returns:
            List of MCPs for glossary nodes and terms (no domain MCPs)
        """
        from datahub.ingestion.source.rdf.entities.glossary_term.urn_generator import (
            GlossaryTermUrnGenerator,
        )

        mcps = []
        report = context.get("report") if context else None

        # Track created glossary nodes to avoid duplicates
        created_nodes = {}  # node_urn -> node_name
        urn_generator = GlossaryTermUrnGenerator()

        def create_glossary_nodes_from_domain(domain, parent_node_urn=None):
            """Recursively create glossary nodes from domain hierarchy."""
            # Create glossary node for this domain
            if domain.path_segments:
                node_name = domain.name
                node_urn = urn_generator.generate_glossary_node_urn_from_name(
                    node_name, parent_node_urn
                )

                if node_urn not in created_nodes:
                    node_mcp = self.create_glossary_node_mcp(
                        node_urn, node_name, parent_node_urn
                    )
                    mcps.append(node_mcp)
                    created_nodes[node_urn] = node_name
                    if report and hasattr(report, "report_entity_emitted"):
                        report.report_entity_emitted()

                # Create terms in this domain
                for term in domain.glossary_terms:
                    try:
                        term_mcps = self.build_mcps(term, {"parent_node_urn": node_urn})
                        mcps.extend(term_mcps)
                        for _ in term_mcps:
                            if report and hasattr(report, "report_entity_emitted"):
                                report.report_entity_emitted()
                    except Exception as e:
                        logger.warning(
                            f"Failed to create MCP for glossary term {term.urn}: {e}"
                        )

                # Recursively process subdomains
                for subdomain in domain.subdomains:
                    create_glossary_nodes_from_domain(subdomain, node_urn)

        # Process all root domains (domains without parents)
        root_domains = [d for d in datahub_graph.domains if d.parent_domain_urn is None]
        for domain in root_domains:
            create_glossary_nodes_from_domain(domain)

        # Also process terms that aren't in any domain (fallback)
        terms_in_domains = set()
        for domain in datahub_graph.domains:
            for term in domain.glossary_terms:
                terms_in_domains.add(term.urn)

        for term in datahub_graph.glossary_terms:
            if term.urn not in terms_in_domains:
                # Term not in any domain - create without parent node
                try:
                    term_mcps = self.build_mcps(term, {"parent_node_urn": None})
                    mcps.extend(term_mcps)
                    for _ in term_mcps:
                        if report:
                            report.report_entity_emitted()
                except Exception as e:
                    logger.warning(
                        f"Failed to create MCP for glossary term {term.urn}: {e}"
                    )

        logger.debug(
            f"Created {len(mcps)} MCPs for glossary nodes and terms from domains"
        )
        return mcps
