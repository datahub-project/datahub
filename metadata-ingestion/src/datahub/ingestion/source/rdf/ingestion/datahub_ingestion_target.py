#!/usr/bin/env python3
"""
DataHub Ingestion Target for RDF.

This module provides a target implementation that converts DataHub AST objects
directly to MCPs (Metadata Change Proposals) and work units for the DataHub
ingestion framework, without relying on DataHubClient.
"""

import logging
from typing import Any, Dict, List

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.rdf.core.target_factory import TargetInterface
from datahub.ingestion.source.rdf.core.utils import entity_type_to_field_name
from datahub.ingestion.source.rdf.entities.registry import (
    create_default_registry,
)

logger = logging.getLogger(__name__)


class DataHubIngestionTarget(TargetInterface):
    """
    Target implementation that yields work units for DataHub ingestion framework.

    This target directly creates MCPs from AST objects and converts them to work units
    without relying on DataHubClient.
    """

    def __init__(self, report):
        """Initialize the target with a report."""
        self.report = report
        self.workunits: List[MetadataWorkUnit] = []

    def send(self, datahub_graph: Any) -> Dict[str, Any]:  # noqa: C901
        """
        Convert DataHub AST to work units.

        Args:
            datahub_graph: DataHubGraph AST containing entities to emit

        Returns:
            Results dictionary with success status
        """
        from datahub.ingestion.source.rdf.core.ast import DataHubGraph

        if not isinstance(datahub_graph, DataHubGraph):
            return {
                "success": False,
                "error": f"Expected DataHubGraph, got {type(datahub_graph)}",
            }

        try:
            # Get registry for entity MCP builders
            registry = create_default_registry()

            # Log what entities are in the graph
            logger.info("Processing DataHub AST with:")
            logger.info(f"  - {len(datahub_graph.glossary_terms)} glossary terms")
            logger.info(f"  - {len(datahub_graph.datasets)} datasets")
            logger.info(
                f"  - {len(datahub_graph.structured_properties)} structured properties"
            )
            logger.info(
                f"  - {len(getattr(datahub_graph, 'structured_property_values', []))} structured property value assignments"
            )
            logger.info(f"  - {len(datahub_graph.data_products)} data products")
            logger.info(f"  - {len(datahub_graph.domains)} domains")
            logger.info(
                f"  - {len(getattr(datahub_graph, 'lineage_relationships', []))} lineage relationships"
            )
            logger.info(f"  - {len(datahub_graph.relationships)} relationships")
            logger.info(f"  - {len(datahub_graph.assertions)} assertions")

            # Generate MCPs for each entity type
            mcps = []

            # Process standard entities in order (using registry pattern)
            # Cross-entity dependencies (structured property values, glossary nodes from domains,
            # dataset-domain associations) are handled via post-processing hooks.
            # Non-registered entities (lineage activities, owner groups, domains) are handled separately.
            entity_types_by_order = registry.get_entity_types_by_processing_order()

            for entity_type in entity_types_by_order:
                mcp_builder = registry.get_mcp_builder(entity_type)
                if not mcp_builder:
                    logger.debug(
                        f"No MCP builder registered for {entity_type}, skipping"
                    )
                    continue

                # Get entity collection from graph (field name is pluralized)
                field_name = entity_type_to_field_name(entity_type)
                entities = getattr(datahub_graph, field_name, [])

                if not entities:
                    logger.debug(f"No {entity_type} entities to process")
                    continue

                metadata = registry.get_metadata(entity_type)
                processing_order = metadata.processing_order if metadata else 100
                logger.info(
                    f"Processing {len(entities)} {entity_type} entities (order: {processing_order})"
                )

                # Build context with full graph and report for post-processing hooks
                build_context = {"datahub_graph": datahub_graph, "report": self.report}

                # Use build_all_mcps if available, otherwise iterate
                if hasattr(mcp_builder, "build_all_mcps"):
                    try:
                        entity_mcps = mcp_builder.build_all_mcps(
                            entities, build_context
                        )
                        if entity_mcps:
                            mcps.extend(entity_mcps)
                            for _ in entity_mcps:
                                self.report.report_entity_emitted()
                            logger.debug(
                                f"Created {len(entity_mcps)} MCPs for {len(entities)} {entity_type} entities"
                            )
                        else:
                            logger.debug(
                                f"No MCPs created for {len(entities)} {entity_type} entities (they may have been filtered out)"
                            )
                    except Exception as e:
                        logger.error(
                            f"Failed to create MCPs for {entity_type}: {e}",
                            exc_info=True,
                        )
                else:
                    # Fallback: iterate and call build_mcps for each entity
                    created_count = 0
                    for entity in entities:
                        try:
                            entity_mcps = mcp_builder.build_mcps(entity, build_context)
                            if entity_mcps:
                                mcps.extend(entity_mcps)
                                for _ in entity_mcps:
                                    self.report.report_entity_emitted()
                                created_count += 1
                            else:
                                logger.debug(
                                    f"No MCPs created for {entity_type} {getattr(entity, 'urn', 'unknown')} (may have been filtered out)"
                                )
                        except Exception as e:
                            logger.error(
                                f"Failed to create MCP for {entity_type} {getattr(entity, 'urn', 'unknown')}: {e}",
                                exc_info=True,
                            )
                    logger.debug(
                        f"Created MCPs for {created_count}/{len(entities)} {entity_type} entities"
                    )

                # Call post-processing hook if available (for cross-entity dependencies)
                if hasattr(mcp_builder, "build_post_processing_mcps"):
                    try:
                        post_mcps = mcp_builder.build_post_processing_mcps(
                            datahub_graph, build_context
                        )
                        if post_mcps:
                            mcps.extend(post_mcps)
                            logger.debug(
                                f"Created {len(post_mcps)} post-processing MCPs for {entity_type}"
                            )
                    except Exception as e:
                        logger.error(
                            f"Failed to create post-processing MCPs for {entity_type}: {e}",
                            exc_info=True,
                        )

            # Special case: Lineage Activities (DataJobs) - per specification Section 6
            if (
                hasattr(datahub_graph, "lineage_activities")
                and datahub_graph.lineage_activities
            ):
                logger.info(
                    f"Processing {len(datahub_graph.lineage_activities)} lineage activities (DataJobs)"
                )
                from datahub.ingestion.source.rdf.entities.lineage.mcp_builder import (
                    LineageMCPBuilder,
                )

                for activity in datahub_graph.lineage_activities:
                    try:
                        logger.debug(
                            f"Creating MCP for DataJob: {activity.name} ({activity.urn})"
                        )
                        mcp = LineageMCPBuilder.create_datajob_mcp(activity)
                        mcps.append(mcp)
                        self.report.report_entity_emitted()
                        logger.debug(
                            f"Successfully created DataJob MCP for {activity.name}"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to create MCP for DataJob {activity.urn}: {e}"
                        )

            # Special case: Owner Groups (must be created before domain ownership assignment per Section 8.8)
            # Use owner groups from AST (extracted from RDF properties per Section 8.2)
            owner_iri_to_urn = {}
            owner_iri_to_type = {}

            if hasattr(datahub_graph, "owner_groups") and datahub_graph.owner_groups:
                logger.info(
                    f"Processing {len(datahub_graph.owner_groups)} owner groups from AST"
                )
                from datahub.ingestion.source.rdf.entities.domain.mcp_builder import (
                    DomainMCPBuilder,
                )

                for owner_group in datahub_graph.owner_groups:
                    try:
                        # Create corpGroup MCP using metadata from RDF properties
                        group_mcp = DomainMCPBuilder.create_corpgroup_mcp(
                            group_urn=owner_group.urn,
                            group_name=owner_group.name,  # From rdfs:label
                            group_description=owner_group.description,  # From rdfs:comment
                        )
                        mcps.append(group_mcp)
                        owner_iri_to_urn[owner_group.iri] = owner_group.urn
                        owner_iri_to_type[owner_group.iri] = (
                            owner_group.owner_type
                        )  # From dh:hasOwnerType or RDF type
                        self.report.report_entity_emitted()
                        logger.debug(
                            f"Created corpGroup MCP for owner group: {owner_group.name} ({owner_group.urn})"
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to create corpGroup MCP for owner group {owner_group.iri}: {e}"
                        )

            # Special case: Domains (only create if they have datasets in their hierarchy)
            # Domains are not registered as entity types (they're built, not extracted)
            # So import DomainMCPBuilder directly
            from datahub.ingestion.source.rdf.entities.domain.mcp_builder import (
                DomainMCPBuilder,
            )

            logger.info(f"Processing {len(datahub_graph.domains)} domains")
            domain_mcp_builder = DomainMCPBuilder()
            for domain in datahub_graph.domains:
                try:
                    domain_path = (
                        tuple(domain.path_segments)
                        if domain.path_segments
                        else domain.name
                    )
                    logger.debug(
                        f"Building MCPs for domain: {domain_path} (URN: {domain.urn})"
                    )
                    domain_mcps = domain_mcp_builder.build_mcps(domain)
                    # build_mcps returns empty list if domain has no datasets
                    if not domain_mcps:
                        logger.debug(
                            f"Skipping domain (no datasets in hierarchy): {domain_path}"
                        )
                        continue

                    logger.debug(
                        f"Created {len(domain_mcps)} MCPs for domain: {domain_path}"
                    )
                    mcps.extend(domain_mcps)
                    for _ in domain_mcps:
                        self.report.report_entity_emitted()

                    # Add domain ownership MCP if domain has owners (Section 8.3, 8.8)
                    if hasattr(domain, "owners") and domain.owners:
                        owner_urns = []
                        owner_types = []

                        # Convert owner IRIs to URNs and get owner types from AST (extracted from RDF)
                        for owner_iri in domain.owners:
                            if owner_iri in owner_iri_to_urn:
                                owner_urn = owner_iri_to_urn[owner_iri]
                                owner_urns.append(owner_urn)

                                # Get owner type from AST (extracted from dh:hasOwnerType or RDF type)
                                owner_type = owner_iri_to_type.get(owner_iri)
                                if not owner_type:
                                    raise ValueError(
                                        f"Cannot determine owner type for {owner_iri}. "
                                        f"Owner must have dh:hasOwnerType property in RDF (supports custom owner types)."
                                    )
                                owner_types.append(owner_type)

                        if owner_urns:
                            try:
                                from datahub.ingestion.source.rdf.entities.domain.mcp_builder import (
                                    DomainMCPBuilder,
                                )

                                ownership_mcp = (
                                    DomainMCPBuilder.create_domain_ownership_mcp(
                                        domain_urn=str(domain.urn),
                                        owner_urns=owner_urns,
                                        owner_types=owner_types,
                                    )
                                )
                                mcps.append(ownership_mcp)
                                self.report.report_entity_emitted()
                                logger.debug(
                                    f"Created ownership MCP for domain {domain.name} with {len(owner_urns)} owners"
                                )
                            except Exception as e:
                                logger.warning(
                                    f"Failed to create ownership MCP for domain {domain.urn}: {e}"
                                )

                except Exception as e:
                    logger.warning(f"Failed to create MCP for domain {domain.urn}: {e}")

            # Note: Assertions are processed via the registry pattern above
            # This section is kept for any special assertion handling if needed

            # Log summary of MCPs created
            glossary_mcps = sum(
                1 for mcp in mcps if "glossary" in str(mcp.entityUrn).lower()
            )
            dataset_mcps = sum(
                1 for mcp in mcps if "dataset" in str(mcp.entityUrn).lower()
            )
            structured_prop_mcps = sum(
                1 for mcp in mcps if "structuredproperty" in str(mcp.entityUrn).lower()
            )
            domain_mcps = sum(
                1 for mcp in mcps if "domain" in str(mcp.entityUrn).lower()
            )
            assertion_mcps = sum(
                1 for mcp in mcps if "assertion" in str(mcp.entityUrn).lower()
            )
            lineage_mcps = sum(
                1
                for mcp in mcps
                if hasattr(mcp.aspect, "__class__")
                and "Lineage" in mcp.aspect.__class__.__name__
            )
            relationship_mcps = sum(
                1
                for mcp in mcps
                if hasattr(mcp.aspect, "__class__")
                and "RelatedTerms" in mcp.aspect.__class__.__name__
            )
            other_mcps = (
                len(mcps)
                - glossary_mcps
                - dataset_mcps
                - structured_prop_mcps
                - domain_mcps
                - assertion_mcps
                - lineage_mcps
                - relationship_mcps
            )

            logger.info(f"Generated {len(mcps)} MCPs total:")
            logger.info(f"  - Glossary terms/nodes: {glossary_mcps}")
            logger.info(f"  - Datasets: {dataset_mcps}")
            logger.info(f"  - Structured property definitions: {structured_prop_mcps}")
            logger.info(f"  - Domains: {domain_mcps}")
            logger.info(f"  - Glossary relationships: {relationship_mcps}")
            logger.info(f"  - Lineage: {lineage_mcps}")
            logger.info(f"  - Assertions: {assertion_mcps}")
            logger.info(f"  - Other: {other_mcps}")

            # Convert MCPs to work units
            for i, mcp in enumerate(mcps):
                workunit = MetadataWorkUnit(id=f"rdf-{i}", mcp=mcp)
                self.workunits.append(workunit)
                self.report.report_workunit_produced()

            logger.info(f"Generated {len(self.workunits)} work units from RDF data")

            return {
                "success": True,
                "workunits_generated": len(self.workunits),
                "entities_emitted": self.report.num_entities_emitted,
            }

        except Exception as e:
            logger.error(f"Failed to generate work units: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    def execute(self, datahub_ast: Any, rdf_graph: Any = None) -> Dict[str, Any]:
        """
        Execute the target with the DataHub AST.

        This method is required by TargetInterface and delegates to send().

        Args:
            datahub_ast: DataHubGraph AST containing entities to emit
            rdf_graph: Optional RDF graph (not used in this implementation)

        Returns:
            Results dictionary with success status
        """
        return self.send(datahub_ast)

    def get_target_info(self) -> dict:
        """Get information about this target."""
        return {
            "type": "datahub-ingestion",
            "description": "DataHub ingestion target that creates work units from AST",
            "workunits_generated": len(self.workunits),
            "entities_emitted": self.report.num_entities_emitted if self.report else 0,
        }

    def get_workunits(self) -> List[MetadataWorkUnit]:
        """Get the generated work units."""
        return self.workunits
