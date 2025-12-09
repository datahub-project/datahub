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
from datahub.ingestion.source.rdf.entities.registry import (
    create_default_registry,
)

logger = logging.getLogger(__name__)


class DataHubIngestionTarget:
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

            # Log what entities are in the graph (MVP only)
            logger.info("Processing DataHub AST with:")
            logger.info(f"  - {len(datahub_graph.glossary_terms)} glossary terms")
            logger.info(f"  - {len(datahub_graph.domains)} domains")
            logger.info(f"  - {len(datahub_graph.relationships)} relationships")

            # Generate MCPs for each entity type
            mcps = []

            # Process standard entities in order (using registry pattern)
            # Cross-entity dependencies (structured property values, glossary nodes from domains,
            # dataset-domain associations, domain ownership) are handled via post-processing hooks.
            # Non-registered entities (lineage activities) are handled separately.
            entity_types_by_order = registry.get_entity_types_by_processing_order()

            # Build context with full graph, report, and registry for post-processing hooks
            # Defined outside loop so it's available for deferred post-processing hooks
            build_context = {
                "datahub_graph": datahub_graph,
                "report": self.report,
                "registry": registry,
            }

            for entity_type in entity_types_by_order:
                # Skip domain - domains are only used as a data structure for glossary hierarchy
                # The glossary module will create glossary nodes and terms from domains
                if entity_type == "domain":
                    logger.debug(
                        "Skipping domain MCP creation - domains are used only as data structure for glossary hierarchy"
                    )
                    continue

                mcp_builder = registry.get_mcp_builder(entity_type)
                if not mcp_builder:
                    logger.debug(
                        f"No MCP builder registered for {entity_type}, skipping"
                    )
                    continue

                # Get entity collection from graph (simplified: direct field access)
                # For 2-3 entity types, we can hardcode the field names
                if entity_type == "glossary_term":
                    entities = datahub_graph.glossary_terms
                elif entity_type == "relationship":
                    entities = datahub_graph.relationships
                else:
                    # Fallback for unknown types
                    entities = getattr(datahub_graph, f"{entity_type}s", [])

                if not entities:
                    logger.debug(f"No {entity_type} entities to process")
                    continue

                metadata = registry.get_metadata(entity_type)
                deps_str = (
                    ", ".join(metadata.dependencies)
                    if metadata and metadata.dependencies
                    else "none"
                )
                logger.debug(
                    f"Processing {len(entities)} {entity_type} entities (depends on: {deps_str})"
                )

                # Use build_all_mcps if available, otherwise iterate
                if hasattr(mcp_builder, "build_all_mcps"):
                    try:
                        entity_mcps = mcp_builder.build_all_mcps(
                            entities, build_context
                        )
                        if entity_mcps:
                            mcps.extend(entity_mcps)
                            for _ in entity_mcps:
                                if hasattr(self.report, "report_entity_emitted"):
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
                                    if hasattr(self.report, "report_entity_emitted"):
                                        self.report.report_entity_emitted()
                                    created_count += 1
                        except Exception as e:
                            logger.error(
                                f"Failed to create MCP for {entity_type} {getattr(entity, 'urn', 'unknown')}: {e}",
                                exc_info=True,
                            )
                    logger.debug(
                        f"Created MCPs for {created_count}/{len(entities)} {entity_type} entities"
                    )

                # Call post-processing hook if available (for cross-entity dependencies)
                # EXCEPT for:
                # - structured_property: defer value assignments until after all entities are processed
                # - glossary_term: defer glossary nodes from domains until after domains are processed
                # - domain: defer owner groups and ownership until after domains are processed
                if hasattr(
                    mcp_builder, "build_post_processing_mcps"
                ) and entity_type not in [
                    "structured_property",
                    "glossary_term",
                    "domain",
                ]:
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

            # Note: Assertions, datasets, and lineage are not part of MVP
            # Note: Domains are not created as MCPs - they are only used as a data structure
            #       for the glossary module to understand hierarchy and create glossary nodes

            # Deferred: Glossary term nodes from domain hierarchy
            # These must be created AFTER domains are processed so the domain hierarchy is available
            glossary_term_mcp_builder = registry.get_mcp_builder("glossary_term")
            if glossary_term_mcp_builder and hasattr(
                glossary_term_mcp_builder, "build_post_processing_mcps"
            ):
                try:
                    logger.info(
                        "Processing glossary nodes from domain hierarchy (deferred until after domains)"
                    )
                    post_mcps = glossary_term_mcp_builder.build_post_processing_mcps(
                        datahub_graph, build_context
                    )
                    if post_mcps:
                        mcps.extend(post_mcps)
                        for _ in post_mcps:
                            if hasattr(self.report, "report_entity_emitted"):
                                self.report.report_entity_emitted()
                        logger.info(
                            f"Created {len(post_mcps)} glossary node/term MCPs from domain hierarchy"
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to create glossary node MCPs from domain hierarchy: {e}",
                        exc_info=True,
                    )

            # Deferred: Structured property value assignments
            # These must be created AFTER all other entities (including definitions) are processed
            # to ensure definitions are committed before value assignments are validated
            structured_property_mcp_builder = registry.get_mcp_builder(
                "structured_property"
            )
            if structured_property_mcp_builder and hasattr(
                structured_property_mcp_builder, "build_post_processing_mcps"
            ):
                try:
                    logger.info(
                        "Processing structured property value assignments (deferred until after all entities)"
                    )
                    post_mcps = (
                        structured_property_mcp_builder.build_post_processing_mcps(
                            datahub_graph, build_context
                        )
                    )
                    if post_mcps:
                        mcps.extend(post_mcps)
                        for _ in post_mcps:
                            if hasattr(self.report, "report_entity_emitted"):
                                self.report.report_entity_emitted()
                        logger.info(
                            f"Created {len(post_mcps)} structured property value assignment MCPs"
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to create structured property value assignment MCPs: {e}",
                        exc_info=True,
                    )

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
                - assertion_mcps
                - lineage_mcps
                - relationship_mcps
            )

            logger.info(f"Generated {len(mcps)} MCPs total:")
            logger.info(f"  - Glossary terms/nodes: {glossary_mcps}")
            logger.info(f"  - Datasets: {dataset_mcps}")
            logger.info(f"  - Structured property definitions: {structured_prop_mcps}")
            logger.info(f"  - Glossary relationships: {relationship_mcps}")
            logger.debug(
                f"  - Domains (data structure only, not ingested): {len(datahub_graph.domains)}"
            )
            logger.info(f"  - Lineage: {lineage_mcps}")
            logger.info(f"  - Assertions: {assertion_mcps}")
            logger.info(f"  - Other: {other_mcps}")

            # Convert MCPs to work units
            for i, mcp in enumerate(mcps):
                workunit = MetadataWorkUnit(id=f"rdf-{i}", mcp=mcp)
                self.workunits.append(workunit)
                if hasattr(self.report, "report_workunit_produced"):
                    self.report.report_workunit_produced()

            logger.info(f"Generated {len(self.workunits)} work units from RDF data")

            return {
                "success": True,
                "workunits_generated": len(self.workunits),
                "entities_emitted": getattr(self.report, "num_entities_emitted", 0),
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
            "entities_emitted": getattr(self.report, "num_entities_emitted", 0)
            if self.report
            else 0,
        }

    def get_workunits(self) -> List[MetadataWorkUnit]:
        """Get the generated work units."""
        return self.workunits
