#!/usr/bin/env python3
"""
Work Unit Generator.

Generates MetadataWorkUnits from DataHub AST.
"""

import logging
from typing import Any, Dict, Iterable, Optional

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.rdf.core.ast import DataHubGraph
from datahub.ingestion.source.rdf.entities.registry import create_default_registry

logger = logging.getLogger(__name__)


class WorkUnitGenerator:
    """
    Generates MetadataWorkUnits from DataHub AST.

    Handles entity processing, MCP building, and work unit creation.
    """

    def __init__(self, report: Any):
        """
        Initialize the generator.

        Args:
            report: Report object for tracking statistics and errors
        """
        self.report = report

    def generate(self, datahub_graph: DataHubGraph) -> Iterable[MetadataWorkUnit]:
        """
        Generate work units from DataHub AST.

        Uses generator pattern for memory efficiency.

        Args:
            datahub_graph: DataHubGraph AST containing entities to emit

        Yields:
            MetadataWorkUnit objects as they're generated
        """

        if not isinstance(datahub_graph, DataHubGraph):
            logger.error(f"Expected DataHubGraph, got {type(datahub_graph)}")
            self.report.report_failure(f"Invalid AST type: {type(datahub_graph)}")
            return  # Generator returns nothing on error

        try:
            registry = create_default_registry()
            build_context = {
                "datahub_graph": datahub_graph,
                "report": self.report,
                "registry": registry,
            }

            logger.info("Processing DataHub AST with:")
            logger.info(f"  - {len(datahub_graph.glossary_terms)} glossary terms")
            logger.info(f"  - {len(datahub_graph.domains)} domains")
            logger.info(f"  - {len(datahub_graph.relationships)} relationships")

            # Process standard entities and yield work units incrementally
            mcp_count = 0
            workunit_id = 0
            for mcp in self._process_standard_entities(
                datahub_graph, registry, build_context
            ):
                mcp_count += 1
                workunit = MetadataWorkUnit(id=f"rdf-{workunit_id}", mcp=mcp)
                workunit_id += 1
                self.report.report_workunit_produced()
                yield workunit

            # Process deferred entities and yield work units incrementally
            for mcp in self._process_deferred_entities(
                datahub_graph, registry, build_context
            ):
                mcp_count += 1
                workunit = MetadataWorkUnit(id=f"rdf-{workunit_id}", mcp=mcp)
                workunit_id += 1
                self.report.report_workunit_produced()
                yield workunit

            # Log summary (simplified since we don't have full MCP list)
            logger.info(f"Generated {mcp_count} MCPs total:")
            logger.info(f"  - Glossary terms: {len(datahub_graph.glossary_terms)}")
            logger.info(f"  - Relationships: {len(datahub_graph.relationships)}")
            logger.debug(
                f"  - Domains (data structure only, not ingested): {len(datahub_graph.domains)}"
            )

        except RuntimeError as e:
            self.report.report_failure(
                "Failed to generate work units from DataHub AST",
                context=f"Glossary terms: {len(datahub_graph.glossary_terms)}, "
                f"Domains: {len(datahub_graph.domains)}, "
                f"Relationships: {len(datahub_graph.relationships)}",
                exc=e,
            )
            logger.error(f"Failed to generate work units: {e}", exc_info=True)
            return  # Generator returns nothing on error

    def generate_safe(
        self, datahub_ast: DataHubGraph
    ) -> Optional[Iterable[MetadataWorkUnit]]:
        """
        Generate work units from DataHub AST with error handling.

        Args:
            datahub_ast: DataHub AST to generate work units from

        Returns:
            Generator of work units, or None if generation failed
        """
        try:
            logger.info("Generating work units from DataHub AST")
            workunits = self.generate(datahub_ast)
            return workunits
        except RuntimeError as e:
            summary = datahub_ast.get_summary()
            summary_str = ", ".join(
                [f"{count} {name}" for name, count in summary.items()]
            )
            self.report.report_failure(
                "Failed to generate work units",
                context=f"AST summary: {summary_str}",
                exc=e,
            )
            logger.error(
                f"Failed to generate work units from DataHub AST: {e}",
                exc_info=True,
            )
            return None

    def yield_with_error_handling(
        self, workunits: Iterable[MetadataWorkUnit]
    ) -> Iterable[MetadataWorkUnit]:
        """
        Yield work units with individual error handling.

        Args:
            workunits: Generator of work units to yield

        Yields:
            MetadataWorkUnit objects, skipping any that fail
        """
        for workunit in workunits:
            try:
                yield workunit
            except RuntimeError as e:
                workunit_id = getattr(workunit, "id", "unknown")
                entity_urn = (
                    str(workunit.mcp.entityUrn)
                    if hasattr(workunit, "mcp") and hasattr(workunit.mcp, "entityUrn")
                    else "unknown"
                )
                self.report.report_failure(
                    "Failed to process work unit",
                    context=f"Work unit ID: {workunit_id}, Entity URN: {entity_urn}",
                    exc=e,
                )
                logger.error(
                    f"Failed to process work unit {workunit_id} (entity: {entity_urn}): {e}",
                    exc_info=True,
                )
                # Continue to next work unit

    def _process_standard_entities(
        self, datahub_graph: DataHubGraph, registry: Any, build_context: Dict[str, Any]
    ) -> Iterable[Any]:
        """
        Process standard entities in processing order.

        Yields MCPs as they're generated for memory efficiency.
        """
        from datahub.ingestion.source.rdf.ingestion.rdf_source_helpers import (
            build_entity_mcps,
            get_entities_from_graph,
            log_entity_processing,
            process_post_processing_hooks,
        )

        entity_types_by_order = registry.get_entity_types_by_processing_order()

        for entity_type in entity_types_by_order:
            if entity_type == "domain":
                logger.debug(
                    "Skipping domain MCP creation - domains are used only as data structure for glossary hierarchy"
                )
                continue

            try:
                mcp_builder = registry.get_mcp_builder(entity_type)
                if not mcp_builder:
                    logger.debug(
                        f"No MCP builder registered for {entity_type}, skipping"
                    )
                    continue

                entities = get_entities_from_graph(datahub_graph, entity_type)
                if not entities:
                    logger.debug(f"No {entity_type} entities to process")
                    continue

                log_entity_processing(entity_type, entities, registry, self.report)
                entity_mcps = build_entity_mcps(
                    mcp_builder, entities, entity_type, build_context, self.report
                )
                # Yield MCPs as they're generated
                for mcp in entity_mcps:
                    yield mcp
                # Process post-processing hooks and yield their MCPs
                for mcp in process_post_processing_hooks(
                    mcp_builder, entity_type, datahub_graph, build_context, self.report
                ):
                    yield mcp
            except RuntimeError as e:
                # Continue processing other entity types even if one fails
                self.report.report_failure(
                    f"Failed to process {entity_type} entities",
                    context=f"Entity type: {entity_type}",
                    exc=e,
                )
                logger.error(
                    f"Failed to process {entity_type} entities: {e}",
                    exc_info=True,
                )
                # Continue to next entity type

    def _process_deferred_entities(
        self, datahub_graph: DataHubGraph, registry: Any, build_context: Dict[str, Any]
    ) -> Iterable[Any]:
        """
        Process deferred entities (glossary terms, structured properties).

        Yields MCPs as they're generated for memory efficiency.
        """

        # Deferred: Glossary term nodes from domain hierarchy
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
                    for mcp in post_mcps:
                        self.report.report_entity_emitted()
                        # Track glossary nodes and terms from post-processing
                        if hasattr(mcp, "entityType"):
                            if mcp.entityType == "glossaryNode":
                                self.report.report_glossary_node()
                            elif mcp.entityType == "glossaryTerm":
                                self.report.report_glossary_term()
                        yield mcp
                    logger.info(
                        f"Created {len(post_mcps)} glossary node/term MCPs from domain hierarchy"
                    )
            except RuntimeError as e:
                self.report.report_failure(
                    "Failed to create glossary node MCPs from domain hierarchy",
                    context=f"Domains: {len(datahub_graph.domains)}",
                    exc=e,
                )
                logger.error(
                    f"Failed to create glossary node MCPs from domain hierarchy: {e}",
                    exc_info=True,
                )

        # Deferred: Structured property value assignments
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
                post_mcps = structured_property_mcp_builder.build_post_processing_mcps(
                    datahub_graph, build_context
                )
                if post_mcps:
                    for mcp in post_mcps:
                        self.report.report_entity_emitted()
                        yield mcp
                    logger.info(
                        f"Created {len(post_mcps)} structured property value assignment MCPs"
                    )
            except RuntimeError as e:
                self.report.report_failure(
                    "Failed to create structured property value assignment MCPs",
                    context="Post-processing hook",
                    exc=e,
                )
                logger.error(
                    f"Failed to create structured property value assignment MCPs: {e}",
                    exc_info=True,
                )
