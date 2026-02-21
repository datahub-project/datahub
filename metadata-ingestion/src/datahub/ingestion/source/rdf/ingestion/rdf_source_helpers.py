#!/usr/bin/env python3
"""
Helper utilities for RDF source processing.
"""

import logging
from typing import Any, Dict, Iterable, List

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.rdf.core.ast import DataHubGraph

logger = logging.getLogger(__name__)


def get_entities_from_graph(datahub_graph: DataHubGraph, entity_type: str) -> List[Any]:
    """Get entity collection from graph."""
    if entity_type == "glossary_term":
        return datahub_graph.glossary_terms
    if entity_type == "relationship":
        return datahub_graph.relationships
    return getattr(datahub_graph, f"{entity_type}s", [])


def log_entity_processing(
    entity_type: str, entities: List[Any], registry: Any, report: Any
) -> None:
    """Log entity processing information."""
    metadata = registry.get_metadata(entity_type)
    deps_str = (
        ", ".join(metadata.dependencies)
        if metadata and metadata.dependencies
        else "none"
    )
    logger.debug(
        f"Processing {len(entities)} {entity_type} entities (depends on: {deps_str})"
    )


def build_entity_mcps(
    mcp_builder: Any,
    entities: List[Any],
    entity_type: str,
    build_context: Dict[str, Any],
    report: Any,
) -> List[Any]:
    """Build MCPs for entities."""
    mcps = []
    if hasattr(mcp_builder, "build_all_mcps"):
        try:
            entity_mcps = mcp_builder.build_all_mcps(entities, build_context)
            if entity_mcps:
                mcps.extend(entity_mcps)
                for _ in entity_mcps:
                    report.report_entity_emitted()
                    # Track entity type statistics
                    if entity_type == "glossary_term":
                        report.report_glossary_term()
                    elif entity_type == "relationship":
                        report.report_relationship()
                logger.debug(
                    f"Created {len(entity_mcps)} MCPs for {len(entities)} {entity_type} entities"
                )
            else:
                logger.debug(
                    f"No MCPs created for {len(entities)} {entity_type} entities (they may have been filtered out)"
                )
        except RuntimeError as e:
            report.report_failure(
                f"Failed to create MCPs for {entity_type}",
                context=f"Entity count: {len(entities)}",
                exc=e,
            )
            logger.error(f"Failed to create MCPs for {entity_type}: {e}", exc_info=True)
    else:
        created_count = 0
        for entity in entities:
            try:
                entity_mcps = mcp_builder.build_mcps(entity, build_context)
                if entity_mcps:
                    mcps.extend(entity_mcps)
                    for _ in entity_mcps:
                        report.report_entity_emitted()
                        created_count += 1
            except RuntimeError as e:
                entity_urn = getattr(entity, "urn", "unknown")
                report.report_failure(
                    f"Failed to create MCP for {entity_type}",
                    context=f"Entity URN: {entity_urn}",
                    exc=e,
                )
                logger.error(
                    f"Failed to create MCP for {entity_type} {entity_urn}: {e}",
                    exc_info=True,
                )
                # Continue processing other entities
        logger.debug(
            f"Created MCPs for {created_count}/{len(entities)} {entity_type} entities"
        )
    return mcps


def process_post_processing_hooks(
    mcp_builder: Any,
    entity_type: str,
    datahub_graph: DataHubGraph,
    build_context: Dict[str, Any],
    report: Any,
) -> Iterable[Any]:
    """
    Process post-processing hooks for cross-entity dependencies.

    Yields MCPs as they're generated.
    """
    if hasattr(mcp_builder, "build_post_processing_mcps") and entity_type not in [
        "structured_property",
        "glossary_term",
        "domain",
    ]:
        try:
            post_mcps = mcp_builder.build_post_processing_mcps(
                datahub_graph, build_context
            )
            if post_mcps:
                logger.debug(
                    f"Created {len(post_mcps)} post-processing MCPs for {entity_type}"
                )
                for mcp in post_mcps:
                    yield mcp
        except RuntimeError as e:
            report.report_failure(
                f"Failed to create post-processing MCPs for {entity_type}",
                context="Post-processing hook",
                exc=e,
            )
            logger.error(
                f"Failed to create post-processing MCPs for {entity_type}: {e}",
                exc_info=True,
            )


def log_mcp_summary(mcps: List[Any], datahub_graph: DataHubGraph) -> None:
    """Log summary of MCPs created."""
    glossary_mcps = sum(1 for mcp in mcps if "glossary" in str(mcp.entityUrn).lower())
    dataset_mcps = sum(1 for mcp in mcps if "dataset" in str(mcp.entityUrn).lower())
    structured_prop_mcps = sum(
        1 for mcp in mcps if "structuredproperty" in str(mcp.entityUrn).lower()
    )
    assertion_mcps = sum(1 for mcp in mcps if "assertion" in str(mcp.entityUrn).lower())
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


def convert_mcps_to_workunits(mcps: List[Any], report: Any) -> List[MetadataWorkUnit]:
    """Convert MCPs to work units."""
    workunits = []
    for i, mcp in enumerate(mcps):
        workunit = MetadataWorkUnit(id=f"rdf-{i}", mcp=mcp)
        workunits.append(workunit)
        report.report_workunit_produced()
    return workunits
