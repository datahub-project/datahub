"""
Entity Pipeline

Orchestrates entity processing through the modular architecture.
Provides a unified interface for processing entities through all stages.
"""

import logging
from typing import Any, Dict, List

from rdflib import Graph

from datahub.ingestion.source.rdf.entities.registry import (
    EntityRegistry,
    create_default_registry,
)

# Import DataHubGraph lazily to avoid circular imports

logger = logging.getLogger(__name__)


class EntityPipeline:
    """
    Orchestrates entity processing through the modular architecture.

    Provides methods for:
    - Running specific entity types through the pipeline
    - Running all registered entity types
    - Collecting results at each stage

    Usage:
        pipeline = EntityPipeline()

        # Process glossary terms only
        mcps = pipeline.process_entity_type(graph, 'glossary_term')

        # Process all entity types
        all_mcps = pipeline.process_all(graph)
    """

    def __init__(self, registry: EntityRegistry | None = None):
        """
        Initialize the pipeline.

        Args:
            registry: Optional registry. If not provided, uses default registry.
        """
        self.registry = registry or create_default_registry()

    def process_entity_type(
        self, graph: Graph, entity_type: str, context: Dict[str, Any] | None = None
    ) -> List[Any]:
        """
        Process a specific entity type through the full pipeline.

        Args:
            graph: The RDF graph
            entity_type: The type of entity to process (e.g., 'glossary_term')
            context: Optional shared context

        Returns:
            List of MCPs for the entity type
        """
        processor = self.registry.get_processor(entity_type)
        if not processor:
            logger.warning(f"No processor registered for entity type: {entity_type}")
            return []

        return processor.process(graph, context or {})

    def process_all(
        self, graph: Graph, context: Dict[str, Any] | None = None
    ) -> List[Any]:
        """
        Process all registered entity types through the pipeline.

        Args:
            graph: The RDF graph
            context: Optional shared context

        Returns:
            List of all MCPs from all entity types
        """
        all_mcps = []
        ctx = context or {}

        for entity_type in self.registry.list_entity_types():
            mcps = self.process_entity_type(graph, entity_type, ctx)
            all_mcps.extend(mcps)
            logger.info(f"Processed {entity_type}: {len(mcps)} MCPs")

        return all_mcps

    def extract_entity_type(
        self, graph: Graph, entity_type: str, context: Dict[str, Any] | None = None
    ) -> List[Any]:
        """
        Extract entities of a specific type (Stage 1 only).

        Args:
            graph: The RDF graph
            entity_type: The type of entity to extract
            context: Optional shared context

        Returns:
            List of RDF AST entities
        """
        extractor = self.registry.get_extractor(entity_type)
        if not extractor:
            logger.warning(f"No extractor registered for entity type: {entity_type}")
            return []

        return extractor.extract_all(graph, context or {})

    def convert_entities(
        self, rdf_entities: List[Any], entity_type: str, context: Dict[str, Any] = None
    ) -> List[Any]:
        """
        Convert RDF AST entities to DataHub AST (Stage 2 only).

        Args:
            rdf_entities: List of RDF AST entities
            entity_type: The type of entities being converted
            context: Optional shared context

        Returns:
            List of DataHub AST entities
        """
        converter = self.registry.get_converter(entity_type)
        if not converter:
            logger.warning(f"No converter registered for entity type: {entity_type}")
            return []

        return converter.convert_all(rdf_entities, context or {})

    def build_mcps(
        self,
        datahub_entities: List[Any],
        entity_type: str,
        context: Dict[str, Any] | None = None,
    ) -> List[Any]:
        """
        Build MCPs from DataHub AST entities (Stage 3 only).

        Args:
            datahub_entities: List of DataHub AST entities
            entity_type: The type of entities
            context: Optional shared context

        Returns:
            List of MCPs
        """
        mcp_builder = self.registry.get_mcp_builder(entity_type)
        if not mcp_builder:
            logger.warning(f"No MCP builder registered for entity type: {entity_type}")
            return []

        return mcp_builder.build_all_mcps(datahub_entities, context or {})

    def build_relationship_mcps(
        self, graph: Graph, context: Dict[str, Any] | None = None
    ) -> List[Any]:
        """
        Build relationship MCPs specifically for glossary terms.

        This is a convenience method that extracts terms, collects their relationships,
        and creates relationship MCPs.

        Args:
            graph: The RDF graph
            context: Optional shared context

        Returns:
            List of relationship MCPs
        """
        # Get the glossary term components
        extractor = self.registry.get_extractor("glossary_term")
        converter = self.registry.get_converter("glossary_term")
        mcp_builder = self.registry.get_mcp_builder("glossary_term")

        if not all([extractor, converter, mcp_builder]):
            logger.warning("Glossary term processor not fully registered")
            return []

        # Type narrowing - mypy doesn't understand all() check
        assert extractor is not None
        assert converter is not None
        assert mcp_builder is not None

        # Extract terms
        rdf_terms = extractor.extract_all(graph, context or {})

        # Collect relationships using the converter
        from datahub.ingestion.source.rdf.entities.glossary_term.converter import (
            GlossaryTermConverter,
        )

        if isinstance(converter, GlossaryTermConverter):
            relationships = converter.collect_relationships(rdf_terms, context)

            # Build relationship MCPs using the MCP builder
            from datahub.ingestion.source.rdf.entities.glossary_term.mcp_builder import (
                GlossaryTermMCPBuilder,
            )

            if isinstance(mcp_builder, GlossaryTermMCPBuilder):
                return mcp_builder.build_relationship_mcps(relationships, context)

        return []
