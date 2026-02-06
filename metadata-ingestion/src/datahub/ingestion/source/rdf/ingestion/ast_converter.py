#!/usr/bin/env python3
"""
RDF to DataHub AST Converter.

Converts RDF graphs to DataHub AST representations.
"""

import logging
from typing import Any, Dict, List, Optional

from rdflib import Graph

from datahub.ingestion.source.rdf.core.ast import DataHubGraph
from datahub.ingestion.source.rdf.entities.domain.builder import DomainBuilder
from datahub.ingestion.source.rdf.entities.registry import create_default_registry
from datahub.ingestion.source.rdf.rdf_config import RDFSourceConfig

logger = logging.getLogger(__name__)


class RDFToASTConverter:
    """
    Converts RDF graphs to DataHub AST.

    Handles entity extraction, domain building, and dialect processing.
    """

    def __init__(self, config: RDFSourceConfig, report: Any):
        """
        Initialize the converter.

        Args:
            config: RDF source configuration
            report: Report object for tracking errors
        """
        self.config = config
        self.report = report

    def convert(
        self,
        graph: Graph,
        environment: str,
        export_only: Optional[List[str]] = None,
        skip_export: Optional[List[str]] = None,
    ) -> DataHubGraph:
        """
        Convert RDF graph to DataHub AST.

        Args:
            graph: RDF graph to convert
            environment: DataHub environment
            export_only: Optional list of entity types to export
            skip_export: Optional list of entity types to skip

        Returns:
            DataHub AST graph
        """
        registry = create_default_registry()
        dialect_instance = self._create_dialect_instance()

        context = {
            "environment": environment,
            "export_only": export_only,
            "skip_export": skip_export,
            "dialect": dialect_instance,
            "include_provisional": self.config.include_provisional,
        }

        # Helper to check if a CLI name should be processed
        def should_process_cli_name(cli_name: str) -> bool:
            if export_only and cli_name not in export_only:
                return False
            if skip_export and cli_name in skip_export:
                return False
            return True

        # Create DataHubGraph
        datahub_graph = DataHubGraph()

        # Extract glossary terms
        if should_process_cli_name("glossary"):
            datahub_graph.glossary_terms = self._extract_glossary_terms(
                graph, registry, context
            )

        # Extract relationships
        if should_process_cli_name("relationship"):
            datahub_graph.relationships = self._extract_relationships(
                graph, registry, context
            )

        # Build domains
        datahub_graph.domains = self._build_domains(
            datahub_graph.glossary_terms, context
        )

        return datahub_graph

    def convert_safe(self, rdf_graph: Graph) -> Optional[DataHubGraph]:
        """
        Convert RDF graph to DataHub AST with error handling.

        Args:
            rdf_graph: Loaded RDF graph

        Returns:
            DataHub AST, or None if conversion failed
        """
        try:
            logger.info("Converting RDF to DataHub AST")
            datahub_ast = self.convert(
                rdf_graph,
                environment=self.config.environment,
                export_only=self.config.export_only,
                skip_export=self.config.skip_export,
            )
            summary = datahub_ast.get_summary()
            summary_str = ", ".join(
                [f"{count} {name}" for name, count in summary.items()]
            )
            logger.info(f"DataHub AST created: {summary_str}")
            return datahub_ast
        except (RuntimeError, ValueError) as e:
            self.report.report_failure(
                "Failed to convert RDF to DataHub AST",
                context=f"Triples processed: {len(rdf_graph)}",
                exc=e,
            )
            logger.error(
                f"Failed to convert RDF to DataHub AST: {e}",
                exc_info=True,
            )
            return None

    def _create_dialect_instance(self):
        """Create dialect instance from config or default to auto-detection."""
        from datahub.ingestion.source.rdf.dialects.base import RDFDialect
        from datahub.ingestion.source.rdf.dialects.bcbs239 import DefaultDialect
        from datahub.ingestion.source.rdf.dialects.fibo import FIBODialect
        from datahub.ingestion.source.rdf.dialects.generic import GenericDialect
        from datahub.ingestion.source.rdf.dialects.router import DialectRouter

        if hasattr(self, "config") and self.config.dialect:
            try:
                dialect_enum = RDFDialect(self.config.dialect)
                if dialect_enum == RDFDialect.FIBO:
                    include_provisional = (
                        self.config.include_provisional
                        if hasattr(self.config, "include_provisional")
                        else False
                    )
                    return FIBODialect(include_provisional=include_provisional)
                if dialect_enum == RDFDialect.DEFAULT:
                    return DefaultDialect()
                if dialect_enum == RDFDialect.GENERIC:
                    return GenericDialect()
            except ValueError:
                logger.warning(
                    f"Invalid dialect '{self.config.dialect}', defaulting to auto-detection"
                )

        include_provisional = (
            self.config.include_provisional
            if hasattr(self.config, "include_provisional")
            else False
        )
        return DialectRouter(include_provisional=include_provisional)

    def _extract_glossary_terms(
        self, graph: Graph, registry: Any, context: Dict[str, Any]
    ) -> List[Any]:
        """Extract glossary terms from RDF graph."""
        entity_type = (
            registry.get_entity_type_from_cli_name("glossary") or "glossary_term"
        )
        extractor = registry.get_extractor(entity_type)

        if not extractor:
            return []

        try:
            datahub_terms = extractor.extract_all(graph, context)
            logger.debug(
                f"Extracted {len(datahub_terms)} glossary terms from RDF graph"
            )
            return datahub_terms
        except (RuntimeError, ValueError) as e:
            self.report.report_failure(
                "Failed to extract glossary terms",
                context=f"Entity type: {entity_type}",
                exc=e,
            )
            logger.error(f"Failed to extract glossary terms: {e}", exc_info=True)
            return []

    def _extract_relationships(
        self, graph: Graph, registry: Any, context: Dict[str, Any]
    ) -> List[Any]:
        """Extract relationships from RDF graph."""
        entity_type = (
            registry.get_entity_type_from_cli_name("relationship") or "relationship"
        )
        extractor = registry.get_extractor(entity_type)

        if not extractor:
            return []

        try:
            rdf_relationships = extractor.extract_all(graph, context)
            converter = registry.get_converter(entity_type)
            if not converter:
                return []

            datahub_relationships = []
            for rdf_rel in rdf_relationships:
                try:
                    datahub_rel = converter.convert(rdf_rel, context)
                    if datahub_rel:
                        datahub_relationships.append(datahub_rel)
                except (RuntimeError, ValueError) as e:
                    rel_source = (
                        str(rdf_rel.source_urn)
                        if hasattr(rdf_rel, "source_urn")
                        else "unknown"
                    )
                    rel_target = (
                        str(rdf_rel.target_urn)
                        if hasattr(rdf_rel, "target_urn")
                        else "unknown"
                    )
                    self.report.report_failure(
                        "Failed to convert relationship",
                        context=f"Source: {rel_source}, Target: {rel_target}",
                        exc=e,
                    )
                    logger.warning(
                        f"Failed to convert relationship {rel_source} -> {rel_target}: {e}",
                        exc_info=True,
                    )

            logger.debug(
                f"Extracted {len(datahub_relationships)} relationships from RDF graph"
            )
            return datahub_relationships
        except (RuntimeError, ValueError) as e:
            self.report.report_failure(
                "Failed to extract relationships",
                context=f"Entity type: {entity_type}",
                exc=e,
            )
            logger.error(f"Failed to extract relationships: {e}", exc_info=True)
            return []

    def _build_domains(
        self, glossary_terms: List[Any], context: Dict[str, Any]
    ) -> List[Any]:
        """Build domain hierarchy from glossary terms."""
        try:
            domain_builder = DomainBuilder()
            domains = domain_builder.build_domains(glossary_terms, context)
            logger.debug(f"Built {len(domains)} domains from glossary term hierarchy")
            return domains
        except (RuntimeError, ValueError) as e:
            self.report.report_failure(
                "Failed to build domain hierarchy",
                context=f"Glossary terms: {len(glossary_terms)}",
                exc=e,
            )
            logger.error(f"Failed to build domain hierarchy: {e}", exc_info=True)
            return []
