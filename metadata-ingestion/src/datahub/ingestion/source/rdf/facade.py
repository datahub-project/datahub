"""
RDF-Lite Facade

Single entry point for processing RDF data to DataHub format.
This facade abstracts the internal implementation, allowing it to be
replaced without changing the public API.

Usage:
    facade = RDFFacade()
    result = facade.process(graph, environment="PROD")
    mcps = facade.generate_mcps(graph, environment="PROD")
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from rdflib import Graph

logger = logging.getLogger(__name__)


@dataclass
class ProcessedGlossaryTerm:
    """Processed glossary term result."""

    urn: str
    name: str
    definition: Optional[str] = None
    source: Optional[str] = None
    custom_properties: Dict[str, Any] = field(default_factory=dict)
    path_segments: tuple = field(default_factory=tuple)
    relationships: Dict[str, List[str]] = field(default_factory=dict)


@dataclass
class ProcessedSchemaField:
    """Processed schema field result."""

    name: str
    field_type: str
    description: Optional[str] = None
    nullable: bool = True


@dataclass
class ProcessedDomain:
    """Processed domain result."""

    urn: str
    name: str
    path_segments: tuple
    parent_domain_urn: Optional[str] = None
    glossary_terms: List[ProcessedGlossaryTerm] = field(default_factory=list)
    subdomains: List["ProcessedDomain"] = field(default_factory=list)


@dataclass
class ProcessedRelationship:
    """Processed relationship result."""

    source_urn: str
    target_urn: str
    relationship_type: Any  # RelationshipType enum
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProcessingResult:
    """Complete processing result from the facade."""

    glossary_terms: List[ProcessedGlossaryTerm] = field(default_factory=list)
    domains: List[ProcessedDomain] = field(default_factory=list)
    relationships: List[ProcessedRelationship] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class RDFFacade:
    """
    Single entry point for RDF-to-DataHub processing.

    This facade provides a stable API that abstracts the internal
    implementation. The implementation can be switched from monolithic
    to modular without changing client code.
    """

    def __init__(self):
        """Initialize the facade."""
        pass

    def process(
        self,
        graph: Graph,
        environment: str = "PROD",
        export_only: List[str] | None = None,
        skip_export: List[str] | None = None,
        create_assertions: bool = False,
        assertion_types: Dict[str, bool] | None = None,
    ) -> ProcessingResult:
        """
        Process an RDF graph and return structured results.

        Args:
            graph: RDFLib Graph containing the RDF data
            environment: DataHub environment (PROD, DEV, etc.)
            export_only: Optional list of entity types to export
            skip_export: Optional list of entity types to skip
            create_assertions: If True, enables assertion creation (default: False)
            assertion_types: Dict with sub-flags for assertion types:
                - required_fields: bool (for minCount/maxCount → NOT_NULL)
                - field_size: bool (for minLength/maxLength)
                - value_checks: bool (for minInclusive/maxInclusive, pattern)

        Returns:
            ProcessingResult with all extracted and converted entities
        """
        return self._process_modular(
            graph,
            environment,
            export_only,
            skip_export,
            create_assertions,
            assertion_types,
        )

    def _process_modular(
        self,
        graph: Graph,
        environment: str,
        export_only: List[str] | None = None,
        skip_export: List[str] | None = None,
        create_assertions: bool = False,
        assertion_types: Dict[str, bool] | None = None,
    ) -> ProcessingResult:
        """Process using the new modular entity-based implementation."""
        from datahub.ingestion.source.rdf.entities.domain.builder import DomainBuilder
        from datahub.ingestion.source.rdf.entities.registry import (
            create_default_registry,
        )

        registry = create_default_registry()

        # Build context with assertion configuration
        context = {
            "environment": environment,
            "export_only": export_only,
            "skip_export": skip_export,
            "create_assertions": create_assertions,
            "assertion_types": assertion_types or {},
        }

        result = ProcessingResult()

        # Helper to check if a CLI name should be processed
        def should_process_cli_name(cli_name: str) -> bool:
            """Check if a CLI name (e.g., 'glossary', 'datasets') should be processed."""
            if export_only and cli_name not in export_only:
                return False
            if skip_export and cli_name in skip_export:
                return False
            return True

        # Helper to get entity type from CLI name
        def get_entity_type(cli_name: str) -> Optional[str]:
            """Get entity type from CLI name using registry."""
            return registry.get_entity_type_from_cli_name(cli_name)

        # Extract and convert glossary terms
        if should_process_cli_name("glossary"):
            entity_type = get_entity_type("glossary") or "glossary_term"
            extractor = registry.get_extractor(entity_type)
            converter = registry.get_converter(entity_type)

            if extractor and converter:
                rdf_terms = extractor.extract_all(graph, context)
                datahub_terms = converter.convert_all(rdf_terms, context)

                for term in datahub_terms:
                    result.glossary_terms.append(
                        ProcessedGlossaryTerm(
                            urn=term.urn,
                            name=term.name,
                            definition=term.definition,
                            source=term.source,
                            custom_properties=term.custom_properties or {},
                            path_segments=tuple(term.path_segments)
                            if term.path_segments
                            else (),
                            relationships=term.relationships or {},
                        )
                    )

                # Collect relationships from terms
                from datahub.ingestion.source.rdf.entities.glossary_term.converter import (
                    GlossaryTermConverter,
                )

                if isinstance(converter, GlossaryTermConverter):
                    relationships = converter.collect_relationships(rdf_terms, context)
                    for rel in relationships:
                        result.relationships.append(
                            ProcessedRelationship(
                                source_urn=str(rel.source_urn),
                                target_urn=str(rel.target_urn),
                                relationship_type=rel.relationship_type,
                                properties=rel.properties or {},
                            )
                        )
            else:
                logger.warning(f"Extractor or converter not found for {entity_type}")

        # Build domains using DomainBuilder (creates its own URN generator)
        domain_builder = DomainBuilder()

        # Convert ProcessedGlossaryTerm to DataHub types for domain builder
        from datahub.ingestion.source.rdf.entities.glossary_term.ast import (
            DataHubGlossaryTerm,
        )

        dh_terms = []
        for t in result.glossary_terms:
            dh_terms.append(
                DataHubGlossaryTerm(
                    urn=t.urn,
                    name=t.name,
                    definition=t.definition,
                    source=t.source,
                    relationships=t.relationships,
                    custom_properties=t.custom_properties,
                    path_segments=list(t.path_segments),
                )
            )

        datahub_domains = domain_builder.build_domains(dh_terms, context)

        for domain in datahub_domains:
            result.domains.append(self._convert_domain(domain))

        return result

    def _convert_datahub_ast_to_result(self, datahub_ast: Any) -> ProcessingResult:
        """Convert DataHub AST to ProcessingResult."""
        result = ProcessingResult()

        # Convert glossary terms
        for term in datahub_ast.glossary_terms:
            result.glossary_terms.append(
                ProcessedGlossaryTerm(
                    urn=term.urn,
                    name=term.name,
                    definition=term.definition,
                    source=term.source,
                    custom_properties=term.custom_properties or {},
                    path_segments=tuple(term.path_segments)
                    if term.path_segments
                    else (),
                    relationships=term.relationships or {},
                )
            )

        # Convert domains
        for domain in datahub_ast.domains:
            processed_domain = self._convert_domain(domain)
            result.domains.append(processed_domain)

        # Convert relationships
        for rel in datahub_ast.relationships:
            result.relationships.append(
                ProcessedRelationship(
                    source_urn=str(rel.source_urn),
                    target_urn=str(rel.target_urn),
                    relationship_type=rel.relationship_type,
                    properties=rel.properties or {},
                )
            )

        # Add metadata
        result.metadata = (
            datahub_ast.get_summary() if hasattr(datahub_ast, "get_summary") else {}
        )

        return result

    def _convert_domain(self, domain: Any) -> ProcessedDomain:
        """Convert a DataHub domain to ProcessedDomain."""
        processed_terms = []
        for term in domain.glossary_terms:
            processed_terms.append(
                ProcessedGlossaryTerm(
                    urn=term.urn,
                    name=term.name,
                    definition=term.definition,
                    source=term.source,
                    custom_properties=term.custom_properties or {},
                    path_segments=tuple(term.path_segments)
                    if term.path_segments
                    else (),
                    relationships=term.relationships or {},
                )
            )

        processed_subdomains = []
        for subdomain in domain.subdomains:
            processed_subdomains.append(self._convert_domain(subdomain))

        return ProcessedDomain(
            urn=str(domain.urn),
            name=domain.name,
            path_segments=tuple(domain.path_segments) if domain.path_segments else (),
            parent_domain_urn=str(domain.parent_domain_urn)
            if domain.parent_domain_urn
            else None,
            glossary_terms=processed_terms,
            subdomains=processed_subdomains,
        )

    def _map_native_type_to_generic(self, native_type: str) -> str:
        """Map native database type back to generic field type."""
        if not native_type:
            return "string"
        native_type_upper = native_type.upper()
        if native_type_upper in ("VARCHAR", "CHAR", "TEXT", "STRING"):
            return "string"
        elif native_type_upper in (
            "NUMERIC",
            "INTEGER",
            "INT",
            "BIGINT",
            "DECIMAL",
            "FLOAT",
            "DOUBLE",
            "NUMBER",
        ):
            return "number"
        elif native_type_upper == "BOOLEAN":
            return "boolean"
        elif native_type_upper == "DATE":
            return "date"
        elif native_type_upper in ("TIMESTAMP", "DATETIME"):
            return "datetime"
        elif native_type_upper == "TIME":
            return "time"
        return "string"

    def _build_domains_from_terms(
        self, terms: List[ProcessedGlossaryTerm]
    ) -> List[ProcessedDomain]:
        """Build domain hierarchy from terms."""
        # Group entities by path
        domains_map: Dict[Tuple[str, ...], ProcessedDomain] = {}

        for term in terms:
            if term.path_segments:
                # Convert path_segments to tuple for use as dict key
                path_segments_tuple = (
                    tuple(term.path_segments)
                    if isinstance(term.path_segments, list)
                    else term.path_segments
                )

                # Build all parent paths
                for i in range(1, len(path_segments_tuple)):
                    path = path_segments_tuple[:i]
                    if path not in domains_map:
                        domains_map[path] = ProcessedDomain(
                            urn=f"urn:li:domain:{'/'.join(path)}",
                            name=path[-1],
                            path_segments=path,
                            parent_domain_urn=f"urn:li:domain:{'/'.join(path[:-1])}"
                            if len(path) > 1
                            else None,
                            glossary_terms=[],
                        )

                # Add term to its domain
                term_path = path_segments_tuple[:-1]  # Exclude term name
                if term_path and term_path in domains_map:
                    domains_map[term_path].glossary_terms.append(term)

        return list(domains_map.values())

    def get_datahub_graph(
        self,
        graph: Graph,
        environment: str = "PROD",
        export_only: List[str] | None = None,
        skip_export: List[str] | None = None,
        create_assertions: bool = False,
        assertion_types: Dict[str, bool] | None = None,
    ):
        """
        Get the DataHub AST (DataHubGraph) from an RDF graph.

        Args:
            graph: RDFLib Graph containing the RDF data
            environment: DataHub environment
            export_only: Optional list of entity types to export
            skip_export: Optional list of entity types to skip
            create_assertions: If True, enables assertion creation (default: False)
            assertion_types: Dict with sub-flags for assertion types:
                - required_fields: bool (for minCount/maxCount → NOT_NULL)
                - field_size: bool (for minLength/maxLength)
                - value_checks: bool (for minInclusive/maxInclusive, pattern)

        Returns:
            DataHubGraph: The DataHub AST representation
        """
        from datahub.ingestion.source.rdf.core.ast import DataHubGraph
        from datahub.ingestion.source.rdf.entities.domain.builder import DomainBuilder
        from datahub.ingestion.source.rdf.entities.registry import (
            create_default_registry,
        )
        from datahub.ingestion.source.rdf.entities.relationship.ast import (
            DataHubRelationship,
        )

        registry = create_default_registry()

        context = {
            "environment": environment,
            "export_only": export_only,
            "skip_export": skip_export,
            "create_assertions": create_assertions,
            "assertion_types": assertion_types or {},
        }

        # Helper to check if a CLI name should be processed
        def should_process_cli_name(cli_name: str) -> bool:
            """Check if a CLI name (e.g., 'glossary', 'datasets') should be processed."""
            if export_only and cli_name not in export_only:
                return False
            if skip_export and cli_name in skip_export:
                return False
            return True

        # Helper to get entity type from CLI name
        def get_entity_type(cli_name: str) -> Optional[str]:
            """Get entity type from CLI name using registry."""
            return registry.get_entity_type_from_cli_name(cli_name)

        # Create DataHubGraph
        datahub_graph = DataHubGraph()

        # Extract and convert glossary terms
        if should_process_cli_name("glossary"):
            entity_type = get_entity_type("glossary") or "glossary_term"
            extractor = registry.get_extractor(entity_type)
            converter = registry.get_converter(entity_type)

            if extractor and converter:
                rdf_terms = extractor.extract_all(graph, context)
                datahub_terms = converter.convert_all(rdf_terms, context)
                datahub_graph.glossary_terms = datahub_terms
            else:
                logger.warning(f"Extractor or converter not found for {entity_type}")

            # Collect relationships
            from datahub.ingestion.source.rdf.entities.glossary_term.converter import (
                GlossaryTermConverter,
            )

            if isinstance(converter, GlossaryTermConverter):
                relationships = converter.collect_relationships(rdf_terms, context)
                for rel in relationships:
                    datahub_graph.relationships.append(
                        DataHubRelationship(
                            source_urn=rel.source_urn,
                            target_urn=rel.target_urn,
                            relationship_type=rel.relationship_type,
                            properties=rel.properties or {},
                        )
                    )

        # Build domains (DomainBuilder creates its own URN generator)
        domain_builder = DomainBuilder()
        datahub_graph.domains = domain_builder.build_domains(
            datahub_graph.glossary_terms, context
        )

        return datahub_graph

    def generate_mcps(
        self,
        graph: Graph,
        environment: str = "PROD",
        export_only: List[str] = None,
        skip_export: List[str] = None,
    ) -> List[Any]:
        """
        Generate DataHub MCPs from an RDF graph.

        Args:
            graph: RDFLib Graph containing the RDF data
            environment: DataHub environment
            export_only: Optional list of entity types to export
            skip_export: Optional list of entity types to skip

        Returns:
            List of MetadataChangeProposalWrapper objects
        """
        return self._generate_mcps_modular(graph, environment, export_only, skip_export)

    def _generate_mcps_modular(
        self,
        graph: Graph,
        environment: str,
        export_only: List[str] = None,
        skip_export: List[str] = None,
    ) -> List[Any]:
        """Generate MCPs using modular entity-based implementation."""
        from datahub.ingestion.source.rdf.entities.pipeline import EntityPipeline
        from datahub.ingestion.source.rdf.entities.registry import (
            create_default_registry,
        )

        pipeline = EntityPipeline()
        registry = create_default_registry()
        context = {
            "environment": environment,
            "export_only": export_only,
            "skip_export": skip_export,
        }

        mcps = []

        # Helper to check if a CLI name should be processed
        def should_process_cli_name(cli_name: str) -> bool:
            """Check if a CLI name (e.g., 'glossary', 'datasets') should be processed."""
            if export_only and cli_name not in export_only:
                return False
            if skip_export and cli_name in skip_export:
                return False
            return True

        # Process all registered entity types
        for entity_type in registry.list_entity_types():
            # Get CLI names for this entity type
            metadata = registry.get_metadata(entity_type)
            if not metadata:
                # Fallback: try to process if no metadata
                if should_process_cli_name(entity_type):
                    mcps.extend(
                        pipeline.process_entity_type(graph, entity_type, context)
                    )
                continue

            # Check if any CLI name for this entity should be processed
            should_process = any(
                should_process_cli_name(cli_name) for cli_name in metadata.cli_names
            )
            if should_process:
                mcps.extend(pipeline.process_entity_type(graph, entity_type, context))

        # Process relationships (special case - not a regular entity type)
        rel_mcps = pipeline.build_relationship_mcps(graph, context)
        mcps.extend(rel_mcps)

        return mcps
