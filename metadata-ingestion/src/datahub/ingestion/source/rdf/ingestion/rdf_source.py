#!/usr/bin/env python3
"""
DataHub Ingestion Source for RDF.

This module provides a DataHub ingestion source that allows RDF to be used
as a native DataHub ingestion plugin in DataHub recipes.

Example recipe:
    source:
      type: rdf
      config:
        source: examples/bcbs239/
        environment: PROD
        export_only:
          - glossary
          - datasets
          - lineage
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Optional

from rdflib import Graph

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.rdf.core.rdf_loader import load_rdf_graph
from datahub.ingestion.source.rdf.ingestion.ast_converter import RDFToASTConverter
from datahub.ingestion.source.rdf.ingestion.workunit_generator import WorkUnitGenerator
from datahub.ingestion.source.rdf.rdf_config import RDFSourceConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)

logger = logging.getLogger(__name__)


@dataclass
class RDFSourceReport(StaleEntityRemovalSourceReport):
    """
    Report for RDF ingestion source.

    Tracks statistics and errors during ingestion.
    """

    num_files_processed: int = 0
    num_triples_processed: int = 0
    num_entities_emitted: int = 0
    num_workunits_produced: int = 0

    # Breakdown by entity type
    num_glossary_terms: int = 0
    num_glossary_nodes: int = 0
    num_relationships: int = 0

    # File-level tracking
    files_processed: List[str] = field(default_factory=list)

    def report_triples_processed(self, count: int) -> None:
        """Add to triples counter."""
        self.num_triples_processed += count

    def report_entity_emitted(self):
        """Increment entity counter."""
        self.num_entities_emitted += 1

    def report_workunit_produced(self):
        """Increment workunit counter."""
        self.num_workunits_produced += 1

    def report_glossary_term(self):
        """Increment glossary term counter."""
        self.num_glossary_terms += 1

    def report_glossary_node(self):
        """Increment glossary node counter."""
        self.num_glossary_nodes += 1

    def report_relationship(self):
        """Increment relationship counter."""
        self.num_relationships += 1

    def report_file_processed(self, file_path: str) -> None:
        """Record a processed file."""
        self.num_files_processed += 1
        self.files_processed.append(file_path)


@platform_name("RDF")
@config_class(RDFSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled via stateful_ingestion.enabled: true",
    supported=True,
)
@capability(
    SourceCapability.PLATFORM_INSTANCE,
    "Supported via platform_instance config",
    supported=True,
)
@capability(
    SourceCapability.DESCRIPTIONS,
    "Enabled by default (from skos:definition or rdfs:comment)",
    supported=True,
)
@capability(
    SourceCapability.DOMAINS,
    "Not applicable (domains used internally for hierarchy)",
    supported=False,
)
@capability(
    SourceCapability.DATA_PROFILING,
    "Not applicable",
    supported=False,
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Not in MVP",
    supported=False,
)
@capability(
    SourceCapability.OWNERSHIP,
    "Not supported",
    supported=False,
)
@capability(
    SourceCapability.TAGS,
    "Not supported",
    supported=False,
)
class RDFSource(StatefulIngestionSourceBase, TestableSource):
    """
    DataHub ingestion source for RDF ontologies.

    This source processes RDF/OWL ontologies (Turtle, RDF/XML, etc.) and
    converts them to DataHub entities using the RDF transpiler.

    Supports:
    - Glossary terms and nodes (SKOS, OWL)
    - Datasets with schemas (VOID, DCAT)
    - Data lineage (PROV-O)
    - Structured properties
    - Domain hierarchy
    - Stateful ingestion (stale entity removal)
    """

    config: RDFSourceConfig
    report: RDFSourceReport

    def __init__(self, config: RDFSourceConfig, ctx: PipelineContext):
        """
        Initialize the RDF source.

        Args:
            config: Source configuration
            ctx: Pipeline context from DataHub
        """
        super().__init__(config, ctx)
        self.config = config
        self.report = RDFSourceReport()
        self.platform = "rdf"
        self.ast_converter = RDFToASTConverter(config, self.report)
        self.workunit_generator = WorkUnitGenerator(self.report)

        logger.info("Initializing RDF source")
        logger.debug(f"RDF source config: {config}")

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "RDFSource":
        """
        Create an instance of the source.

        Args:
            config_dict: Configuration dictionary
            ctx: Pipeline context

        Returns:
            Initialized RDFSource instance
        """
        config = RDFSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def _test_source_accessibility(config: RDFSourceConfig) -> CapabilityReport:
        """
        Test if the source is accessible.

        Args:
            config: RDF source configuration

        Returns:
            CapabilityReport indicating if source is accessible
        """
        from pathlib import Path

        try:
            source_path = config.source

            # Check if it's a URL
            if source_path.startswith(("http://", "https://")):
                # URL accessibility will be tested in parsing step
                return CapabilityReport(capable=True, failure_reason=None)

            # Check if file/directory exists
            path = Path(source_path)
            if not path.exists():
                return CapabilityReport(
                    capable=False,
                    failure_reason=f"Source not found: {source_path}",
                )

            if path.is_dir():
                # Check if directory has RDF files
                rdf_files = list(
                    path.rglob("*") if config.recursive else path.glob("*")
                )
                rdf_files = [
                    f
                    for f in rdf_files
                    if f.is_file()
                    and f.suffix.lower() in [ext.lower() for ext in config.extensions]
                ]
                if not rdf_files:
                    return CapabilityReport(
                        capable=False,
                        failure_reason=(
                            f"No RDF files found in directory {source_path}. "
                            f"Supported extensions: {', '.join(config.extensions)}"
                        ),
                    )
                return CapabilityReport(capable=True, failure_reason=None)
            else:
                # Single file - check extension
                if path.suffix.lower() not in [
                    ext.lower() for ext in config.extensions
                ]:
                    return CapabilityReport(
                        capable=False,
                        failure_reason=(
                            f"File extension '{path.suffix}' not in supported extensions: "
                            f"{', '.join(config.extensions)}"
                        ),
                    )
                return CapabilityReport(capable=True, failure_reason=None)
        except (OSError, ValueError) as e:
            return CapabilityReport(
                capable=False,
                failure_reason=f"Error checking source accessibility: {e}",
            )

    @staticmethod
    def _test_rdf_parsing(config: RDFSourceConfig) -> CapabilityReport:
        """
        Test if the source can be parsed as RDF.

        Args:
            config: RDF source configuration

        Returns:
            CapabilityReport indicating if RDF parsing succeeded
        """
        try:
            from datahub.ingestion.source.rdf.core.rdf_loader import load_rdf_graph

            test_graph = load_rdf_graph(
                source=config.source,
                format=config.format,
                recursive=False,  # Don't recurse during test
                file_extensions=config.extensions,
            )
            triple_count = len(test_graph)

            return CapabilityReport(
                capable=True,
                failure_reason=None,
                metadata={"triples_loaded": triple_count},
            )
        except FileNotFoundError as e:
            return CapabilityReport(
                capable=False,
                failure_reason=f"File not found: {e}",
            )
        except ValueError as e:
            return CapabilityReport(
                capable=False,
                failure_reason=f"Invalid RDF format or source: {e}",
            )
        except (OSError, RuntimeError) as e:
            return CapabilityReport(
                capable=False,
                failure_reason=f"Failed to parse RDF: {e}",
            )

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """
        Test connection to RDF source.

        Validates that the source is accessible and can be parsed as RDF.

        Args:
            config_dict: Configuration dictionary

        Returns:
            TestConnectionReport with connection test results
        """
        config = RDFSourceConfig.model_validate(config_dict)
        report = TestConnectionReport()

        # Test 1: Source accessibility
        basic_connectivity = RDFSource._test_source_accessibility(config)
        report.basic_connectivity = basic_connectivity

        # Test 2: RDF parsing (only if source is accessible)
        if basic_connectivity.capable:
            rdf_parsing = RDFSource._test_rdf_parsing(config)
        else:
            rdf_parsing = CapabilityReport(
                capable=False,
                failure_reason="Skipped due to source accessibility failure",
            )

        # Add RDF parsing as a capability
        from datahub.ingestion.api.source import SourceCapability

        report.capability_report = {
            SourceCapability.SCHEMA_METADATA: rdf_parsing,
        }

        return report

    def get_workunit_processors(self) -> List[Any]:
        """
        Get work unit processors for stateful ingestion.

        Returns:
            List of work unit processors including stale entity removal handler
        """
        return [
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Generate work units from RDF data.

        This is the main method that DataHub calls to get metadata.

        Yields:
            MetadataWorkUnit objects containing MCPs
        """
        logger.info("Starting RDF ingestion")

        rdf_graph = self._load_rdf_graph()
        if rdf_graph is None:
            return

        datahub_ast = self.ast_converter.convert_safe(rdf_graph)
        if datahub_ast is None:
            return

        workunits = self.workunit_generator.generate_safe(datahub_ast)
        if workunits is None:
            return

        yield from self.workunit_generator.yield_with_error_handling(workunits)

    def _handle_load_error(
        self, error: Exception, error_type: str, error_context: str
    ) -> None:
        """
        Handle errors during RDF graph loading.

        Args:
            error: The exception that occurred
            error_type: Type of error for reporting
            error_context: Contextual error message
        """
        self.report.report_failure(error_type, context=error_context, exc=error)
        logger.error(
            f"{error_type}: {self.config.source}. {error_context}",
            exc_info=True,
        )

    def _apply_sparql_filter_with_logging(
        self, rdf_graph: Graph, triple_count: int
    ) -> Optional[Graph]:
        """
        Apply SPARQL filter and log the results.

        Args:
            rdf_graph: Original RDF graph
            triple_count: Original triple count before filtering

        Returns:
            Filtered RDF graph, or None if filtering failed
        """
        filtered_graph = self._apply_sparql_filter(rdf_graph)
        if filtered_graph is None:
            return None

        filtered_count = len(filtered_graph)
        if triple_count > 0:
            percentage = filtered_count / triple_count * 100
            logger.info(
                f"SPARQL filter applied: {triple_count} → {filtered_count} triples "
                f"({percentage:.1f}% retained)"
            )
        else:
            logger.info(
                f"SPARQL filter applied: {triple_count} → {filtered_count} triples"
            )
        self.report.report_triples_processed(filtered_count)
        return filtered_graph

    def _load_rdf_graph(self) -> Optional[Graph]:
        """
        Load RDF graph from configured source.

        Returns:
            Loaded RDF graph, or None if loading failed
        """
        try:
            logger.info("Loading RDF graph from source")
            rdf_graph = load_rdf_graph(
                source=self.config.source,
                format=self.config.format,
                recursive=self.config.recursive,
                file_extensions=self.config.extensions,
            )
            triple_count = len(rdf_graph)
            logger.info(f"Loaded {triple_count} triples from source")

            # Apply SPARQL filter if provided
            if self.config.sparql_filter:
                filtered_graph = self._apply_sparql_filter_with_logging(
                    rdf_graph, triple_count
                )
                if filtered_graph is None:
                    return None
                # Type narrowing: filtered_graph is guaranteed to be Graph after None check
                rdf_graph = filtered_graph
            else:
                # Report original triple count when no filter
                self.report.report_triples_processed(triple_count)

            if len(rdf_graph) > 10000:
                logger.warning(
                    f"Large RDF graph detected ({len(rdf_graph)} triples). "
                    "Processing may take some time. Consider splitting large files "
                    "or using more specific export filters to improve performance."
                )
            return rdf_graph
        except FileNotFoundError as e:
            error_context = (
                f"Source: {self.config.source}. "
                f"Please verify the file or directory exists and is accessible. "
                f"If using a URL, ensure it's reachable. "
                f"Supported file extensions: {', '.join(self.config.extensions)}"
            )
            self._handle_load_error(e, "RDF source file not found", error_context)
            return None
        except ValueError as e:
            error_context = (
                f"Source: {self.config.source}, Error: {str(e)}. "
                f"Please verify the file is valid RDF in a supported format "
                f"(turtle, xml, json-ld, n3, nt). "
                f"If format is not auto-detected, specify it explicitly using the 'format' config option."
            )
            self._handle_load_error(e, "Invalid RDF source", error_context)
            return None
        except (OSError, RuntimeError) as e:
            error_context = (
                f"Source: {self.config.source}. "
                f"Unexpected error while loading RDF. "
                f"Please verify the file is accessible, properly formatted, and in a supported RDF format. "
                f"Check the logs for more details."
            )
            self._handle_load_error(e, "Failed to load RDF graph", error_context)
            return None

    def _validate_query_type(self, query_algebra_name: str) -> None:
        """
        Validate that the SPARQL query type is supported.

        Args:
            query_algebra_name: Name of the query algebra

        Raises:
            ValueError: If query type is not supported
        """
        if query_algebra_name == "SelectQuery":
            raise ValueError(
                "SELECT queries are not supported for SPARQL filtering. "
                "Please use a CONSTRUCT query instead. "
                "Example: 'CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o . FILTER(...) }'"
            )
        if query_algebra_name not in ("ConstructQuery",):
            raise ValueError(
                f"Unsupported SPARQL query type: {query_algebra_name}. "
                "Only CONSTRUCT queries are supported for filtering."
            )

    def _build_graph_from_query_result(self, query_result: Any) -> Graph:
        """
        Build a Graph from SPARQL query result.

        Handles both Graph objects and iterables of triples.

        Args:
            query_result: Result from SPARQL query execution

        Returns:
            Graph containing the query results
        """
        if isinstance(query_result, Graph):
            return query_result

        # Fallback: if it's not a Graph, try to iterate and build one
        filtered_graph = Graph()
        for item in query_result:
            # Type check: ensure we have a valid triple (tuple of 3 Nodes)
            if isinstance(item, tuple) and len(item) == 3:
                filtered_graph.add(item)  # type: ignore[arg-type]
            else:
                # Skip invalid items (shouldn't happen for CONSTRUCT queries)
                logger.warning(f"Skipping invalid query result item: {type(item)}")
        return filtered_graph

    def _apply_sparql_filter(self, graph: Graph) -> Optional[Graph]:
        """
        Apply SPARQL CONSTRUCT query to filter RDF graph.

        Args:
            graph: Original RDF graph to filter

        Returns:
            Filtered RDF graph, or None if filtering failed
        """
        # Ensure sparql_filter is not None (should be checked by caller)
        sparql_filter_str = self.config.sparql_filter
        if sparql_filter_str is None:
            return None

        # Type narrowing: after None check, sparql_filter_str is guaranteed to be str
        # Assign to explicitly typed variable for mypy
        filter_query: str = sparql_filter_str

        try:
            from rdflib.plugins.sparql import prepareQuery

            logger.info("Applying SPARQL filter to RDF graph")
            query = prepareQuery(filter_query)

            # Execute query - for CONSTRUCT queries, RDFLib returns a Graph
            # For SELECT queries, it returns a Result object (which we don't support)
            query_result = graph.query(query)

            # Validate query type
            self._validate_query_type(query.algebra.name)

            # Build filtered graph from query result
            filtered_graph = self._build_graph_from_query_result(query_result)
            return filtered_graph

        except Exception as e:
            query_preview = (
                filter_query[:100] if len(filter_query) > 100 else filter_query
            )
            error_context = (
                f"SPARQL query: {query_preview}... "
                f"Error: {str(e)}. "
                "Please verify your SPARQL query syntax and ensure it's a valid CONSTRUCT query."
            )
            self.report.report_failure(
                "Failed to apply SPARQL filter",
                context=error_context,
                exc=e,
            )
            logger.error(
                f"Failed to apply SPARQL filter: {e}. {error_context}",
                exc_info=True,
            )
            return None

    def get_report(self) -> RDFSourceReport:
        """
        Get the ingestion report.

        Returns:
            Report with statistics and errors
        """
        return self.report

    def close(self) -> None:
        """Clean up resources."""
        logger.info("Closing RDF source")
        super().close()
