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
from typing import Any, Dict, Iterable, List, Optional

from pydantic import Field, field_validator

from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
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
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.rdf.core.ast import DataHubGraph
from datahub.ingestion.source.rdf.core.rdf_loader import load_rdf_graph
from datahub.ingestion.source.rdf.dialects import RDFDialect
from datahub.ingestion.source.rdf.entities.domain.builder import DomainBuilder
from datahub.ingestion.source.rdf.entities.registry import create_default_registry
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)

logger = logging.getLogger(__name__)


class RDFSourceConfig(
    StatefulIngestionConfigBase, EnvConfigMixin, PlatformInstanceConfigMixin
):
    """
    Configuration for RDF ingestion source.

    Mirrors the CLI parameters to provide consistent behavior between
    CLI and ingestion framework usage.

    Example configuration:
        ```yaml
        source:
          type: rdf
          config:
            source: /path/to/glossary.ttl
            format: turtle
            environment: PROD
            stateful_ingestion:
              enabled: true
              remove_stale_metadata: true
        ```

    Example with directory:
        ```yaml
        source:
          type: rdf
          config:
            source: ./rdf_data/
            format: turtle
            recursive: true
            environment: PROD
        ```

    Example with filtering:
        ```yaml
        source:
          type: rdf
          config:
            source: /path/to/ontology.owl
            export_only: ["glossary"]
            environment: PROD
        ```
    """

    # Source Options
    source: str = Field(
        description=(
            "Source to process: file path, folder path, server URL, or comma-separated files. "
            "Examples: '/path/to/file.ttl', './rdf_data/', 'http://example.org/ontology.owl', '/path/to/file1.ttl,/path/to/file2.ttl'"
        )
    )
    format: Optional[str] = Field(
        default=None,
        description="RDF format (auto-detected if not specified). Examples: turtle, xml, n3, nt",
    )
    extensions: List[str] = Field(
        default=[".ttl", ".rdf", ".owl", ".n3", ".nt"],
        description="File extensions to process when source is a folder",
    )
    recursive: bool = Field(
        default=True, description="Enable recursive folder processing (default: true)"
    )

    # DataHub Options
    environment: str = Field(
        default="PROD", description="DataHub environment (PROD, DEV, TEST, etc.)"
    )

    # RDF Dialect Options
    dialect: Optional[str] = Field(
        default=None,
        description="Force a specific RDF dialect (default: auto-detect). Options: default, fibo, generic",
    )

    # Selective Export Options
    export_only: Optional[List[str]] = Field(
        default=None,
        description="Export only specified entity types. Options are dynamically determined from registered entity types.",
    )
    skip_export: Optional[List[str]] = Field(
        default=None,
        description="Skip exporting specified entity types. Options are dynamically determined from registered entity types.",
    )

    # Stateful Ingestion Options
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion configuration. See https://datahubproject.io/docs/stateful-ingestion for more details.",
    )

    @field_validator("dialect")
    @classmethod
    def validate_dialect(cls, v):
        """Validate dialect is a valid RDFDialect value."""
        if v is not None:
            try:
                RDFDialect(v)
            except ValueError as e:
                valid_dialects = [d.value for d in RDFDialect]
                raise ValueError(
                    f"Invalid dialect '{v}'. Must be one of: {valid_dialects}"
                ) from e
        return v

    @field_validator("export_only", "skip_export")
    @classmethod
    def validate_export_options(cls, v):
        """Validate export options are valid entity types."""
        if v is not None:
            # Get valid CLI choices from registry
            from datahub.ingestion.source.rdf.entities.registry import (
                create_default_registry,
            )

            registry = create_default_registry()
            valid_types = registry.get_all_cli_choices()

            for entity_type in v:
                if entity_type not in valid_types:
                    raise ValueError(
                        f"Invalid entity type '{entity_type}'. Must be one of: {sorted(valid_types)}"
                    )
        return v


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

    def report_triples_processed(self, count: int):
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

    def report_file_processed(self, file_path: str):
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
class RDFSource(StatefulIngestionSourceBase):
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
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """
        Test connection to RDF source.

        Validates that the source is accessible and can be parsed as RDF.

        Args:
            config_dict: Configuration dictionary

        Returns:
            TestConnectionReport with connection test results
        """
        from pathlib import Path

        config = RDFSourceConfig.model_validate(config_dict)
        report = TestConnectionReport()

        # Test 1: Source accessibility
        try:
            source_path = config.source

            # Check if it's a URL
            if source_path.startswith(("http://", "https://")):
                # URL accessibility will be tested in parsing step
                basic_connectivity = CapabilityReport(
                    capable=True,
                    failure_reason=None,
                )
            else:
                # Check if file/directory exists
                path = Path(source_path)
                if not path.exists():
                    basic_connectivity = CapabilityReport(
                        capable=False,
                        failure_reason=f"Source not found: {source_path}",
                    )
                elif path.is_dir():
                    # Check if directory has RDF files
                    rdf_files = list(
                        path.rglob("*") if config.recursive else path.glob("*")
                    )
                    rdf_files = [
                        f
                        for f in rdf_files
                        if f.is_file()
                        and f.suffix.lower()
                        in [ext.lower() for ext in config.extensions]
                    ]
                    if not rdf_files:
                        basic_connectivity = CapabilityReport(
                            capable=False,
                            failure_reason=(
                                f"No RDF files found in directory {source_path}. "
                                f"Supported extensions: {', '.join(config.extensions)}"
                            ),
                        )
                    else:
                        basic_connectivity = CapabilityReport(
                            capable=True,
                            failure_reason=None,
                        )
                else:
                    # Single file - check extension
                    if path.suffix.lower() not in [
                        ext.lower() for ext in config.extensions
                    ]:
                        basic_connectivity = CapabilityReport(
                            capable=False,
                            failure_reason=(
                                f"File extension '{path.suffix}' not in supported extensions: "
                                f"{', '.join(config.extensions)}"
                            ),
                        )
                    else:
                        basic_connectivity = CapabilityReport(
                            capable=True,
                            failure_reason=None,
                        )
        except Exception as e:
            basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=f"Error checking source accessibility: {e}",
            )

        report.basic_connectivity = basic_connectivity

        # Test 2: RDF parsing (only if source is accessible)
        if basic_connectivity.capable:
            try:
                # Try to parse a small sample
                from datahub.ingestion.source.rdf.core.rdf_loader import (
                    load_rdf_graph,
                )

                # For URLs, we'll do a limited test
                # For files, parse a small portion
                test_graph = load_rdf_graph(
                    source=config.source,
                    format=config.format,
                    recursive=False,  # Don't recurse during test
                    file_extensions=config.extensions,
                )
                triple_count = len(test_graph)

                rdf_parsing = CapabilityReport(
                    capable=True,
                    failure_reason=None,
                    metadata={"triples_loaded": triple_count},
                )
            except FileNotFoundError as e:
                rdf_parsing = CapabilityReport(
                    capable=False,
                    failure_reason=f"File not found: {e}",
                )
            except ValueError as e:
                rdf_parsing = CapabilityReport(
                    capable=False,
                    failure_reason=f"Invalid RDF format or source: {e}",
                )
            except Exception as e:
                rdf_parsing = CapabilityReport(
                    capable=False,
                    failure_reason=f"Failed to parse RDF: {e}",
                )
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

        # Load RDF graph directly
        try:
            logger.info("Loading RDF graph from source")
            rdf_graph = load_rdf_graph(
                source=self.config.source,
                format=self.config.format,
                recursive=self.config.recursive,
                file_extensions=self.config.extensions,
            )
            triple_count = len(rdf_graph)
            self.report.report_triples_processed(triple_count)
            logger.info(f"Loaded {triple_count} triples from source")

            # Performance warning for large files
            if triple_count > 10000:
                logger.warning(
                    f"Large RDF graph detected ({triple_count} triples). "
                    "Processing may take some time. Consider splitting large files "
                    "or using more specific export filters to improve performance."
                )
        except FileNotFoundError as e:
            # Provide actionable error message
            error_context = (
                f"Source: {self.config.source}. "
                f"Please verify the file or directory exists and is accessible. "
                f"If using a URL, ensure it's reachable. "
                f"Supported file extensions: {', '.join(self.config.extensions)}"
            )
            self.report.report_failure(
                "RDF source file not found",
                context=error_context,
                exc=e,
            )
            logger.error(
                f"RDF source file not found: {self.config.source}. {error_context}",
                exc_info=True,
            )
            return  # Early return, don't continue
        except ValueError as e:
            # Provide actionable error message
            error_context = (
                f"Source: {self.config.source}, Error: {str(e)}. "
                f"Please verify the file is valid RDF in a supported format "
                f"(turtle, xml, json-ld, n3, nt). "
                f"If format is not auto-detected, specify it explicitly using the 'format' config option."
            )
            self.report.report_failure(
                "Invalid RDF source",
                context=error_context,
                exc=e,
            )
            logger.error(
                f"Invalid RDF source: {self.config.source}. {error_context}",
                exc_info=True,
            )
            return
        except Exception as e:
            # Provide actionable error message
            error_context = (
                f"Source: {self.config.source}. "
                f"Unexpected error while loading RDF. "
                f"Please verify the file is accessible, properly formatted, and in a supported RDF format. "
                f"Check the logs for more details."
            )
            self.report.report_failure(
                "Failed to load RDF graph",
                context=error_context,
                exc=e,
            )
            logger.error(
                f"Failed to load RDF graph from {self.config.source}. {error_context}",
                exc_info=True,
            )
            return

            # Convert RDF to DataHub AST
        summary_str = "unknown"
        try:
            logger.info("Converting RDF to DataHub AST")
            datahub_ast = self._convert_rdf_to_datahub_ast(
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
        except Exception as e:
            self.report.report_failure(
                "Failed to convert RDF to DataHub AST",
                context=f"Triples processed: {len(rdf_graph)}",
                exc=e,
            )
            logger.error(
                f"Failed to convert RDF to DataHub AST: {e}",
                exc_info=True,
            )
            return

        # Generate work units from DataHub AST
        try:
            logger.info("Generating work units from DataHub AST")
            workunits = self._generate_workunits_from_ast(datahub_ast)
            # Note: workunits is a generator, so we can't get its length without consuming it
        except Exception as e:
            self.report.report_failure(
                "Failed to generate work units",
                context=f"AST summary: {summary_str}",
                exc=e,
            )
            logger.error(
                f"Failed to generate work units from DataHub AST: {e}",
                exc_info=True,
            )
            return

        # Yield all work units with individual error handling
        for workunit in workunits:
            try:
                yield workunit
            except Exception as e:
                # Continue processing other work units even if one fails
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

    def _convert_rdf_to_datahub_ast(
        self,
        graph,
        environment: str,
        export_only: Optional[List[str]] = None,
        skip_export: Optional[List[str]] = None,
    ) -> DataHubGraph:
        """
        Convert RDF graph to DataHub AST.

        Inlined from facade to eliminate unnecessary abstraction layer.
        """
        registry = create_default_registry()

        context = {
            "environment": environment,
            "export_only": export_only,
            "skip_export": skip_export,
        }

        # Helper to check if a CLI name should be processed
        def should_process_cli_name(cli_name: str) -> bool:
            if export_only and cli_name not in export_only:
                return False
            if skip_export and cli_name in skip_export:
                return False
            return True

        # Helper to get entity type from CLI name
        def get_entity_type(cli_name: str) -> Optional[str]:
            return registry.get_entity_type_from_cli_name(cli_name)

        # Create DataHubGraph
        datahub_graph = DataHubGraph()

        # Extract glossary terms (now returns DataHub AST directly)
        if should_process_cli_name("glossary"):
            entity_type = get_entity_type("glossary") or "glossary_term"
            extractor = registry.get_extractor(entity_type)

            if extractor:
                try:
                    datahub_terms = extractor.extract_all(graph, context)
                    datahub_graph.glossary_terms = datahub_terms
                    logger.debug(
                        f"Extracted {len(datahub_terms)} glossary terms from RDF graph"
                    )
                except Exception as e:
                    self.report.report_failure(
                        "Failed to extract glossary terms",
                        context=f"Entity type: {entity_type}",
                        exc=e,
                    )
                    logger.error(
                        f"Failed to extract glossary terms: {e}",
                        exc_info=True,
                    )
                    # Continue with empty list - partial success
                    datahub_graph.glossary_terms = []

        # Extract relationships independently (using RelationshipExtractor)
        if should_process_cli_name("relationship"):
            entity_type = get_entity_type("relationship") or "relationship"
            extractor = registry.get_extractor(entity_type)

            if extractor:
                try:
                    # Extract RDF relationships
                    rdf_relationships = extractor.extract_all(graph, context)

                    # Convert RDF relationships to DataHub relationships
                    converter = registry.get_converter(entity_type)
                    if converter:
                        datahub_relationships = []
                        for rdf_rel in rdf_relationships:
                            try:
                                datahub_rel = converter.convert(rdf_rel, context)
                                if datahub_rel:
                                    datahub_relationships.append(datahub_rel)
                            except Exception as e:
                                # Continue processing other relationships
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
                                # Continue to next relationship
                        datahub_graph.relationships.extend(datahub_relationships)
                        logger.debug(
                            f"Extracted {len(datahub_relationships)} relationships from RDF graph"
                        )
                except Exception as e:
                    self.report.report_failure(
                        "Failed to extract relationships",
                        context=f"Entity type: {entity_type}",
                        exc=e,
                    )
                    logger.error(
                        f"Failed to extract relationships: {e}",
                        exc_info=True,
                    )
                    # Continue with empty list - partial success
                    datahub_graph.relationships = []

        # Build domains
        try:
            domain_builder = DomainBuilder()
            datahub_graph.domains = domain_builder.build_domains(
                datahub_graph.glossary_terms, context
            )
            logger.debug(
                f"Built {len(datahub_graph.domains)} domains from glossary term hierarchy"
            )
        except Exception as e:
            self.report.report_failure(
                "Failed to build domain hierarchy",
                context=f"Glossary terms: {len(datahub_graph.glossary_terms)}",
                exc=e,
            )
            logger.error(
                f"Failed to build domain hierarchy: {e}",
                exc_info=True,
            )
            # Continue with empty list - partial success
            datahub_graph.domains = []

        return datahub_graph

    def _generate_workunits_from_ast(
        self, datahub_graph: DataHubGraph
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate work units from DataHub AST.

        Inlined from DataHubIngestionTarget to eliminate unnecessary abstraction.
        Uses generator pattern for memory efficiency.

        Args:
            datahub_graph: DataHubGraph AST containing entities to emit

        Yields:
            MetadataWorkUnit objects as they're generated
        """
        from datahub.ingestion.source.rdf.core.ast import DataHubGraph

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
                if hasattr(self.report, "report_workunit_produced"):
                    self.report.report_workunit_produced()
                yield workunit

            # Process deferred entities and yield work units incrementally
            for mcp in self._process_deferred_entities(
                datahub_graph, registry, build_context
            ):
                mcp_count += 1
                workunit = MetadataWorkUnit(id=f"rdf-{workunit_id}", mcp=mcp)
                workunit_id += 1
                if hasattr(self.report, "report_workunit_produced"):
                    self.report.report_workunit_produced()
                yield workunit

            # Log summary (simplified since we don't have full MCP list)
            logger.info(f"Generated {mcp_count} MCPs total:")
            logger.info(f"  - Glossary terms: {len(datahub_graph.glossary_terms)}")
            logger.info(f"  - Relationships: {len(datahub_graph.relationships)}")
            logger.debug(
                f"  - Domains (data structure only, not ingested): {len(datahub_graph.domains)}"
            )

        except Exception as e:
            self.report.report_failure(
                "Failed to generate work units from DataHub AST",
                context=f"Glossary terms: {len(datahub_graph.glossary_terms)}, "
                f"Domains: {len(datahub_graph.domains)}, "
                f"Relationships: {len(datahub_graph.relationships)}",
                exc=e,
            )
            logger.error(f"Failed to generate work units: {e}", exc_info=True)
            return  # Generator returns nothing on error

    def _process_standard_entities(
        self, datahub_graph: DataHubGraph, registry: Any, build_context: Dict[str, Any]
    ) -> Iterable[Any]:
        """
        Process standard entities in processing order.

        Yields MCPs as they're generated for memory efficiency.
        """
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

                entities = self._get_entities_from_graph(datahub_graph, entity_type)
                if not entities:
                    logger.debug(f"No {entity_type} entities to process")
                    continue

                self._log_entity_processing(entity_type, entities, registry)
                entity_mcps = self._build_entity_mcps(
                    mcp_builder, entities, entity_type, build_context
                )
                # Yield MCPs as they're generated
                for mcp in entity_mcps:
                    yield mcp
                # Process post-processing hooks and yield their MCPs
                for mcp in self._process_post_processing_hooks(
                    mcp_builder, entity_type, datahub_graph, build_context
                ):
                    yield mcp
            except Exception as e:
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

    def _get_entities_from_graph(
        self, datahub_graph: DataHubGraph, entity_type: str
    ) -> List[Any]:
        """Get entity collection from graph."""
        if entity_type == "glossary_term":
            return datahub_graph.glossary_terms
        if entity_type == "relationship":
            return datahub_graph.relationships
        return getattr(datahub_graph, f"{entity_type}s", [])

    def _log_entity_processing(
        self, entity_type: str, entities: List[Any], registry: Any
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

    def _build_entity_mcps(
        self,
        mcp_builder: Any,
        entities: List[Any],
        entity_type: str,
        build_context: Dict[str, Any],
    ) -> List[Any]:
        """Build MCPs for entities."""
        mcps = []
        if hasattr(mcp_builder, "build_all_mcps"):
            try:
                entity_mcps = mcp_builder.build_all_mcps(entities, build_context)
                if entity_mcps:
                    mcps.extend(entity_mcps)
                    for _ in entity_mcps:
                        if hasattr(self.report, "report_entity_emitted"):
                            self.report.report_entity_emitted()
                        # Track entity type statistics
                        if entity_type == "glossary_term" and hasattr(
                            self.report, "report_glossary_term"
                        ):
                            self.report.report_glossary_term()
                        elif entity_type == "relationship" and hasattr(
                            self.report, "report_relationship"
                        ):
                            self.report.report_relationship()
                    logger.debug(
                        f"Created {len(entity_mcps)} MCPs for {len(entities)} {entity_type} entities"
                    )
                else:
                    logger.debug(
                        f"No MCPs created for {len(entities)} {entity_type} entities (they may have been filtered out)"
                    )
            except Exception as e:
                self.report.report_failure(
                    f"Failed to create MCPs for {entity_type}",
                    context=f"Entity count: {len(entities)}",
                    exc=e,
                )
                logger.error(
                    f"Failed to create MCPs for {entity_type}: {e}", exc_info=True
                )
        else:
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
                    entity_urn = getattr(entity, "urn", "unknown")
                    self.report.report_failure(
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

    def _process_post_processing_hooks(
        self,
        mcp_builder: Any,
        entity_type: str,
        datahub_graph: DataHubGraph,
        build_context: Dict[str, Any],
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
            except Exception as e:
                self.report.report_failure(
                    f"Failed to create post-processing MCPs for {entity_type}",
                    context="Post-processing hook",
                    exc=e,
                )
                logger.error(
                    f"Failed to create post-processing MCPs for {entity_type}: {e}",
                    exc_info=True,
                )

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
                        if hasattr(self.report, "report_entity_emitted"):
                            self.report.report_entity_emitted()
                        # Track glossary nodes and terms from post-processing
                        if hasattr(mcp, "entityType"):
                            if mcp.entityType == "glossaryNode" and hasattr(
                                self.report, "report_glossary_node"
                            ):
                                self.report.report_glossary_node()
                            elif mcp.entityType == "glossaryTerm" and hasattr(
                                self.report, "report_glossary_term"
                            ):
                                self.report.report_glossary_term()
                        yield mcp
                    logger.info(
                        f"Created {len(post_mcps)} glossary node/term MCPs from domain hierarchy"
                    )
            except Exception as e:
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
                        if hasattr(self.report, "report_entity_emitted"):
                            self.report.report_entity_emitted()
                        yield mcp
                    logger.info(
                        f"Created {len(post_mcps)} structured property value assignment MCPs"
                    )
            except Exception as e:
                self.report.report_failure(
                    "Failed to create structured property value assignment MCPs",
                    context="Post-processing hook",
                    exc=e,
                )
                logger.error(
                    f"Failed to create structured property value assignment MCPs: {e}",
                    exc_info=True,
                )

    def _log_mcp_summary(self, mcps: List[Any], datahub_graph: DataHubGraph) -> None:
        """Log summary of MCPs created."""
        glossary_mcps = sum(
            1 for mcp in mcps if "glossary" in str(mcp.entityUrn).lower()
        )
        dataset_mcps = sum(1 for mcp in mcps if "dataset" in str(mcp.entityUrn).lower())
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

    def _convert_mcps_to_workunits(self, mcps: List[Any]) -> List[MetadataWorkUnit]:
        """Convert MCPs to work units."""
        workunits = []
        for i, mcp in enumerate(mcps):
            workunit = MetadataWorkUnit(id=f"rdf-{i}", mcp=mcp)
            workunits.append(workunit)
            if hasattr(self.report, "report_workunit_produced"):
                self.report.report_workunit_produced()
        return workunits

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
