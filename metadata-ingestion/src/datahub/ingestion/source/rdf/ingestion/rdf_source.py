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
from typing import Iterable, List, Optional

from pydantic import Field, field_validator

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.rdf.core.ast import DataHubGraph
from datahub.ingestion.source.rdf.core.rdf_loader import load_rdf_graph
from datahub.ingestion.source.rdf.dialects import RDFDialect
from datahub.ingestion.source.rdf.entities.domain.builder import DomainBuilder
from datahub.ingestion.source.rdf.entities.registry import create_default_registry
from datahub.ingestion.source.rdf.ingestion.datahub_ingestion_target import (
    DataHubIngestionTarget,
)

logger = logging.getLogger(__name__)


class RDFSourceConfig(ConfigModel):
    """
    Configuration for RDF ingestion source.

    Mirrors the CLI parameters to provide consistent behavior between
    CLI and ingestion framework usage.
    """

    # Source Options
    source: str = Field(
        description="Source to process: file path, folder path, server URL, or comma-separated files"
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
            # Add 'ownership' as a special export target (not an entity type)
            if "ownership" not in valid_types:
                valid_types.append("ownership")

            for entity_type in v:
                if entity_type not in valid_types:
                    raise ValueError(
                        f"Invalid entity type '{entity_type}'. Must be one of: {sorted(valid_types)}"
                    )
        return v


class RDFSourceReport(SourceReport):
    """
    Report for RDF ingestion source.

    Tracks statistics and errors during ingestion.
    """

    num_files_processed: int = 0
    num_triples_processed: int = 0
    num_entities_emitted: int = 0
    num_workunits_produced: int = 0

    def report_file_processed(self):
        """Increment file counter."""
        self.num_files_processed += 1

    def report_triples_processed(self, count: int):
        """Add to triples counter."""
        self.num_triples_processed += count

    def report_entity_emitted(self):
        """Increment entity counter."""
        self.num_entities_emitted += 1

    def report_workunit_produced(self):
        """Increment workunit counter."""
        self.num_workunits_produced += 1


@platform_name("RDF")
@config_class(RDFSourceConfig)
@support_status(SupportStatus.INCUBATING)
class RDFSource(Source):
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
    """

    def __init__(self, config: RDFSourceConfig, ctx: PipelineContext):
        """
        Initialize the RDF source.

        Args:
            config: Source configuration
            ctx: Pipeline context from DataHub
        """
        super().__init__(ctx)
        self.config = config
        self.report = RDFSourceReport()

        logger.info(f"Initializing RDF source with config: {config}")

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

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        """
        Generate work units from RDF data.

        This is the main method that DataHub calls to get metadata.

        Yields:
            MetadataWorkUnit objects containing MCPs
        """
        try:
            logger.info("Starting RDF ingestion")

            # Load RDF graph directly
            logger.info("Loading RDF graph from source")
            rdf_graph = load_rdf_graph(
                source=self.config.source,
                format=self.config.format,
                recursive=self.config.recursive,
                file_extensions=self.config.extensions,
            )
            self.report.report_triples_processed(len(rdf_graph))
            logger.info(f"Loaded {len(rdf_graph)} triples from source")

            # Convert RDF to DataHub AST
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

            # Create target and execute
            target = DataHubIngestionTarget(self.report)
            logger.info("Generating work units from DataHub AST")
            results = target.execute(datahub_ast, rdf_graph)

            if not results.get("success", False):
                error_msg = results.get("error", "Unknown error")
                logger.error(f"Target execution failed: {error_msg}")
                self.report.report_failure(f"Target execution failed: {error_msg}")
                return

            logger.info(
                f"Pipeline execution completed. Generated {len(target.workunits)} work units"
            )

            # Yield all work units
            for workunit in target.get_workunits():
                yield workunit

        except Exception as e:
            logger.error(f"RDF ingestion failed: {e}", exc_info=True)
            self.report.report_failure(f"Ingestion failed: {e}")

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
                datahub_terms = extractor.extract_all(graph, context)
                datahub_graph.glossary_terms = datahub_terms

                # Collect relationships from terms
                from datahub.ingestion.source.rdf.entities.glossary_term.relationship_collector import (
                    collect_relationships_from_terms,
                )

                relationships = collect_relationships_from_terms(datahub_terms)
                datahub_graph.relationships.extend(relationships)

        # Build domains
        domain_builder = DomainBuilder()
        datahub_graph.domains = domain_builder.build_domains(
            datahub_graph.glossary_terms, context
        )

        return datahub_graph

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
