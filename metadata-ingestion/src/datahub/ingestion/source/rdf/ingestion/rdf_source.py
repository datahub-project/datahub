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
from typing import Dict, Iterable, List, Optional

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
from datahub.ingestion.source.rdf.core import (
    Orchestrator,
    QueryFactory,
    RDFToDataHubTranspiler,
    SourceFactory,
)
from datahub.ingestion.source.rdf.dialects import RDFDialect
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

    # Query Options
    sparql: Optional[str] = Field(
        default=None, description="Optional SPARQL query to execute on the RDF graph"
    )
    filter: Optional[Dict[str, str]] = Field(
        default=None, description="Optional filter criteria as key-value pairs"
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

            # Create RDF source
            source = self._create_source()

            # Create query
            query = self._create_query()

            # Create target (collects work units)
            target = DataHubIngestionTarget(self.report)

            # Create transpiler
            transpiler = self._create_transpiler()

            # Create orchestrator
            orchestrator = Orchestrator(source, query, target, transpiler)

            # Execute pipeline
            logger.info("Executing RDF pipeline")
            results = orchestrator.execute()

            if not results["success"]:
                error_msg = results.get("error", "Unknown error")
                logger.error(f"Pipeline execution failed: {error_msg}")
                self.report.report_failure(f"Pipeline execution failed: {error_msg}")
                return

            # Report statistics
            source_results = results.get("source_results", {})
            if "triples_loaded" in source_results:
                self.report.report_triples_processed(source_results["triples_loaded"])

            logger.info(
                f"Pipeline execution completed. Generated {len(target.workunits)} work units"
            )

            # Yield all work units
            for workunit in target.get_workunits():
                yield workunit

        except Exception as e:
            logger.error(f"RDF ingestion failed: {e}", exc_info=True)
            self.report.report_failure(f"Ingestion failed: {e}")

    def _create_source(self):
        """Create RDF source from configuration."""
        from pathlib import Path

        source_path = self.config.source

        # Check if it's a server URL
        if source_path.startswith(("http://", "https://")):
            return SourceFactory.create_server_source(source_path, self.config.format)

        # Check if it's a folder
        path = Path(source_path)
        if path.is_dir():
            return SourceFactory.create_folder_source(
                source_path,
                recursive=self.config.recursive,
                file_extensions=self.config.extensions,
            )

        # Check if it's a single file
        if path.is_file():
            return SourceFactory.create_file_source(source_path, self.config.format)

        # Check if it's comma-separated files
        if "," in source_path:
            files = [f.strip() for f in source_path.split(",")]
            return SourceFactory.create_multi_file_source(files, self.config.format)

        # Try glob pattern
        import glob

        matching_files = glob.glob(source_path)
        if matching_files:
            if len(matching_files) == 1:
                return SourceFactory.create_file_source(
                    matching_files[0], self.config.format
                )
            else:
                return SourceFactory.create_multi_file_source(
                    matching_files, self.config.format
                )

        raise ValueError(f"Source not found: {source_path}")

    def _create_query(self):
        """Create query from configuration."""
        if self.config.sparql:
            return QueryFactory.create_sparql_query(
                self.config.sparql, "Custom SPARQL Query"
            )
        elif self.config.filter:
            return QueryFactory.create_filter_query(self.config.filter, "Filter Query")
        else:
            return QueryFactory.create_pass_through_query("Pass-through Query")

    def _create_transpiler(self):
        """Create transpiler from configuration."""
        # Parse dialect if provided
        forced_dialect = None
        if self.config.dialect:
            forced_dialect = RDFDialect(self.config.dialect)

        return RDFToDataHubTranspiler(
            environment=self.config.environment,
            forced_dialect=forced_dialect,
            export_only=self.config.export_only,
            skip_export=self.config.skip_export,
        )

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
