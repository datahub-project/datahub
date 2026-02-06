"""
Configuration for RDF ingestion source.

This module contains the configuration class for the RDF ingestion source,
following DataHub conventions for separating configuration from source implementation.
"""

from typing import List, Optional

from pydantic import Field, field_validator

from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.rdf.dialects import RDFDialect
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


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

    Example with zip file:
        ```yaml
        source:
          type: rdf
          config:
            source: /path/to/rdf_data.zip
            format: turtle
            recursive: true
            environment: PROD
        ```

    Example with zip file URL:
        ```yaml
        source:
          type: rdf
          config:
            source: https://example.org/rdf_data.zip
            format: turtle
            recursive: true
            environment: PROD
        ```

    Example with web folder URL:
        ```yaml
        source:
          type: rdf
          config:
            source: https://example.org/rdf_data/
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

    Example with SPARQL filter (filtering by namespace/module):
        ```yaml
        source:
          type: rdf
          config:
            source: https://spec.edmcouncil.org/fibo/ontology/master/latest/fibo-all.ttl
            sparql_filter: |
              CONSTRUCT { ?s ?p ?o }
              WHERE {
                ?s ?p ?o .
                FILTER(
                  STRSTARTS(STR(?s), "https://spec.edmcouncil.org/fibo/ontology/FBC/") ||
                  STRSTARTS(STR(?s), "https://spec.edmcouncil.org/fibo/ontology/FND/")
                )
              }
            environment: PROD
        ```
    """

    # Source Options
    source: str = Field(
        description=(
            "Source to process: file path, folder path, zip file (local or remote), "
            "web folder URL, server URL, or comma-separated files. "
            "Examples: '/path/to/file.ttl', './rdf_data/', '/path/to/data.zip', "
            "'http://example.org/data.zip', 'http://example.org/folder/', "
            "'http://example.org/ontology.owl', '/path/to/file1.ttl,/path/to/file2.ttl'"
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
    include_provisional: bool = Field(
        default=False,
        description=(
            "Include terms with provisional/work-in-progress status (default: False). "
            "When False, only terms that have been fully approved/released are included. "
            "Many ontologies use workflow status properties (e.g., maturity level) to mark "
            "terms that are in the pipeline but not yet fully approved. Setting this to False "
            "helps reduce noise from unapproved or draft terms."
        ),
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

    # SPARQL Filter Options
    sparql_filter: Optional[str] = Field(
        default=None,
        description=(
            "Optional SPARQL CONSTRUCT query to filter the RDF graph before ingestion. "
            "Useful for filtering by namespace, module, or custom patterns. "
            "The query should use CONSTRUCT to build a filtered graph. "
            "Example: Filter to specific FIBO modules: "
            "'CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o . "
            'FILTER(STRSTARTS(STR(?s), "https://spec.edmcouncil.org/fibo/ontology/FBC/")) }\''
        ),
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
                    f"Invalid dialect '{v}'. Must be one of: {', '.join(valid_dialects)}. "
                    f"Please check the dialect value and ensure it matches one of the supported RDF dialects. "
                    f"If unsure, omit this field to use the default dialect detection."
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
                    valid_types_str = ", ".join(sorted(valid_types))
                    raise ValueError(
                        f"Invalid entity type '{entity_type}'. Must be one of: {valid_types_str}. "
                        f"Please check the entity type name and ensure it matches one of the supported types. "
                        f"Supported entity types are: {valid_types_str}."
                    )
        return v

    @field_validator("source")
    @classmethod
    def validate_source(cls, v):
        """Validate source parameter is not empty."""
        if not v or not v.strip():
            raise ValueError(
                "Source parameter cannot be empty. Please provide a file path, folder path, URL, "
                "or comma-separated list of files."
            )
        return v.strip()

    @field_validator("sparql_filter")
    @classmethod
    def validate_sparql_filter(cls, v):
        """Validate SPARQL filter query syntax."""
        if v is not None:
            v = v.strip()
            if not v:
                raise ValueError(
                    "SPARQL filter cannot be empty. Provide a valid SPARQL CONSTRUCT query, "
                    "or omit this field to disable filtering."
                )
            # Basic validation: check if it looks like a SPARQL query
            v_upper = v.upper()
            if "CONSTRUCT" not in v_upper and "SELECT" not in v_upper:
                raise ValueError(
                    "SPARQL filter must be a CONSTRUCT or SELECT query. "
                    "CONSTRUCT queries are recommended for filtering RDF graphs. "
                    "Example: 'CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o . FILTER(...) }'"
                )
            # Note: We don't fully validate the query here because it may reference
            # namespaces or prefixes that will be available when executed against the graph.
            # Full validation will happen when the query is executed.
        return v

    def model_post_init(self, __context):
        """Validate configuration after initialization."""
        # Check for conflicting filters
        if self.export_only and self.skip_export:
            overlapping = set(self.export_only) & set(self.skip_export)
            if overlapping:
                raise ValueError(
                    f"Conflicting filters: export_only and skip_export both include: {', '.join(sorted(overlapping))}. "
                    "You cannot both export and skip the same entity types. "
                    "Please remove the conflicting entity types from one of the filters."
                )
