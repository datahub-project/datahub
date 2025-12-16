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
