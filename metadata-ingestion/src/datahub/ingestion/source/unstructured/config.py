"""Shared configuration models for Unstructured.io-based sources."""

import re
from typing import Any, Dict, List, Literal, Optional

from pydantic import Field, field_validator

from datahub.configuration.common import ConfigModel

# ===== Unstructured Processing Configs =====


class PartitionConfig(ConfigModel):
    """Unstructured partitioning configuration."""

    strategy: Literal["auto", "hi_res", "fast", "ocr_only"] = Field(
        default="auto", description="Partitioning strategy"
    )
    partition_by_api: bool = Field(
        default=False, description="Use Unstructured API for partitioning"
    )
    api_key: Optional[str] = Field(default=None, description="Unstructured API key")
    split_pdf_page: bool = Field(
        default=False, description="Enable page-level splitting for large PDFs"
    )
    split_pdf_concurrency_level: int = Field(
        default=5, description="Number of parallel requests for PDF pages"
    )
    ocr_languages: List[str] = Field(default=["eng"], description="Languages for OCR")
    additional_args: Dict[str, Any] = Field(
        default_factory=dict, description="Additional partition arguments"
    )


class ParallelismConfig(ConfigModel):
    """Parallelism configuration."""

    num_processes: int = Field(
        default=2, ge=1, le=32, description="Number of worker processes"
    )
    disable_parallelism: bool = Field(
        default=False, description="Disable all parallelism"
    )
    max_connections: int = Field(
        default=10, description="Max concurrent connections for async operations"
    )


class ProcessingConfig(ConfigModel):
    """Processing configuration (partitioning only, no chunking)."""

    partition: PartitionConfig = Field(
        default_factory=PartitionConfig, description="Partition configuration"
    )
    parallelism: ParallelismConfig = Field(
        default_factory=ParallelismConfig, description="Parallelism configuration"
    )


# ===== Document Mapping Configs =====


class IdNormalizationConfig(ConfigModel):
    """Document ID normalization rules."""

    lowercase: bool = Field(default=True, description="Convert to lowercase")
    replace_spaces_with: str = Field(
        default="-", description="Replace spaces with this character"
    )
    remove_special_chars: bool = Field(
        default=True, description="Remove special characters except _ and -"
    )
    max_length: int = Field(default=200, description="Maximum ID length")


class TitleExtractionConfig(ConfigModel):
    """Title extraction configuration."""

    extract_from_content: bool = Field(
        default=True, description="Try to extract title from document content"
    )
    fallback_to_filename: bool = Field(
        default=True, description="Use filename as title if not found in content"
    )
    max_length: int = Field(default=500, description="Maximum title length")


class SourceConfig(ConfigModel):
    """Document source configuration."""

    type: Literal["NATIVE", "EXTERNAL"] = Field(
        default="EXTERNAL",
        description="Source type (always EXTERNAL for ingested docs)",
    )
    include_external_url: bool = Field(
        default=True, description="Include external URL in DocumentSource"
    )
    include_external_id: bool = Field(
        default=True, description="Include external ID in DocumentSource"
    )


class DocumentMappingConfig(ConfigModel):
    """Document entity mapping configuration."""

    id_pattern: str = Field(
        default="{source_type}-{directory}-{basename}",
        description="Pattern for generating document IDs",
    )
    id_normalization: IdNormalizationConfig = Field(
        default_factory=IdNormalizationConfig, description="ID normalization rules"
    )
    title: TitleExtractionConfig = Field(
        default_factory=TitleExtractionConfig,
        description="Title extraction configuration",
    )
    source: SourceConfig = Field(
        default_factory=SourceConfig, description="Source configuration"
    )
    status: Literal["PUBLISHED", "UNPUBLISHED"] = Field(
        default="PUBLISHED", description="Default publication status"
    )

    @field_validator("id_pattern")
    @classmethod
    def validate_pattern(cls, v: str) -> str:
        valid_vars = {
            "source_type",
            "directory",
            "filename",
            "basename",
            "extension",
            "date",
            "uuid",
        }
        vars_in_pattern = set(re.findall(r"\{(\w+)\}", v))
        invalid = vars_in_pattern - valid_vars
        if invalid:
            raise ValueError(
                f"Invalid template variables: {invalid}. Valid variables: {valid_vars}"
            )
        return v


# ===== Hierarchy Configs =====


class FolderMappingConfig(ConfigModel):
    """Folder hierarchy mapping configuration."""

    create_parent_docs: bool = Field(
        default=True, description="Create Document entities for folders"
    )
    parent_id_pattern: str = Field(
        default="{source_type}-{directory}",
        description="Pattern for parent document IDs",
    )
    max_depth: int = Field(
        default=10, ge=1, le=50, description="Maximum hierarchy depth"
    )
    root_parent: Optional[str] = Field(
        default=None, description="Optional root document URN"
    )


class CustomParentRule(ConfigModel):
    """Custom parent mapping rule."""

    pattern: str = Field(description="Glob pattern to match file paths")
    parent_id: str = Field(description="Parent document ID for matching files")


class CustomMappingConfig(ConfigModel):
    """Custom parent mapping configuration."""

    rules: List[CustomParentRule] = Field(
        default_factory=list, description="Custom parent mapping rules"
    )


class HierarchyConfig(ConfigModel):
    """Hierarchy configuration."""

    enabled: bool = Field(default=True, description="Enable parent-child relationships")
    parent_strategy: Literal["folder", "none", "custom", "notion"] = Field(
        default="folder",
        description="Parent document creation strategy. "
        "'notion' extracts parent from Notion API metadata",
    )
    folder_mapping: FolderMappingConfig = Field(
        default_factory=FolderMappingConfig, description="Folder mapping configuration"
    )
    custom_mapping: Optional[CustomMappingConfig] = Field(
        default=None, description="Custom mapping configuration"
    )


# ===== Metadata Configs =====


class MetadataCaptureConfig(ConfigModel):
    """What metadata to capture."""

    file_metadata: bool = Field(default=True)
    document_properties: bool = Field(default=True)
    element_statistics: bool = Field(default=True)
    processing_metadata: bool = Field(default=True)
    source_metadata: bool = Field(default=True)


class CustomPropertyMapping(ConfigModel):
    """Custom property extraction rule."""

    path: str = Field(description="JSONPath expression to extract value")
    name: str = Field(description="Property name in customProperties")


class MetadataConfig(ConfigModel):
    """Metadata configuration."""

    capture: MetadataCaptureConfig = Field(
        default_factory=MetadataCaptureConfig, description="What metadata to capture"
    )
    custom_properties: List[CustomPropertyMapping] = Field(
        default_factory=list, description="Custom property mappings"
    )


# ===== Relationships Configs =====


class RelatedAssetsConfig(ConfigModel):
    """Related assets configuration."""

    enabled: bool = Field(default=True, description="Enable relatedAssets links")
    create_source_asset_links: bool = Field(
        default=True, description="Create links to source system URNs"
    )
    source_platform_mapping: Dict[str, str] = Field(
        default_factory=lambda: {
            "s3": "s3",
            "google-drive": "gdrive",
            "sharepoint": "sharepoint",
        },
        description="Map source types to platform names",
    )


class RelatedDocumentsConfig(ConfigModel):
    """Related documents configuration."""

    enabled: bool = Field(default=False, description="Enable relatedDocuments links")


class RelationshipsConfig(ConfigModel):
    """Relationships configuration."""

    related_assets: RelatedAssetsConfig = Field(
        default_factory=RelatedAssetsConfig,
        description="Related assets configuration",
    )
    related_documents: RelatedDocumentsConfig = Field(
        default_factory=RelatedDocumentsConfig,
        description="Related documents configuration",
    )


# ===== Filtering Configs =====


class FilteringConfig(ConfigModel):
    """File filtering configuration."""

    include_patterns: List[str] = Field(
        default_factory=lambda: ["*"], description="Glob patterns to include"
    )
    exclude_patterns: List[str] = Field(
        default_factory=list, description="Glob patterns to exclude"
    )
    min_file_size: Optional[int] = Field(
        default=None, description="Minimum file size in bytes"
    )
    max_file_size: Optional[int] = Field(
        default=None, description="Maximum file size in bytes"
    )
    modified_after: Optional[str] = Field(
        default=None, description="Only files modified after this date (ISO format)"
    )
    modified_before: Optional[str] = Field(
        default=None, description="Only files modified before this date (ISO format)"
    )
    skip_empty_documents: bool = Field(
        default=True, description="Skip documents with no text content"
    )
    min_text_length: int = Field(
        default=50, description="Minimum text length in characters"
    )


# ===== Advanced Configs =====


class RetryConfig(ConfigModel):
    """Retry configuration."""

    enabled: bool = Field(default=True)
    max_attempts: int = Field(default=3)
    backoff_factor: int = Field(default=2)
    retry_on_timeout: bool = Field(default=True)


class CacheConfig(ConfigModel):
    """Cache configuration."""

    enabled: bool = Field(default=True)
    cache_dir: str = Field(default="~/.cache/unstructured_datahub")
    ttl: int = Field(default=86400, description="Cache TTL in seconds")


class AdvancedConfig(ConfigModel):
    """Advanced configuration options."""

    work_dir: str = Field(default="/tmp/unstructured_datahub")
    preserve_outputs: bool = Field(default=False)
    output_format: Literal["json", "xml"] = Field(default="json")
    raise_on_error: bool = Field(default=False)
    max_errors: int = Field(default=10)
    continue_on_failure: bool = Field(default=True)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
