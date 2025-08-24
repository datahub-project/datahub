"""
Pydantic models for Dremio API responses and data structures.

This module provides strongly-typed models that replace dictionary access
with direct object attributes, reducing the need for .get() statements
and improving type safety.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator


class DremioEntityType(str, Enum):
    """Dremio entity types."""

    SPACE = "SPACE"
    SOURCE = "SOURCE"
    FOLDER = "FOLDER"
    DATASET = "DATASET"
    PHYSICAL_DATASET = "PHYSICAL_DATASET"
    VIRTUAL_DATASET = "VIRTUAL_DATASET"
    FILE = "FILE"
    FUNCTION = "FUNCTION"


class DremioColumnInfo(BaseModel):
    """Represents a column in a Dremio dataset."""

    column_name: str = Field(alias="COLUMN_NAME")
    data_type: str = Field(alias="DATA_TYPE")
    is_nullable: Optional[str] = Field(default=None, alias="IS_NULLABLE")
    column_size: Optional[int] = Field(default=None, alias="COLUMN_SIZE")
    ordinal_position: Optional[int] = Field(default=None, alias="ORDINAL_POSITION")

    class Config:
        allow_population_by_field_name = True

    @validator("is_nullable")
    def normalize_nullable(cls, v):
        """Convert nullable string to boolean-like representation."""
        if v is None:
            return None
        return v.upper() in ("YES", "TRUE", "1")


class DremioViewDefinition(BaseModel):
    """Represents a reassembled view definition from chunks."""

    definition: str
    total_length: int
    chunk_count: int
    is_truncated: bool = False

    @classmethod
    def from_chunks(cls, row: Dict[str, Any]) -> "DremioViewDefinition":
        """Create view definition from chunked row data."""
        chunks = []

        # Collect all non-null chunks in order
        for i in range(4):  # Support up to 4 chunks (128KB total)
            chunk_key = f"VIEW_DEFINITION_CHUNK_{i}"
            if chunk_key in row and row[chunk_key] is not None:
                chunks.append(row[chunk_key])
            else:
                break

        definition = "".join(chunks) if chunks else ""
        total_length = row.get("VIEW_DEFINITION_TOTAL_LENGTH", len(definition))

        return cls(
            definition=definition,
            total_length=total_length,
            chunk_count=len(chunks),
            is_truncated=len(definition) < total_length,
        )


class DremioDatasetInfo(BaseModel):
    """Represents a Dremio dataset with all its metadata."""

    table_name: str = Field(alias="TABLE_NAME")
    table_schema: str = Field(alias="TABLE_SCHEMA")
    full_table_path: str = Field(alias="FULL_TABLE_PATH")
    resource_id: Optional[str] = Field(default=None, alias="RESOURCE_ID")
    location_id: Optional[str] = Field(default=None, alias="LOCATION_ID")
    owner: Optional[str] = Field(default=None, alias="OWNER")
    owner_type: Optional[str] = Field(default=None, alias="OWNER_TYPE")
    format_type: Optional[str] = Field(default=None, alias="FORMAT_TYPE")
    created: Optional[datetime] = Field(default=None, alias="CREATED")

    # View definition (reassembled from chunks)
    view_definition: Optional[DremioViewDefinition] = None

    # Columns
    columns: List[DremioColumnInfo] = Field(default_factory=list)

    class Config:
        allow_population_by_field_name = True

    @classmethod
    def from_raw_data(
        cls, raw_data: Dict[str, Any], columns: Optional[List[DremioColumnInfo]] = None
    ) -> "DremioDatasetInfo":
        """Create dataset info from raw Dremio API response."""
        # Handle view definition chunks
        view_def = None
        if any(f"VIEW_DEFINITION_CHUNK_{i}" in raw_data for i in range(4)):
            view_def = DremioViewDefinition.from_chunks(raw_data)

        return cls(**raw_data, view_definition=view_def, columns=columns or [])

    @property
    def is_view(self) -> bool:
        """Check if this dataset is a view (has view definition)."""
        return self.view_definition is not None and bool(
            self.view_definition.definition
        )

    @property
    def is_physical_dataset(self) -> bool:
        """Check if this is a physical dataset."""
        return not self.is_view and self.format_type is not None


class DremioContainerInfo(BaseModel):
    """Represents a Dremio container (space, source, folder)."""

    container_name: str
    container_type: DremioEntityType
    resource_id: str
    location_id: Optional[str] = None
    owner: Optional[str] = None
    owner_type: Optional[str] = None
    path: List[str] = Field(default_factory=list)

    @property
    def formatted_path(self) -> List[str]:
        """Get formatted path for DataHub."""
        return [self.container_name] if not self.path else self.path


class DremioQueryResult(BaseModel):
    """Represents the result of a Dremio query execution."""

    rows: List[Dict[str, Any]]
    row_count: int
    job_id: str
    query_state: Optional[str] = None

    @classmethod
    def from_api_response(
        cls, response: Dict[str, Any], job_id: str
    ) -> "DremioQueryResult":
        """Create query result from API response with proper error handling."""
        if not isinstance(response, dict):
            raise ValueError(
                f"Invalid response format: expected dict, got {type(response)}"
            )

        if "errorMessage" in response:
            raise ValueError(f"Query error: {response['errorMessage']}")

        if "rows" not in response:
            available_keys = list(response.keys())
            raise ValueError(
                f"Query result missing 'rows' key. Available keys: {available_keys}. "
                f"This may indicate an out-of-memory error on the Dremio server."
            )

        rows = response["rows"]
        row_count = response.get("rowCount", len(rows))

        return cls(
            rows=rows,
            row_count=row_count,
            job_id=job_id,
            query_state=response.get("queryState"),
        )


class DremioJobStatus(BaseModel):
    """Represents Dremio job status information."""

    job_id: str
    job_state: str
    query_type: Optional[str] = None
    user: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    query_text: Optional[str] = None

    @property
    def is_completed(self) -> bool:
        """Check if job is completed."""
        return self.job_state in ("COMPLETED", "FAILED", "CANCELED")

    @property
    def is_successful(self) -> bool:
        """Check if job completed successfully."""
        return self.job_state == "COMPLETED"


class DremioSourceMapEntry(BaseModel):
    """Represents a mapping entry for Dremio sources to DataHub platforms."""

    platform: str
    source_name: str
    dremio_source_category: str
    root_path: str = "/"
    database_name: str = ""
    platform_instance: Optional[str] = None
    env: Optional[str] = None


class DremioAPIError(BaseModel):
    """Represents a Dremio API error response."""

    error_message: str = Field(alias="errorMessage")
    more_info: Optional[str] = Field(default=None, alias="moreInfo")
    error_code: Optional[str] = Field(default=None, alias="errorCode")

    class Config:
        allow_population_by_field_name = True


class DremioProcessingStats(BaseModel):
    """Statistics for Dremio data processing."""

    total_datasets: int = 0
    total_containers: int = 0
    views_processed: int = 0
    physical_datasets_processed: int = 0
    large_view_definitions_chunked: int = 0
    errors_encountered: int = 0
    processing_time_seconds: float = 0.0

    def add_dataset(self, dataset: DremioDatasetInfo) -> None:
        """Add a dataset to the statistics."""
        self.total_datasets += 1
        if dataset.is_view:
            self.views_processed += 1
            if dataset.view_definition and dataset.view_definition.chunk_count > 1:
                self.large_view_definitions_chunked += 1
        else:
            self.physical_datasets_processed += 1

    def add_container(self) -> None:
        """Add a container to the statistics."""
        self.total_containers += 1

    def add_error(self) -> None:
        """Add an error to the statistics."""
        self.errors_encountered += 1


# Type aliases for common patterns
DremioRawRow = Dict[str, Any]
DremioDatasetList = List[DremioDatasetInfo]
DremioContainerList = List[DremioContainerInfo]
