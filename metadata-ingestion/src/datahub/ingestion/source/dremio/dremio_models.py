from typing import Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator

from datahub.utilities.str_enum import StrEnum


class DremioEntityContainerType(StrEnum):
    """Dremio container types."""

    SOURCE = "SOURCE"
    SPACE = "SPACE"
    FOLDER = "FOLDER"
    CONTAINER = "CONTAINER"
    HOME = "HOME SPACE"


class DremioDatasetType(StrEnum):
    """Dremio dataset types."""

    VIEW = "View"
    TABLE = "Table"


class DremioContainerResponse(BaseModel):
    """Flexible Dremio container response that can parse various API response formats."""

    # Core fields with flexible parsing
    id: Optional[str] = None
    name: str = Field(alias="name")
    container_type: str = Field(alias="containerType")
    path: Optional[List[str]] = Field(default=None)

    # Optional fields that may or may not be present
    source_type: Optional[str] = Field(default=None, alias="type")
    root_path: Optional[str] = Field(default=None)
    database_name: Optional[str] = Field(default=None)

    # Additional API fields that might be useful
    tag: Optional[str] = Field(default=None)
    created_at: Optional[str] = Field(default=None, alias="createdAt")
    root_container_type: Optional[str] = (
        None  # Track the root container type for folders
    )

    class Config:
        # Allow extra fields from API without failing
        extra = "ignore"
        # Allow population by field name OR alias
        populate_by_name = True

    @field_validator("path", mode="before")
    @classmethod
    def parse_path(cls, v: Union[str, List[str], None]) -> List[str]:
        """Handle path field which might come as string or list."""
        if isinstance(v, str):
            return [v] if v else []
        return v or []

    @model_validator(mode="before")
    @classmethod
    def extract_name_from_path_if_missing(
        cls, values: Dict[str, Union[str, List[str], None]]
    ) -> Dict[str, Union[str, List[str], None]]:
        """Extract name from path if name is not directly provided."""
        if isinstance(values, dict):
            # If name is missing, try to extract from path
            if not values.get("name"):
                path = values.get("path", [])
                if isinstance(path, list) and path:
                    values["name"] = path[-1]  # Last element is usually the name
                elif isinstance(path, str):
                    values["name"] = path
                else:
                    values["name"] = "unknown"
        return values


class DremioDatasetColumn(BaseModel):
    """Flexible Dremio dataset column that can parse various API response formats."""

    name: str
    ordinal_position: int = Field(alias="ordinal_position")
    data_type: str = Field(alias="data_type")
    column_size: int = Field(alias="column_size")
    is_nullable: str = Field(default="NO", alias="is_nullable")

    class Config:
        extra = "ignore"
        populate_by_name = True

    @field_validator("ordinal_position", mode="before")
    @classmethod
    def parse_ordinal_position(cls, v: Union[str, int, None]) -> int:
        """Ensure ordinal_position is always an integer for proper sorting."""
        if v is None:
            return 0
        try:
            return int(v)
        except (ValueError, TypeError):
            return 0

    @field_validator("column_size", mode="before")
    @classmethod
    def parse_column_size(cls, v: Union[str, int, None]) -> int:
        """Ensure column_size is always an integer."""
        if v is None:
            return 0
        try:
            return int(v)
        except (ValueError, TypeError):
            return 0


class DremioDatasetResponse(BaseModel):
    """Flexible Dremio dataset response that can parse various API response formats."""

    # Core dataset fields
    resource_id: str = Field(alias="RESOURCE_ID")
    table_name: str = Field(alias="TABLE_NAME")
    table_schema: str = Field(alias="TABLE_SCHEMA")
    location_id: str = Field(alias="LOCATION_ID")
    columns: List[DremioDatasetColumn] = Field(default_factory=list, alias="COLUMNS")

    # Optional fields
    view_definition: Optional[str] = Field(default=None, alias="VIEW_DEFINITION")
    owner: Optional[str] = Field(default=None, alias="OWNER")
    owner_type: Optional[str] = Field(default=None, alias="OWNER_TYPE")
    created: Optional[str] = Field(default=None, alias="CREATED")
    format_type: Optional[str] = Field(default=None, alias="FORMAT_TYPE")

    class Config:
        extra = "ignore"
        populate_by_name = True

    @field_validator("columns", mode="before")
    @classmethod
    def parse_columns(
        cls,
        v: Union[List[Dict[str, Union[str, int]]], List[DremioDatasetColumn], None],
    ) -> List[DremioDatasetColumn]:
        """Parse columns from various formats and ensure they're sorted by ordinal_position."""
        if not v:
            return []
        if isinstance(v, list):
            # Parse columns into DremioDatasetColumn objects
            parsed_columns = [
                DremioDatasetColumn.model_validate(col)
                if isinstance(col, dict)
                else col
                for col in v
            ]
            # Sort by ordinal_position to ensure consistent ordering
            return sorted(parsed_columns, key=lambda col: col.ordinal_position)
        return []

    @property
    def path(self) -> List[str]:
        """Extract path from table_schema field."""
        if self.table_schema:
            # Remove brackets and split, then remove last empty element
            return self.table_schema[1:-1].split(", ")[:-1]
        return []


class DremioColumnStats(BaseModel):
    """Column-specific statistics with dot notation access."""

    distinct_count: Optional[int] = None
    null_count: Optional[int] = None
    min: Optional[Union[str, int, float]] = None
    max: Optional[Union[str, int, float]] = None
    mean: Optional[float] = None
    median: Optional[float] = None
    stdev: Optional[float] = None
    percentile_25th: Optional[float] = Field(default=None, alias="25th_percentile")
    percentile_75th: Optional[float] = Field(default=None, alias="75th_percentile")

    class Config:
        extra = "allow"
        populate_by_name = True

    @property
    def quantiles(self) -> Optional[List[float]]:
        """Get quantiles as a list for compatibility."""
        if self.percentile_25th is not None and self.percentile_75th is not None:
            return [self.percentile_25th, self.percentile_75th]
        return None


class DremioProfilingResult(BaseModel):
    """Flexible Dremio profiling result that can parse various API response formats."""

    # Core profiling fields
    row_count: int = Field(default=0, alias="row_count")
    column_count: int = Field(default=0, alias="column_count")

    class Config:
        extra = "allow"  # Allow all extra fields for dynamic column stats
        populate_by_name = True

    def get_column_stats(self, column_name: str) -> DremioColumnStats:
        """Get column statistics as a structured object with dot notation."""
        # Extract all stats for this column from the raw data
        column_data = {}

        # Use model_dump to get all fields including extra ones
        all_fields = self.model_dump()

        # Map the dynamic fields to our structured model
        for field_name, value in all_fields.items():
            if field_name.startswith(f"{column_name}_"):
                stat_name = field_name[len(f"{column_name}_") :]
                column_data[stat_name] = value

        return DremioColumnStats.model_validate(column_data)

    def get_column_stat(
        self,
        column_name: str,
        stat_type: str,
        default: Optional[Union[str, int, float]] = None,
    ) -> Optional[Union[str, int, float]]:
        """Get a specific column statistic (backwards compatibility)."""
        field_name = f"{column_name}_{stat_type}"
        return getattr(self, field_name, default)
