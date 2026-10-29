import json
from typing import Dict, List, Optional, Set, Union

from pydantic import BaseModel, Field, PrivateAttr, field_validator, model_validator

from datahub.metadata.schema_classes import SchemaFieldDataTypeClass
from datahub.utilities.str_enum import StrEnum


class DremioEntityContainerType(StrEnum):
    """Dremio container types."""

    SOURCE = "SOURCE"
    SPACE = "SPACE"
    FOLDER = "FOLDER"
    CONTAINER = "CONTAINER"
    HOME = "HOME"


class DremioDatasetType(StrEnum):
    """Dremio dataset types."""

    VIEW = "View"
    TABLE = "Table"


class DremioJobState(StrEnum):
    """Dremio async job terminal and in-progress states."""

    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"
    RUNNING = "RUNNING"


class DremioOwnerType(StrEnum):
    """Dremio dataset/space owner types."""

    USER = "USER"
    GROUP = "GROUP"


class DremioFieldType(BaseModel):
    schema_field_type: SchemaFieldDataTypeClass
    native_data_type: str

    class Config:
        arbitrary_types_allowed = True
        frozen = True


class DremioContainerMatch(StrEnum):
    EXACT = "EXACT"
    CASE_INSENSITIVE = "CASE_INSENSITIVE"
    AMBIGUOUS = "AMBIGUOUS"
    MISSING = "MISSING"


class DremioContainerPathResolution(BaseModel):
    match: DremioContainerMatch
    # The emitted casing; set only for EXACT and CASE_INSENSITIVE.
    path: Optional[List[str]] = None


class DremioContainerPathIndex(BaseModel):
    """Emitted container paths, matched case-insensitively like Dremio identifiers."""

    _exact: Set[str] = PrivateAttr(default_factory=set)
    _by_folded_key: Dict[str, List[List[str]]] = PrivateAttr(default_factory=dict)

    @staticmethod
    def _key(path: List[str]) -> str:
        # JSON keeps segments distinct even when a name contains a dot.
        return json.dumps(path)

    @classmethod
    def _folded_key(cls, path: List[str]) -> str:
        return cls._key([part.lower() for part in path])

    def add(self, path: List[str]) -> None:
        key = self._key(path)
        if key in self._exact:
            return
        self._exact.add(key)
        self._by_folded_key.setdefault(self._folded_key(path), []).append(list(path))

    def resolve(self, path: List[str]) -> DremioContainerPathResolution:
        if self._key(path) in self._exact:
            return DremioContainerPathResolution(
                match=DremioContainerMatch.EXACT, path=list(path)
            )
        matches = self._by_folded_key.get(self._folded_key(path), [])
        if len(matches) == 1:
            return DremioContainerPathResolution(
                match=DremioContainerMatch.CASE_INSENSITIVE, path=list(matches[0])
            )
        # A case-sensitive source can hold both `Data` and `data`.
        return DremioContainerPathResolution(
            match=DremioContainerMatch.AMBIGUOUS
            if matches
            else DremioContainerMatch.MISSING
        )


class DremioContainerResponse(BaseModel):
    """Typed Dremio container payload covering Catalog and child-listing shapes."""

    id: Optional[str] = None
    name: str = Field(alias="name")
    container_type: str = Field(alias="containerType")
    path: Optional[List[str]] = Field(default=None)
    source_type: Optional[str] = Field(default=None, alias="type")
    root_path: Optional[str] = Field(default=None)
    database_name: Optional[str] = Field(default=None)
    tag: Optional[str] = Field(default=None)
    created_at: Optional[str] = Field(default=None, alias="createdAt")
    # Stamped by the recursive catalog walk in DremioAPIOperations so a
    # folder inherits its root container's subtype for browse-path
    # prefixing.
    root_container_type: Optional[str] = None

    @property
    def full_path(self) -> List[str]:
        return [*(self.path or []), self.name]

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
        # Catalog-walk payloads expose the entry via `path` only; backfill
        # `name` from the last segment. Raise rather than fall back to a
        # placeholder — an "unknown" entity would corrupt URNs downstream.
        if isinstance(values, dict) and not values.get("name"):
            path = values.get("path", [])
            if isinstance(path, list) and path:
                values["name"] = path[-1]
            elif isinstance(path, str) and path:
                values["name"] = path
            else:
                raise ValueError(
                    "DremioContainerResponse requires `name` or a non-empty `path`; "
                    f"got name={values.get('name')!r}, path={path!r}"
                )
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

    resource_id: str = Field(alias="RESOURCE_ID")
    table_name: str = Field(alias="TABLE_NAME")
    table_schema: str = Field(alias="TABLE_SCHEMA")
    # Community edition can return null LOCATION_ID for views; preserve
    # the legacy "" default downstream rather than rejecting in pydantic.
    location_id: Optional[str] = Field(default=None, alias="LOCATION_ID")
    columns: List[DremioDatasetColumn] = Field(default_factory=list, alias="COLUMNS")
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
    """Per-column profile statistics. Consumer lands in #17653 / A3."""

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
    """Typed Dremio profiling response. Consumer lands in #17653 / A3."""

    row_count: int = Field(default=0, alias="row_count")
    column_count: int = Field(default=0, alias="column_count")

    class Config:
        extra = "allow"  # Allow all extra fields for dynamic column stats
        populate_by_name = True

    def get_column_stats(self, column_name: str) -> DremioColumnStats:
        """Get column statistics as a structured object with dot notation."""
        column_data = {}
        all_fields = self.model_dump()
        for field_name, value in all_fields.items():
            if field_name.startswith(f"{column_name}_"):
                stat_name = field_name[len(f"{column_name}_") :]
                column_data[stat_name] = value

        return DremioColumnStats.model_validate(column_data)
