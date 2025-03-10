from typing import List, Optional

from pydantic import Field

from datahub.ingestion.source.ms_fabric.tmdl.models.base import (
    DataType,
    Expression,
    TMDLBase,
    TMDLMetadata,
)


class Column(TMDLBase):
    """Column definition in a table."""

    data_type: DataType = Field(description="Data type of the column")
    source_column: Optional[str] = Field(
        None, description="Source column name if different"
    )
    is_hidden: bool = Field(False, description="Whether column is hidden")
    is_nullable: bool = Field(True, description="Whether column allows null values")
    expression: Optional[Expression] = Field(
        None, description="Expression for calculated columns"
    )
    lineage_dependencies: Optional[List[str]] = Field(
        default=None, description="List of lineage dependency IDs"
    )
    source_lineage_tag: Optional[str] = Field(None, description="Source lineage tag")
    source_provider_type: Optional[str] = Field(
        None, description="Source provider type"
    )
    format_string: Optional[str] = Field(None, description="Format string")
    summarize_by: Optional[str] = Field(None, description="Summarization method")
    table_name: Optional[str] = Field(None, description="Name of the containing table")
    lineage_tag: Optional[str] = Field(None, description="Lineage tag")

    @property
    def full_name(self) -> str:
        """Get fully qualified column name including table name."""
        if self.table_name:
            return f"{self.table_name}.{self.name}"
        return self.name


class Measure(TMDLBase):
    """Measure definition in a table."""

    expression: Expression = Field(description="DAX expression for the measure")
    format_string: Optional[str] = Field(None, description="Format string")
    is_hidden: bool = Field(False, description="Whether measure is hidden")
    display_folder: Optional[str] = Field(
        None, description="Display folder for organization"
    )
    lineage_dependencies: Optional[List[str]] = Field(
        default=None, description="List of lineage dependency IDs"
    )
    lineage_tag: Optional[str] = Field(None, description="Lineage tag")


class Partition(TMDLBase):
    """Partition definition for a table."""

    mode: str = Field("import", description="Storage mode for the partition")
    source: Expression = Field(..., description="M expression defining the data source")


class Table(TMDLBase):
    """Table definition in TMDL."""

    columns: List[Column] = Field(description="List of columns in the table")
    measures: List[Measure] = Field(description="List of measures in the table")
    partitions: List[Partition] = Field(description="List of partitions for the table")
    is_hidden: bool = Field(False, description="Whether table is hidden")
    lineage_tag: Optional[str] = Field(None, description="Lineage tag")
    source_lineage_tag: Optional[str] = Field(None, description="Source lineage tag")


class TMDLModel(TMDLBase):
    """Top level TMDL model definition."""

    metadata: TMDLMetadata
    database_name: str = Field(description="Database name")
    culture: Optional[str] = Field("en-US", description="Model culture")
    tables: List[Table] = Field(description="List of tables")
    compatibility_level: Optional[int] = Field(1550, description="Compatibility level")
