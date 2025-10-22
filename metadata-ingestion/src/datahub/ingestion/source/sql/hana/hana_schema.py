"""
SAP HANA schema models and data structures.

This module contains the data models and structures used to represent
SAP HANA database objects like tables, views, calculation views, and schemas.
"""

import datetime
from typing import List, Optional, Union

from pydantic import BaseModel, Field

from datahub.ingestion.source.common.subtypes import DatasetSubTypes


class HanaColumn(BaseModel):
    """Represents a column in a SAP HANA table or view."""

    name: str
    data_type: str
    description: Optional[str] = None
    comment: Optional[str] = None
    nullable: bool = True
    character_maximum_length: Optional[int] = None
    numeric_precision: Optional[int] = None
    numeric_scale: Optional[int] = None

    def get_precise_native_type(self) -> str:
        """Get the precise native data type including precision and scale."""
        precise_native_type = self.data_type

        # Handle numeric types with precision and scale
        if (
            self.data_type in ("NUMBER", "NUMERIC", "DECIMAL")
            and self.numeric_precision is not None
            and self.numeric_scale is not None
        ):
            precise_native_type = (
                f"DECIMAL({self.numeric_precision},{self.numeric_scale})"
            )
        # Handle string types with length
        elif (
            self.data_type in ("VARCHAR", "NVARCHAR", "ALPHANUM", "SHORTTEXT")
            and self.character_maximum_length is not None
        ):
            precise_native_type = f"{self.data_type}({self.character_maximum_length})"

        return precise_native_type


class HanaTable(BaseModel):
    """Represents a table in SAP HANA."""

    name: str
    schema_name: str
    type: Optional[str] = None
    created: Optional[datetime.datetime] = None
    last_altered: Optional[datetime.datetime] = None
    comment: Optional[str] = None
    rows_count: Optional[int] = None
    size_in_bytes: Optional[int] = None
    columns: List[HanaColumn] = Field(default_factory=list)

    def get_subtype(self) -> DatasetSubTypes:
        return DatasetSubTypes.TABLE


class HanaView(BaseModel):
    """Represents a view in SAP HANA."""

    name: str
    schema_name: str
    materialized: bool = False
    created: Optional[datetime.datetime] = None
    last_altered: Optional[datetime.datetime] = None
    comment: Optional[str] = None
    definition: Optional[str] = None
    view_definition: Optional[str] = None  # Alias for definition
    columns: List[HanaColumn] = Field(default_factory=list)
    view_type: Optional[str] = None

    def get_subtype(self) -> DatasetSubTypes:
        return DatasetSubTypes.VIEW


class HanaCalculationView(BaseModel):
    """Represents a SAP HANA Calculation View."""

    name: str
    package_id: str
    definition: str
    created: Optional[datetime.datetime] = None
    columns: List[HanaColumn] = Field(default_factory=list)

    def get_subtype(self) -> DatasetSubTypes:
        return DatasetSubTypes.VIEW

    @property
    def full_name(self) -> str:
        """Get the full name including package path."""
        return f"{self.package_id}/{self.name}"


class HanaSchema(BaseModel):
    """Represents a schema in SAP HANA."""

    name: str
    created: Optional[datetime.datetime] = None
    last_altered: Optional[datetime.datetime] = None
    comment: Optional[str] = None
    tables: List[str] = Field(default_factory=list)
    views: List[str] = Field(default_factory=list)


class HanaDatabase(BaseModel):
    """Represents a database in SAP HANA."""

    name: str
    created: Optional[datetime.datetime] = None
    comment: Optional[str] = None
    last_altered: Optional[datetime.datetime] = None
    schemas: List[HanaSchema] = Field(default_factory=list)


# Type alias for any HANA dataset
HanaDataset = Union[HanaTable, HanaView, HanaCalculationView]
