from dataclasses import dataclass
from typing import List, Optional, Set

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.ingestion.source.jdbc.constants import JDBC_TYPE_MAP
from datahub.metadata.schema_classes import (
    ForeignKeyConstraintClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
)


@dataclass
class JDBCColumn:
    """Represents a JDBC column with its metadata."""

    name: str
    type_name: str
    nullable: bool
    remarks: Optional[str]
    column_size: int
    decimal_digits: int

    def to_schema_field(self) -> SchemaFieldClass:
        """Convert JDBC column to DataHub schema field."""
        # Map JDBC type to DataHub type
        type_class = JDBC_TYPE_MAP.get(self.type_name.upper(), StringTypeClass)

        # Add native type parameters
        native_type = self.type_name
        if self.column_size > 0:
            if self.type_name.upper() in ["CHAR", "VARCHAR", "BINARY", "VARBINARY"]:
                native_type = f"{self.type_name}({self.column_size})"
            elif (
                self.type_name.upper() in ["DECIMAL", "NUMERIC"]
                and self.decimal_digits >= 0
            ):
                native_type = (
                    f"{self.type_name}({self.column_size},{self.decimal_digits})"
                )

        return SchemaFieldClass(
            fieldPath=self.name,
            nativeDataType=native_type,
            type=SchemaFieldDataTypeClass(type=type_class()),
            description=self.remarks if self.remarks else None,
            nullable=self.nullable,
        )


@dataclass
class JDBCTable:
    """Represents a JDBC table or view with its metadata."""

    name: str
    type: str
    schema: Optional[str]
    remarks: Optional[str]
    columns: List[JDBCColumn]
    pk_columns: Set[str]
    foreign_keys: List[ForeignKeyConstraintClass]

    @property
    def full_name(self) -> str:
        """Get fully qualified table name."""
        return f"{self.schema}.{self.name}" if self.schema else self.name

    @staticmethod
    def create_foreign_key_constraint(
        name: str,
        source_column: str,
        target_schema: Optional[str],
        target_table: str,
        target_column: str,
        platform: str,
        platform_instance: Optional[str],
        env: str,
    ) -> ForeignKeyConstraintClass:
        """Create a foreign key constraint with proper URN."""
        source_dataset_urn = make_dataset_urn_with_platform_instance(
            platform=platform,
            name=name,
            platform_instance=platform_instance,
            env=env,
        )

        foreign_dataset_urn = make_dataset_urn_with_platform_instance(
            platform=platform,
            name=f"{target_schema}.{target_table}" if target_schema else target_table,
            platform_instance=platform_instance,
            env=env,
        )

        # DataHub expects arrays for source and foreign fields
        source_fields = (
            [
                make_schema_field_urn(
                    parent_urn=source_dataset_urn,
                    field_path=source_column,
                )
            ]
            if isinstance(source_column, str)
            else source_column
        )

        foreign_fields = (
            [
                make_schema_field_urn(
                    parent_urn=foreign_dataset_urn,
                    field_path=target_column,
                )
            ]
            if isinstance(target_column, str)
            else target_column
        )

        return ForeignKeyConstraintClass(
            name=name,
            sourceFields=source_fields,
            foreignDataset=foreign_dataset_urn,
            foreignFields=foreign_fields,
        )


@dataclass
class ViewDefinition:
    """Represents a view definition with its metadata."""

    schema: str
    name: str
    definition: str
    materialized: bool = False
    language: str = "SQL"

    @property
    def full_name(self) -> str:
        """Get fully qualified view name."""
        return f"{self.schema}.{self.name}"


@dataclass
class StoredProcedure:
    """Represents a stored procedure with its metadata."""

    name: str
    schema: str
    remarks: Optional[str]
    proc_type: int
    language: str = "SQL"

    @property
    def full_name(self) -> str:
        """Get fully qualified procedure name."""
        return f"{self.schema}.{self.name}"
