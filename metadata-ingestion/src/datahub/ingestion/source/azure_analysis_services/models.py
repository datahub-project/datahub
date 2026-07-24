from typing import List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from datahub.ingestion.source.azure_analysis_services import constants
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    StringTypeClass,
)

DataHubFieldType = Union[
    BooleanTypeClass, DateTypeClass, NullTypeClass, NumberTypeClass, StringTypeClass
]


def tom_data_type_to_datahub(data_type: Optional[int]) -> DataHubFieldType:
    # Maps a Tabular Object Model DataType enumeration value to the DataHub
    # schema-field type. Unknown/blank types fall back to NullType so the field
    # still emits rather than being dropped.
    if data_type == constants.TOM_DATA_TYPE_STRING:
        return StringTypeClass()
    if data_type in (
        constants.TOM_DATA_TYPE_INT64,
        constants.TOM_DATA_TYPE_DOUBLE,
        constants.TOM_DATA_TYPE_DECIMAL,
    ):
        return NumberTypeClass()
    if data_type == constants.TOM_DATA_TYPE_BOOLEAN:
        return BooleanTypeClass()
    if data_type == constants.TOM_DATA_TYPE_DATETIME:
        return DateTypeClass()
    return NullTypeClass()


TOM_DATA_TYPE_NAMES = {
    constants.TOM_DATA_TYPE_STRING: "String",
    constants.TOM_DATA_TYPE_INT64: "Int64",
    constants.TOM_DATA_TYPE_DOUBLE: "Double",
    constants.TOM_DATA_TYPE_DECIMAL: "Decimal",
    constants.TOM_DATA_TYPE_BOOLEAN: "Boolean",
    constants.TOM_DATA_TYPE_DATETIME: "DateTime",
}


class AasRow(BaseModel):
    # DMV rowsets return columns by their TMSCHEMA property name. populate_by_name
    # lets us address them by alias; extra='ignore' tolerates engine-version
    # columns we do not model.
    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class AasModelRow(AasRow):
    id: int = Field(alias="ID")
    name: str = Field(alias="Name")
    description: Optional[str] = Field(default=None, alias="Description")
    culture: Optional[str] = Field(default=None, alias="Culture")
    default_mode: Optional[int] = Field(default=None, alias="DefaultMode")


class AasTableRow(AasRow):
    id: int = Field(alias="ID")
    model_id: Optional[int] = Field(default=None, alias="ModelID")
    name: str = Field(alias="Name")
    description: Optional[str] = Field(default=None, alias="Description")
    is_hidden: bool = Field(default=False, alias="IsHidden")
    data_category: Optional[str] = Field(default=None, alias="DataCategory")


class AasColumnRow(AasRow):
    id: int = Field(alias="ID")
    table_id: int = Field(alias="TableID")
    explicit_name: Optional[str] = Field(default=None, alias="ExplicitName")
    inferred_name: Optional[str] = Field(default=None, alias="InferredName")
    explicit_data_type: Optional[int] = Field(default=None, alias="ExplicitDataType")
    inferred_data_type: Optional[int] = Field(default=None, alias="InferredDataType")
    column_type: Optional[int] = Field(default=None, alias="Type")
    expression: Optional[str] = Field(default=None, alias="Expression")
    description: Optional[str] = Field(default=None, alias="Description")
    is_hidden: bool = Field(default=False, alias="IsHidden")
    display_folder: Optional[str] = Field(default=None, alias="DisplayFolder")

    @property
    def resolved_name(self) -> Optional[str]:
        return self.explicit_name or self.inferred_name

    @property
    def resolved_data_type(self) -> Optional[int]:
        return (
            self.explicit_data_type
            if self.explicit_data_type is not None
            else self.inferred_data_type
        )


class AasMeasureRow(AasRow):
    id: int = Field(alias="ID")
    table_id: int = Field(alias="TableID")
    name: str = Field(alias="Name")
    expression: Optional[str] = Field(default=None, alias="Expression")
    description: Optional[str] = Field(default=None, alias="Description")
    format_string: Optional[str] = Field(default=None, alias="FormatString")
    display_folder: Optional[str] = Field(default=None, alias="DisplayFolder")
    is_hidden: bool = Field(default=False, alias="IsHidden")


class AasPartitionRow(AasRow):
    id: int = Field(alias="ID")
    table_id: int = Field(alias="TableID")
    name: Optional[str] = Field(default=None, alias="Name")
    query_definition: Optional[str] = Field(default=None, alias="QueryDefinition")
    partition_type: Optional[int] = Field(default=None, alias="Type")
    mode: Optional[int] = Field(default=None, alias="Mode")
    data_source_id: Optional[int] = Field(default=None, alias="DataSourceID")


class AasRelationshipRow(AasRow):
    id: int = Field(alias="ID")
    name: Optional[str] = Field(default=None, alias="Name")
    is_active: bool = Field(default=True, alias="IsActive")
    from_table_id: int = Field(alias="FromTableID")
    from_column_id: int = Field(alias="FromColumnID")
    to_table_id: int = Field(alias="ToTableID")
    to_column_id: int = Field(alias="ToColumnID")
    cross_filtering_behavior: Optional[int] = Field(
        default=None, alias="CrossFilteringBehavior"
    )


class AasRoleRow(AasRow):
    id: int = Field(alias="ID")
    name: str = Field(alias="Name")
    description: Optional[str] = Field(default=None, alias="Description")
    model_permission: Optional[int] = Field(default=None, alias="ModelPermission")


class AasDataSourceRow(AasRow):
    id: int = Field(alias="ID")
    name: str = Field(alias="Name")
    data_source_type: Optional[int] = Field(default=None, alias="Type")
    connection_string: Optional[str] = Field(default=None, alias="ConnectionString")
    description: Optional[str] = Field(default=None, alias="Description")


class AasCalcDependencyRow(AasRow):
    # DISCOVER_CALC_DEPENDENCY exposes intra-model dependencies: which measure /
    # calculated column / calculated table references which other object.
    database_name: Optional[str] = Field(default=None, alias="DATABASE_NAME")
    object_type: Optional[str] = Field(default=None, alias="OBJECT_TYPE")
    table: Optional[str] = Field(default=None, alias="TABLE")
    object_name: Optional[str] = Field(default=None, alias="OBJECT")
    expression: Optional[str] = Field(default=None, alias="EXPRESSION")
    referenced_object_type: Optional[str] = Field(
        default=None, alias="REFERENCED_OBJECT_TYPE"
    )
    referenced_table: Optional[str] = Field(default=None, alias="REFERENCED_TABLE")
    referenced_object: Optional[str] = Field(default=None, alias="REFERENCED_OBJECT")


# --- Resolved domain models -----------------------------------------------
# Assembled from the raw rows with foreign keys resolved. The column/table
# shapes intentionally satisfy the shared M-Query engine protocols
# (MQueryColumn / MQueryTable) so lineage can be driven without an adapter.


class AasColumn(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    data_type: int = 0
    datahub_data_type: DataHubFieldType
    is_calculated: bool = False
    expression: Optional[str] = None
    description: Optional[str] = None
    is_hidden: bool = False
    display_folder: Optional[str] = None

    # Aliases required by the shared M-Query engine's MQueryColumn protocol.
    @property
    def dataType(self) -> str:
        return TOM_DATA_TYPE_NAMES.get(self.data_type, "Unknown")

    @property
    def datahubDataType(self) -> DataHubFieldType:
        return self.datahub_data_type


class AasMeasure(BaseModel):
    name: str
    expression: Optional[str] = None
    description: Optional[str] = None
    format_string: Optional[str] = None
    display_folder: Optional[str] = None
    is_hidden: bool = False


class AasPartition(BaseModel):
    name: Optional[str] = None
    query_definition: Optional[str] = None
    partition_type: Optional[int] = None
    data_source_id: Optional[int] = None


class AasTable(BaseModel):
    name: str
    description: Optional[str] = None
    is_hidden: bool = False
    is_calculated: bool = False
    columns: List[AasColumn] = Field(default_factory=list)
    measures: List[AasMeasure] = Field(default_factory=list)
    partitions: List[AasPartition] = Field(default_factory=list)

    # ``full_name`` and ``expression`` satisfy the MQueryTable protocol.
    @property
    def full_name(self) -> str:
        return self.name

    @property
    def expression(self) -> Optional[str]:
        # The M/Power Query expression the shared engine parses for lineage.
        # A table's first query partition carries it; calculated tables have
        # a DAX expression instead, which the engine correctly ignores.
        for partition in self.partitions:
            if partition.query_definition:
                return partition.query_definition
        return None


class AasRelationship(BaseModel):
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    is_active: bool = True


class AasRole(BaseModel):
    name: str
    description: Optional[str] = None
    model_permission: Optional[int] = None


class AasDataSource(BaseModel):
    name: str
    connection_string: Optional[str] = None


class AasTabularModel(BaseModel):
    catalog: str
    name: str
    description: Optional[str] = None
    culture: Optional[str] = None
    tables: List[AasTable] = Field(default_factory=list)
    relationships: List[AasRelationship] = Field(default_factory=list)
    roles: List[AasRole] = Field(default_factory=list)
    data_sources: List[AasDataSource] = Field(default_factory=list)
    calc_dependencies: List[AasCalcDependencyRow] = Field(default_factory=list)
    # Full TMSL/TMDL model definition (DISCOVER_XML_METADATA), attached to the
    # model-level cube dataset's ViewProperties when extraction is enabled.
    definition: Optional[str] = None
