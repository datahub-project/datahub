from datahub.ingestion.source.azure_analysis_services import constants
from datahub.ingestion.source.azure_analysis_services.models import (
    AasColumn,
    AasColumnRow,
    databricks_catalog_and_schema,
    tom_data_type_to_datahub,
)
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    StringTypeClass,
)


def test_tom_data_type_mapping() -> None:
    assert isinstance(
        tom_data_type_to_datahub(constants.TomDataType.STRING), StringTypeClass
    )
    assert isinstance(
        tom_data_type_to_datahub(constants.TomDataType.INT64), NumberTypeClass
    )
    assert isinstance(
        tom_data_type_to_datahub(constants.TomDataType.DECIMAL), NumberTypeClass
    )
    assert isinstance(
        tom_data_type_to_datahub(constants.TomDataType.BOOLEAN), BooleanTypeClass
    )
    assert isinstance(
        tom_data_type_to_datahub(constants.TomDataType.DATETIME), DateTypeClass
    )


def test_tom_data_type_unknown_falls_back_to_null() -> None:
    # An unmodelled TOM code and a blank type both degrade to NullType so the
    # field still emits rather than being dropped.
    assert isinstance(tom_data_type_to_datahub(999), NullTypeClass)
    assert isinstance(tom_data_type_to_datahub(None), NullTypeClass)


def test_resolved_data_type_prefers_concrete_explicit() -> None:
    # A regular/calculated column reports a concrete ExplicitDataType, which wins
    # over InferredDataType.
    row = AasColumnRow(
        id=1,
        table_id=1,
        explicit_data_type=constants.TomDataType.DOUBLE,
        inferred_data_type=constants.TomDataType.INT64,
    )
    assert row.resolved_data_type == constants.TomDataType.DOUBLE


def test_resolved_data_type_automatic_falls_through_to_inferred() -> None:
    # A calculated-table column reports Automatic in ExplicitDataType; its real
    # type lives in InferredDataType.
    row = AasColumnRow(
        id=1,
        table_id=1,
        explicit_data_type=constants.TomDataType.AUTOMATIC,
        inferred_data_type=constants.TomDataType.STRING,
    )
    assert row.resolved_data_type == constants.TomDataType.STRING


def test_resolved_data_type_automatic_without_inferred_is_none() -> None:
    # Automatic with no inferred type has nothing to resolve to; the mapper then
    # emits NullType rather than a wrong type.
    row = AasColumnRow(
        id=1,
        table_id=1,
        explicit_data_type=constants.TomDataType.AUTOMATIC,
        inferred_data_type=None,
    )
    assert row.resolved_data_type is None


def test_column_datatype_name_unknown_fallback() -> None:
    known = AasColumn(
        name="Amount",
        data_type=constants.TomDataType.INT64,
        datahub_data_type=NumberTypeClass(),
    )
    assert known.dataType == "Int64"

    unknown = AasColumn(
        name="Mystery", data_type=None, datahub_data_type=NullTypeClass()
    )
    assert unknown.dataType == "Unknown"


_DBX = (
    "Host=adb-1.azuredatabricks.net;HTTPPath=/sql/1.0/warehouses/abc;"
    "UID=token;PWD={sek;ret};Catalog=prod;Schema=finance"
)


def test_databricks_connection_yields_catalog_and_schema() -> None:
    assert databricks_catalog_and_schema(_DBX) == ("prod", "finance")


def test_databricks_database_key_is_the_schema() -> None:
    connection = "Provider=Databricks;Host=adb-1.azuredatabricks.net;Catalog=prod;Database=finance"
    assert databricks_catalog_and_schema(connection) == ("prod", "finance")


def test_sql_server_connection_is_not_a_databricks_catalog() -> None:
    connection = "Server=tcp:sql.example;Initial Catalog=salesdb;PWD=secret"
    assert databricks_catalog_and_schema(connection) == (None, None)


def test_dsn_connection_with_catalog_and_schema() -> None:
    # AAS often returns a DSN plus Catalog and Schema, with the host in the DSN.
    connection = "DSN=warehouse;Catalog=prod;Schema=finance"
    assert databricks_catalog_and_schema(connection) == ("prod", "finance")
