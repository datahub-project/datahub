from datahub.ingestion.source.azure_analysis_services import constants
from datahub.ingestion.source.azure_analysis_services.models import (
    AasColumn,
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
