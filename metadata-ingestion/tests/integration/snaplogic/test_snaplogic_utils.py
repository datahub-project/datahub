import pytest

from datahub.ingestion.source.snaplogic.snaplogic_utils import SnaplogicUtils
from datahub.metadata._internal_schema_classes import (
    BooleanTypeClass,
    NumberTypeClass,
    StringTypeClass,
)
from datahub.metadata.schema_classes import (
    SchemaFieldDataTypeClass,
)


@pytest.mark.parametrize(
    "input_type, expected_class",
    [
        ("string", StringTypeClass),
        ("varchar", StringTypeClass),
        ("int", NumberTypeClass),
        ("long", NumberTypeClass),
        ("float", NumberTypeClass),
        ("double", NumberTypeClass),
        ("number", NumberTypeClass),
        ("boolean", BooleanTypeClass),
        ("unknown", StringTypeClass),  # default fallback
        ("STRING", StringTypeClass),  # test case-insensitivity
        ("Int", NumberTypeClass),
        ("BOOLEAN", BooleanTypeClass),
    ],
)
def test_get_datahub_type(input_type, expected_class):
    result = SnaplogicUtils.get_datahub_type(input_type)
    assert isinstance(result, SchemaFieldDataTypeClass)
    assert isinstance(result.type, expected_class)
