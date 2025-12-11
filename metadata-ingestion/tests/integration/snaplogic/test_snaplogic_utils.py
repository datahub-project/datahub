# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import pytest

from datahub.ingestion.source.snaplogic.snaplogic_utils import SnaplogicUtils
from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    NumberTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
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
