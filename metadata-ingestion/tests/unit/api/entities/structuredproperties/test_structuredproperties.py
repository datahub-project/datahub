import pathlib

import pydantic
import pytest

from datahub.api.entities.structuredproperties.structuredproperties import (
    StructuredProperties,
    TypeQualifierAllowedTypes,
)
from tests.test_helpers.mce_helpers import check_goldens_stream

RESOURCE_DIR = pathlib.Path(__file__).parent


def test_type_validation() -> None:
    with pytest.raises(pydantic.ValidationError):
        TypeQualifierAllowedTypes(allowed_types=["thisdoesnotexist"])

    types = TypeQualifierAllowedTypes(allowed_types=["dataset"])
    assert types.allowed_types == ["urn:li:entityType:datahub.dataset"]


def test_structuredproperties_load(pytestconfig: pytest.Config) -> None:
    example_properties_file = (
        pytestconfig.rootpath
        / "examples/structured_properties/structured_properties.yaml"
    )

    properties = StructuredProperties.from_yaml(str(example_properties_file))
    mcps = []
    for property in properties:
        mcps.extend(property.generate_mcps())

    check_goldens_stream(
        mcps,
        golden_path=RESOURCE_DIR / "example_structured_properties_golden.json",
    )
