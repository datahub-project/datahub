import pytest
from pydantic import ValidationError

from datahub.configuration.common import ConfigModel
from datahub.configuration.validate_field_rename import pydantic_renamed_field


def test_field_rename():
    class TestModel(ConfigModel):
        b: str

        _validate_deprecated = pydantic_renamed_field("a", "b")

    v = TestModel.parse_obj({"b": "original"})
    assert v.b == "original"

    v = TestModel.parse_obj({"a": "renamed"})
    assert v.b == "renamed"

    with pytest.raises(ValidationError):
        TestModel.parse_obj({"a": "foo", "b": "bar"})

    with pytest.raises(ValidationError):
        TestModel.parse_obj({})
