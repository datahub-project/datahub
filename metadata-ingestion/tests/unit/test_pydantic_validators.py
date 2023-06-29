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


def test_field_multiple_fields_rename():
    class TestModel(ConfigModel):
        b: str
        b1: str

        _validate_deprecated = pydantic_renamed_field("a", "b")
        _validate_deprecated1 = pydantic_renamed_field("a1", "b1")

    v = TestModel.parse_obj({"b": "original", "b1": "original"})
    assert v.b == "original"
    assert v.b1 == "original"

    v = TestModel.parse_obj({"a": "renamed", "a1": "renamed"})
    assert v.b == "renamed"
    assert v.b1 == "renamed"

    with pytest.raises(ValidationError):
        TestModel.parse_obj({"a": "foo", "b": "bar", "b1": "ok"})

    with pytest.raises(ValidationError):
        TestModel.parse_obj({"a1": "foo", "b1": "bar", "b": "ok"})

    with pytest.raises(ValidationError):
        TestModel.parse_obj({"b": "foo"})

    with pytest.raises(ValidationError):
        TestModel.parse_obj({"b1": "foo"})

    with pytest.raises(ValidationError):
        TestModel.parse_obj({})
