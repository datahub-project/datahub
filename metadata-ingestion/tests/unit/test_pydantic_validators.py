from typing import Optional

import pytest
from pydantic import ValidationError

from datahub.configuration.common import ConfigModel
from datahub.configuration.pydantic_field_deprecation import pydantic_field_deprecated
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.utilities.global_warning_util import get_global_warnings


def test_field_rename():
    class TestModel(ConfigModel):
        b: str

        _validate_rename = pydantic_renamed_field("a", "b")

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


def test_field_remove():
    class TestModel(ConfigModel):
        b: str

        _validate_removed_r1 = pydantic_removed_field("r1")
        _validate_removed_r2 = pydantic_removed_field("r2")

    v = TestModel.parse_obj({"b": "original"})
    assert v.b == "original"

    v = TestModel.parse_obj({"b": "original", "r1": "removed", "r2": "removed"})
    assert v.b == "original"


def test_field_deprecated():
    class TestModel(ConfigModel):
        d1: Optional[str]
        d2: Optional[str]
        b: str

        _validate_deprecated_d1 = pydantic_field_deprecated("d1")
        _validate_deprecated_d2 = pydantic_field_deprecated("d2")

    v = TestModel.parse_obj({"b": "original"})
    assert v.b == "original"

    v = TestModel.parse_obj({"b": "original", "d1": "deprecated", "d2": "deprecated"})
    assert v.b == "original"
    assert v.d1 == "deprecated"
    assert v.d2 == "deprecated"
    assert any(["d1 is deprecated" in warning for warning in get_global_warnings()])
    assert any(["d2 is deprecated" in warning for warning in get_global_warnings()])
