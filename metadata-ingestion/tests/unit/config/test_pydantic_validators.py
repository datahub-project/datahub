from typing import Optional

import pydantic
import pytest
from pydantic import ValidationError

from datahub.configuration.common import ConfigModel, ConfigurationWarning
from datahub.configuration.validate_field_deprecation import pydantic_field_deprecated
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.configuration.validate_multiline_string import pydantic_multiline_string
from datahub.utilities.global_warning_util import (
    clear_global_warnings,
    get_global_warnings,
)


def test_field_rename():
    class TestModel(ConfigModel):
        b: str

        _validate_rename = pydantic_renamed_field("a", "b")

    v = TestModel.parse_obj({"b": "original"})
    assert v.b == "original"

    with pytest.warns(ConfigurationWarning, match="a is deprecated"):
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

    with pytest.warns(ConfigurationWarning, match=r"a.* is deprecated"):
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

    with pytest.warns(ConfigurationWarning, match=r"r\d was removed"):
        v = TestModel.parse_obj({"b": "original", "r1": "removed", "r2": "removed"})
        assert v.b == "original"


def test_field_deprecated():
    clear_global_warnings()

    class TestModel(ConfigModel):
        d1: Optional[str] = None
        d2: Optional[str] = None
        b: str

        _validate_deprecated_d1 = pydantic_field_deprecated("d1")
        _validate_deprecated_d2 = pydantic_field_deprecated("d2")

    v = TestModel.parse_obj({"b": "original"})
    assert v.b == "original"

    with pytest.warns(ConfigurationWarning, match=r"d\d.+ deprecated"):
        v = TestModel.parse_obj(
            {"b": "original", "d1": "deprecated", "d2": "deprecated"}
        )
    assert v.b == "original"
    assert v.d1 == "deprecated"
    assert v.d2 == "deprecated"
    assert any(["d1 is deprecated" in warning for warning in get_global_warnings()])
    assert any(["d2 is deprecated" in warning for warning in get_global_warnings()])

    clear_global_warnings()


def test_multiline_string_fixer():
    class TestModel(ConfigModel):
        s: str
        m: Optional[pydantic.SecretStr] = None

        _validate_s = pydantic_multiline_string("s")
        _validate_m = pydantic_multiline_string("m")

    v = TestModel.parse_obj({"s": "foo\nbar"})
    assert v.s == "foo\nbar"

    v = TestModel.parse_obj({"s": "foo\\nbar"})
    assert v.s == "foo\nbar"

    v = TestModel.parse_obj({"s": "normal", "m": "foo\\nbar"})
    assert v.s == "normal"
    assert v.m
    assert v.m.get_secret_value() == "foo\nbar"

    v = TestModel.parse_obj({"s": "normal", "m": pydantic.SecretStr("foo\\nbar")})
    assert v.m
    assert v.m.get_secret_value() == "foo\nbar"
