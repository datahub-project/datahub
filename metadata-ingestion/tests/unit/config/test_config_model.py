from typing import List

import pydantic
import pytest
from pydantic import AliasChoices, Field

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    redact_raw_config,
)


def test_extras_not_allowed():
    class MyConfig(ConfigModel):
        required: str
        optional: str = "bar"

    MyConfig.model_validate({"required": "foo"})
    MyConfig.model_validate({"required": "foo", "optional": "baz"})

    with pytest.raises(pydantic.ValidationError):
        MyConfig.model_validate({"required": "foo", "extra": "extra"})


def test_extras_allowed():
    class MyConfig(ConfigModel):
        required: str
        optional: str = "bar"

    MyConfig.parse_obj_allow_extras({"required": "foo"})
    MyConfig.parse_obj_allow_extras({"required": "foo", "optional": "baz"})
    MyConfig.parse_obj_allow_extras({"required": "foo", "extra": "extra"})


def test_parse_obj_allow_extras_strips_unknown_fields():
    """Extras should be silently dropped, not stored on the model."""

    class MyConfig(ConfigModel):
        required: str

    config = MyConfig.parse_obj_allow_extras(
        {"required": "foo", "unknown1": "a", "unknown2": 42}
    )
    assert config.required == "foo"
    assert not hasattr(config, "unknown1")
    assert not hasattr(config, "unknown2")


def test_parse_obj_allow_extras_with_non_dict():
    """Non-dict input should be passed through to model_validate as-is."""

    class MyConfig(ConfigModel):
        required: str

    config = MyConfig.parse_obj_allow_extras(MyConfig(required="foo"))
    assert config.required == "foo"


def test_parse_obj_allow_extras_multi_inheritance():
    """parse_obj_allow_extras must work with deep multi-inheritance.

    This is the scenario that caused SourceConnectionErrorException in
    datahub-executor: parent classes like GcsDatasetLineageProviderConfigBase
    have extra="forbid" and only define a subset of the child's fields.
    The method must recognize fields from ALL parents in the MRO.
    """

    class ParentA(ConfigModel):
        field_a: str = "a"

    class ParentB(ConfigModel):
        field_b: str = "b"

    class ParentC(ConfigModel):
        field_c: str = "c"

    class Child(ParentA, ParentB, ParentC):
        field_child: str = "child"

    # All parent fields should be recognized; extras should be stripped
    config = Child.parse_obj_allow_extras(
        {
            "field_a": "A",
            "field_b": "B",
            "field_c": "C",
            "field_child": "CHILD",
            "extra_field": "should_be_stripped",
        }
    )
    assert config.field_a == "A"
    assert config.field_b == "B"
    assert config.field_c == "C"
    assert config.field_child == "CHILD"
    assert not hasattr(config, "extra_field")


def test_parse_obj_allow_extras_diamond_inheritance():
    """Diamond inheritance: multiple parents share a common ancestor."""

    class Base(ConfigModel):
        base_field: str = "base"

    class Left(Base):
        left_field: str = "left"

    class Right(Base):
        right_field: str = "right"

    class Diamond(Left, Right):
        diamond_field: str = "diamond"

    config = Diamond.parse_obj_allow_extras(
        {
            "base_field": "BASE",
            "left_field": "LEFT",
            "right_field": "RIGHT",
            "diamond_field": "DIAMOND",
            "unknown": "stripped",
        }
    )
    assert config.base_field == "BASE"
    assert config.left_field == "LEFT"
    assert config.right_field == "RIGHT"
    assert config.diamond_field == "DIAMOND"


def test_parse_obj_allow_extras_with_aliases():
    """Fields with alias or validation_alias should be recognized by their alias names."""

    class AliasConfig(ConfigModel):
        normal_field: str
        aliased: str = Field(default="x", alias="aliasName")
        val_aliased: str = Field(default="y", validation_alias="valAliasName")

    # Alias names should be kept; extras should be stripped
    config = AliasConfig.parse_obj_allow_extras(
        {"normal_field": "n", "aliasName": "A", "valAliasName": "V", "extra": "gone"}
    )
    assert config.normal_field == "n"
    assert config.aliased == "A"
    assert config.val_aliased == "V"


def test_parse_obj_allow_extras_with_alias_choices():
    """AliasChoices validation aliases must be handled correctly.

    The key-stripping approach failed here because isinstance(AliasChoices, str)
    is False, so alternate alias names were incorrectly stripped from the dict.
    """

    class ChoicesConfig(ConfigModel):
        pattern: str = Field(
            default="default",
            validation_alias=AliasChoices("pattern", "dataset_pattern"),
        )

    # Primary alias
    config = ChoicesConfig.parse_obj_allow_extras(
        {"pattern": "primary", "extra": "gone"}
    )
    assert config.pattern == "primary"

    # Alternate alias
    config2 = ChoicesConfig.parse_obj_allow_extras(
        {"dataset_pattern": "alternate", "extra": "gone"}
    )
    assert config2.pattern == "alternate"


def test_parse_obj_allow_extras_does_not_corrupt_class():
    """Calling parse_obj_allow_extras must not affect subsequent normal validation.

    The old model_rebuild approach could corrupt shared class state. Verify that
    normal validation still rejects extras after parse_obj_allow_extras is called.
    """

    class MyConfig(ConfigModel):
        field: str

    # First call with extras
    MyConfig.parse_obj_allow_extras({"field": "ok", "extra": "stripped"})

    # Normal validation must still reject extras
    with pytest.raises(pydantic.ValidationError, match="extra"):
        MyConfig.model_validate({"field": "ok", "extra": "should_fail"})


def test_default_object_copy():
    # Doing this with dataclasses would yield a subtle bug: the default list
    # objects would be shared between instances. However, pydantic is smart
    # enough to copy the object when it's used as a default value.

    class MyConfig(ConfigModel):
        items: List[str] = []

        items_field: List[str] = pydantic.Field(
            default=[],
            description="A list of items",
        )

    config_1 = MyConfig()
    config_2 = MyConfig()

    config_1.items.append("foo")
    config_1.items_field.append("foo")

    assert config_2.items == []
    assert config_2.items_field == []


def test_config_redaction():
    obj = {
        "config": {
            "password": "this_is_sensitive",
            "aws_key_id": "${AWS_KEY_ID}",
            "projects": ["default"],
            "options": {},
        },
        "options": {"foo": "bar"},
    }

    redacted = redact_raw_config(obj)
    assert redacted == {
        "config": {
            "password": "********",
            "aws_key_id": "${AWS_KEY_ID}",
            "projects": ["default"],
            "options": {},
        },
        "options": "********",
    }


def test_config_redaction_2():
    obj = {
        "config": {
            "catalog": {
                "config": {
                    "s3.access-key-id": "ABCDEF",
                    "s3.secret-access-key": "8126818",
                }
            }
        },
    }

    redacted = redact_raw_config(obj)
    assert redacted == {
        "config": {
            "catalog": {
                "config": {
                    "s3.access-key-id": "********",
                    "s3.secret-access-key": "********",
                }
            }
        },
    }


def test_shared_defaults():
    class SourceConfig(ConfigModel):
        token: str
        workspace_url: str
        catalog_pattern: AllowDenyPattern = pydantic.Field(
            default=AllowDenyPattern.allow_all(),
        )

    c1 = SourceConfig(token="s", workspace_url="https://workspace_url")
    c2 = SourceConfig(token="s", workspace_url="https://workspace_url")

    assert c2.catalog_pattern.allow == [".*"]
    c1.catalog_pattern.allow += ["foo"]
    assert c2.catalog_pattern.allow == [".*"]
