from typing import List

import pydantic
import pytest

from datahub.configuration.common import (
    AllowDenyPattern,
    ConfigModel,
    redact_raw_config,
)


def test_extras_not_allowed():
    class MyConfig(ConfigModel):
        required: str
        optional: str = "bar"

    MyConfig.parse_obj({"required": "foo"})
    MyConfig.parse_obj({"required": "foo", "optional": "baz"})

    with pytest.raises(pydantic.ValidationError):
        MyConfig.parse_obj({"required": "foo", "extra": "extra"})


def test_extras_allowed():
    class MyConfig(ConfigModel):
        required: str
        optional: str = "bar"

    MyConfig.parse_obj_allow_extras({"required": "foo"})
    MyConfig.parse_obj_allow_extras({"required": "foo", "optional": "baz"})
    MyConfig.parse_obj_allow_extras({"required": "foo", "extra": "extra"})


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
