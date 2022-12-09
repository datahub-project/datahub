from typing import List

import pydantic
import pytest

from datahub.configuration.common import ConfigModel


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
