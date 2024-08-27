import enum

import pydantic
import pytest

from datahub.configuration.common import ConfigEnum, ConfigModel


def test_config_enum():
    class Fruit(ConfigEnum):
        APPLE = enum.auto()
        ORANGE = "ORANGE"
        PEAR = enum.auto()

    class FruitConfig(ConfigModel):
        fruit: Fruit = Fruit.APPLE

    # Test auto-generated and manual enum values.
    assert Fruit.APPLE.value == "APPLE"
    assert Fruit.ORANGE.value == "ORANGE"

    # Check that config loading works.
    assert FruitConfig.parse_obj({}).fruit == Fruit.APPLE
    assert FruitConfig.parse_obj({"fruit": "PEAR"}).fruit == Fruit.PEAR
    assert FruitConfig.parse_obj({"fruit": "pear"}).fruit == Fruit.PEAR
    assert FruitConfig.parse_obj({"fruit": "Orange"}).fruit == Fruit.ORANGE

    # Check that errors are thrown.
    with pytest.raises(pydantic.ValidationError):
        FruitConfig.parse_obj({"fruit": "banana"})
