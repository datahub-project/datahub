# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

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
    assert FruitConfig.model_validate({}).fruit == Fruit.APPLE
    assert FruitConfig.model_validate({"fruit": "PEAR"}).fruit == Fruit.PEAR
    assert FruitConfig.model_validate({"fruit": "pear"}).fruit == Fruit.PEAR
    assert FruitConfig.model_validate({"fruit": "Orange"}).fruit == Fruit.ORANGE

    # Check that errors are thrown.
    with pytest.raises(pydantic.ValidationError):
        FruitConfig.model_validate({"fruit": "banana"})
