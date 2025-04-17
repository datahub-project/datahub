# Copyright 2021 Acryl Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.api.registry import PluginRegistry
from datahub_actions.action.action import Action
from datahub_actions.action.action_registry import action_registry
from datahub_actions.plugin.action.hello_world.hello_world import HelloWorldAction


def test_registry_nonempty():
    assert len(action_registry.mapping) > 0


def test_registry():
    fake_registry = PluginRegistry[Action]()
    fake_registry.register("hello_world", HelloWorldAction)

    assert len(fake_registry.mapping) > 0
    assert fake_registry.is_enabled("hello_world")
    assert fake_registry.get("hello_world") == HelloWorldAction
    assert (
        fake_registry.get(
            "datahub_actions.plugin.action.hello_world.hello_world.HelloWorldAction"
        )
        == HelloWorldAction
    )

    # Test lazy-loading capabilities.
    fake_registry.register_lazy(
        "lazy-hello-world",
        "datahub_actions.plugin.action.hello_world.hello_world:HelloWorldAction",
    )
    assert fake_registry.get("lazy-hello-world") == HelloWorldAction

    # Test Registry Errors
    fake_registry.register_lazy("lazy-error", "thisdoesnot.exist")
    with pytest.raises(ConfigurationError, match="disabled"):
        fake_registry.get("lazy-error")
    with pytest.raises(KeyError, match="special characters"):
        fake_registry.register("thisdoesnotexist.otherthing", HelloWorldAction)
    with pytest.raises(KeyError, match="in use"):
        fake_registry.register("hello_world", HelloWorldAction)
    with pytest.raises(KeyError, match="not find"):
        fake_registry.get("thisdoesnotexist")

    # Test error-checking on registered types.
    with pytest.raises(ValueError, match="abstract"):
        fake_registry.register("thisdoesnotexist", Action)  # type: ignore

    class DummyClass:  # Does not extend Action.
        pass

    with pytest.raises(ValueError, match="derived"):
        fake_registry.register("thisdoesnotexist", DummyClass)  # type: ignore

    # Test disabled actions
    fake_registry.register_disabled("disabled", ModuleNotFoundError("disabled action"))
    fake_registry.register_disabled(
        "disabled-exception", Exception("second disabled action")
    )
    with pytest.raises(ConfigurationError, match="disabled"):
        fake_registry.get("disabled")
    with pytest.raises(ConfigurationError, match="disabled"):
        fake_registry.get("disabled-exception")
