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
from datahub_actions.plugin.transform.filter.filter_transformer import FilterTransformer
from datahub_actions.transform.transformer import Transformer
from datahub_actions.transform.transformer_registry import transformer_registry


def test_registry_nonempty():
    assert len(transformer_registry.mapping) > 0


def test_registry():
    fake_registry = PluginRegistry[Transformer]()
    fake_registry.register("filter", FilterTransformer)

    assert len(fake_registry.mapping) > 0
    assert fake_registry.is_enabled("filter")
    assert fake_registry.get("filter") == FilterTransformer
    assert (
        fake_registry.get(
            "datahub_actions.plugin.transform.filter.filter_transformer.FilterTransformer"
        )
        == FilterTransformer
    )

    # Test lazy-loading capabilities.
    fake_registry.register_lazy(
        "lazy-filter",
        "datahub_actions.plugin.transform.filter.filter_transformer.FilterTransformer",
    )
    assert fake_registry.get("lazy-filter") == FilterTransformer

    # Test Registry Errors
    fake_registry.register_lazy("lazy-error", "thisdoesnot.exist")
    with pytest.raises(ConfigurationError, match="disabled"):
        fake_registry.get("lazy-error")
    with pytest.raises(KeyError, match="special characters"):
        fake_registry.register("thisdoesnotexist.otherthing", FilterTransformer)
    with pytest.raises(KeyError, match="in use"):
        fake_registry.register("filter", FilterTransformer)
    with pytest.raises(KeyError, match="not find"):
        fake_registry.get("thisdoesnotexist")

    # Test error-checking on registered types.
    with pytest.raises(ValueError, match="abstract"):
        fake_registry.register("thisdoesnotexist", Transformer)  # type: ignore

    class DummyClass:  # Does not extend Transformer.
        pass

    with pytest.raises(ValueError, match="derived"):
        fake_registry.register("thisdoesnotexist", DummyClass)  # type: ignore

    # Test disabled Transformer
    fake_registry.register_disabled(
        "disabled", ModuleNotFoundError("disabled transformer")
    )
    fake_registry.register_disabled(
        "disabled-exception", Exception("second disabled transformer")
    )
    with pytest.raises(ConfigurationError, match="disabled"):
        fake_registry.get("disabled")
    with pytest.raises(ConfigurationError, match="disabled"):
        fake_registry.get("disabled-exception")
