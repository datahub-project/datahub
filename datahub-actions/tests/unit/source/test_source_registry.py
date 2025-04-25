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
from datahub_actions.plugin.source.kafka.kafka_event_source import KafkaEventSource
from datahub_actions.source.event_source import EventSource
from datahub_actions.source.event_source_registry import event_source_registry


def test_registry_nonempty():
    assert len(event_source_registry.mapping) > 0


def test_registry():
    fake_registry = PluginRegistry[EventSource]()
    fake_registry.register("kafka", KafkaEventSource)

    assert len(fake_registry.mapping) > 0
    assert fake_registry.is_enabled("kafka")
    assert fake_registry.get("kafka") == KafkaEventSource
    assert (
        fake_registry.get(
            "datahub_actions.plugin.source.kafka.kafka_event_source.KafkaEventSource"
        )
        == KafkaEventSource
    )

    # Test lazy-loading capabilities.
    fake_registry.register_lazy(
        "lazy-kafka",
        "datahub_actions.plugin.source.kafka.kafka_event_source:KafkaEventSource",
    )
    assert fake_registry.get("lazy-kafka") == KafkaEventSource

    # Test Registry Errors
    fake_registry.register_lazy("lazy-error", "thisdoesnot.exist")
    with pytest.raises(ConfigurationError, match="disabled"):
        fake_registry.get("lazy-error")
    with pytest.raises(KeyError, match="special characters"):
        fake_registry.register("thisdoesnotexist.otherthing", KafkaEventSource)
    with pytest.raises(KeyError, match="in use"):
        fake_registry.register("kafka", KafkaEventSource)
    with pytest.raises(KeyError, match="not find"):
        fake_registry.get("thisdoesnotexist")

    # Test error-checking on registered types.
    with pytest.raises(ValueError, match="abstract"):
        fake_registry.register("thisdoesnotexist", EventSource)  # type: ignore

    class DummyClass:  # Does not extend Event source.
        pass

    with pytest.raises(ValueError, match="derived"):
        fake_registry.register("thisdoesnotexist", DummyClass)  # type: ignore

    # Test disabled event source
    fake_registry.register_disabled(
        "disabled", ModuleNotFoundError("disabled event source")
    )
    fake_registry.register_disabled(
        "disabled-exception", Exception("second disabled event source")
    )
    with pytest.raises(ConfigurationError, match="disabled"):
        fake_registry.get("disabled")
    with pytest.raises(ConfigurationError, match="disabled"):
        fake_registry.get("disabled-exception")
