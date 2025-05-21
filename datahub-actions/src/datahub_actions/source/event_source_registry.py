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

from datahub.ingestion.api.registry import PluginRegistry
from datahub_actions.plugin.source.acryl.datahub_cloud_event_source import (
    DataHubEventSource,
)
from datahub_actions.plugin.source.kafka.kafka_event_source import KafkaEventSource
from datahub_actions.source.event_source import EventSource

event_source_registry = PluginRegistry[EventSource]()
event_source_registry.register_from_entrypoint("datahub_actions.source.plugins")
event_source_registry.register("kafka", KafkaEventSource)
event_source_registry.register("datahub-cloud", DataHubEventSource)
