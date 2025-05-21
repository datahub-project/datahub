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

import json

from datahub.ingestion.api.registry import PluginRegistry
from datahub.metadata.schema_classes import (
    EntityChangeEventClass,
    MetadataChangeLogClass,
)
from datahub_actions.event.event import Event

# TODO: Figure out where to put these.
# TODO: Perform runtime validation based on the event types found in the registry.


# A DataHub Event representing a Metadata Change Log Event.
# See MetadataChangeLogEvent class object for full field set.
class MetadataChangeLogEvent(MetadataChangeLogClass, Event):
    @classmethod
    def from_class(cls, clazz: MetadataChangeLogClass) -> "MetadataChangeLogEvent":
        instance = cls._construct({})
        instance._restore_defaults()
        # Shallow map inner dictionaries.
        instance._inner_dict = clazz._inner_dict
        return instance

    @classmethod
    def from_json(cls, json_str: str) -> "Event":
        json_obj = json.loads(json_str)
        return cls.from_class(cls.from_obj(json_obj))

    def as_json(self) -> str:
        return json.dumps(self.to_obj())


# A DataHub Event representing an Entity Change Event.
# See EntityChangeEventClass class object for full field set.
class EntityChangeEvent(EntityChangeEventClass, Event):
    @classmethod
    def from_class(cls, clazz: EntityChangeEventClass) -> "EntityChangeEvent":
        instance = cls._construct({})
        instance._restore_defaults()
        # Shallow map inner dictionaries.
        instance._inner_dict = clazz._inner_dict
        return instance

    @classmethod
    def from_json(cls, json_str: str) -> "EntityChangeEvent":
        json_obj = json.loads(json_str)

        # Remove parameters from json_obj and add it later to _inner_dict, this hack exists because of the way EntityChangeLogClass does not support "AnyRecord"
        parameters = json_obj.pop("parameters", None)

        event = cls.from_class(cls.from_obj(json_obj))

        # Hack: Since parameters is an "AnyRecord" (arbitrary json) we have to insert into the underlying map directly
        # to avoid validation at object creation time. This means the reader is responsible to understand the serialized JSON format, which
        # is simply PDL serialized to JSON.
        if parameters:
            event._inner_dict["__parameters_json"] = parameters

        return event

    def as_json(self) -> str:
        json_obj = self.to_obj()
        # Insert parameters, this hack exists because of the way EntityChangeLogClass does not support "AnyRecord"
        if "__parameters_json" in self._inner_dict:
            json_obj["parameters"] = self._inner_dict["__parameters_json"]
        return json.dumps(json_obj)


# Standard Event Types for easy reference.
ENTITY_CHANGE_EVENT_V1_TYPE = "EntityChangeEvent_v1"
METADATA_CHANGE_LOG_EVENT_V1_TYPE = "MetadataChangeLogEvent_v1"

# Lightweight Event Registry
event_registry = PluginRegistry[Event]()

# Register standard event library. Each type can be considered a separate "stream" / "topic"
event_registry.register(METADATA_CHANGE_LOG_EVENT_V1_TYPE, MetadataChangeLogEvent)
event_registry.register(ENTITY_CHANGE_EVENT_V1_TYPE, EntityChangeEvent)
