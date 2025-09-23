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
import time
from typing import Dict, Iterable, Optional

from datahub.metadata.schema_classes import (
    AuditStampClass,
    DictWrapper,
    EntityChangeEventClass,
    MetadataChangeLogClass,
    ParametersClass,
)
from datahub_actions.action.action import Action
from datahub_actions.action.action_registry import action_registry
from datahub_actions.event.event import Event
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import (
    EntityChangeEvent,
    MetadataChangeLogEvent,
)
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.source.event_source import EventSource
from datahub_actions.source.event_source_registry import event_source_registry
from datahub_actions.transform.transformer import Transformer
from datahub_actions.transform.transformer_registry import transformer_registry

# Mocked Metadata Change Log representing a Domain change for a Dataset.
metadata_change_log_event = MetadataChangeLogEvent.from_class(
    MetadataChangeLogClass(
        entityType="dataset",
        changeType="UPSERT",
        entityUrn="urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        aspectName="domains",
        created=AuditStampClass(0, "urn:li:corpuser:datahub"),
    )
)


# Mocked Entity Change Event representing a PII tag added to a Dataset.
entity_change_event = EntityChangeEvent.from_class(
    EntityChangeEventClass(
        "dataset",
        "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
        "TAG",
        "ADD",
        AuditStampClass(0, "urn:li:corpuser:datahub"),
        0,
        "urn:li:tag:pii",
        ParametersClass(),
    )
)

# Mocked Envelopes
metadata_change_log_event_env = EventEnvelope(
    "MetadataChangeLogEvent_v1", metadata_change_log_event, {}
)
entity_change_event_env = EventEnvelope(
    "EntityChangeLogEvent_v1", entity_change_event, {}
)

# Mocked Pipeline Context
pipeline_context = PipelineContext(pipeline_name="test", graph=None)


class TestMessage:
    def __init__(self, msg: Dict):
        self.msg: Dict = msg

    def value(self) -> Dict:
        return self.msg

    def topic(self) -> str:
        return self.msg.get("topic", "dummytopic")

    def offset(self) -> int:
        return self.msg.get("offset", 1)

    def partition(self) -> int:
        return self.msg.get("partition", 1)


class TestEvent(Event, DictWrapper):
    def __init__(self, field: str):
        super().__init__()
        self._inner_dict["field"] = field

    @classmethod
    def from_obj(cls, obj: dict, tuples: bool = False) -> "TestEvent":
        return cls(obj["field"])

    def to_obj(self, tuples: bool = False) -> dict:
        return self._inner_dict

    @classmethod
    def from_json(cls, json_str: str) -> "TestEvent":
        json_obj = json.loads(json_str)
        return TestEvent.from_obj(json_obj)

    def as_json(self) -> str:
        return json.dumps(self.to_obj())


class TestEventSource(EventSource):
    """
    Event Source used for testing which counts the number of ack invocations.
    """

    ack_count: int = 0

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "EventSource":
        return TestEventSource()

    def events(self) -> Iterable[EventEnvelope]:
        return [
            metadata_change_log_event_env,
            entity_change_event_env,
            EventEnvelope("TestEvent", TestEvent("value"), {}),
        ]

    def ack(self, event: EventEnvelope, processed: bool = True) -> None:
        self.ack_count = self.ack_count + 1

    def close(self) -> None:
        pass


class TestTransformer(Transformer):
    """
    Transformer used for testing. This transformer simply inserts a smiley face
    into the 'meta' field of the event envelope.
    """

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Transformer":
        return TestTransformer()

    def transform(self, event: EventEnvelope) -> Optional[EventEnvelope]:
        event.meta["test"] = ":)"
        return event


class TestAction(Action):
    """
    Action used for testing valid flows. This action simply increments counters,
    and occassionally throws.
    """

    total_event_count: int = 0
    mcl_count: int = 0
    ece_count: int = 0
    skipped_count: int = 0
    smiley_count: int = 0

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        return TestAction()

    def act(self, event_env: EventEnvelope) -> None:
        # Increment a total events counter.
        self.total_event_count = self.total_event_count + 1

        # Increment the number of events that have a smiley face in their meta
        if event_env.meta["test"] == ":)":
            self.smiley_count = self.smiley_count + 1

        # Handle Metadata Change Events
        if isinstance(event_env.event, MetadataChangeLogClass):
            self.mcl_count = self.mcl_count + 1
            return

        # Handle Entity Change Events
        if isinstance(event_env.event, EntityChangeEventClass):
            self.ece_count = self.ece_count + 1
            return

        # Handle skipped events
        self.skipped_count = self.skipped_count + 1

    def close(self) -> None:
        pass


class StoppableEventSource(EventSource):
    """
    Event Source which generates the same event repeatedly until 'close' is invoked.
    """

    stopped: bool = False

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "EventSource":
        return StoppableEventSource()

    def events(self) -> Iterable[EventEnvelope]:
        while True:
            time.sleep(1)
            if self.stopped:
                return
            yield EventEnvelope(
                "MetadataChangeLogEvent_v1", metadata_change_log_event, {}
            )

    def ack(self, event: EventEnvelope, processed: bool = True) -> None:
        pass

    def close(self) -> None:
        self.stopped = True


class ThrowingTestTransformer(Transformer):
    """
    Transformer used for testing exceptions thrown within an action.
    """

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Transformer":
        return ThrowingTestTransformer()

    def transform(self, env_event: EventEnvelope) -> None:
        raise Exception("Ouch! Transformer code threw an exception.")


class ThrowingTestAction(Action):
    """
    Action used for testing exceptions thrown within an action.
    """

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        return ThrowingTestAction()

    def act(self, event_env: EventEnvelope) -> None:
        raise Exception("Ouch! Action code threw an exception.")

    def close(self) -> None:
        pass


# Register test components.
event_source_registry.register("test_source", TestEventSource)
event_source_registry.register("stoppable_event_source", StoppableEventSource)

transformer_registry.register("test_transformer", TestTransformer)
transformer_registry.register("throwing_test_transformer", ThrowingTestTransformer)

action_registry.register("test_action", TestAction)
action_registry.register("throwing_test_action", ThrowingTestAction)
