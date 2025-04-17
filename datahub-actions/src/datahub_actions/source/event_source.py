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

from abc import ABCMeta, abstractmethod
from typing import Iterable

from datahub.ingestion.api.closeable import Closeable
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext


class EventSource(Closeable, metaclass=ABCMeta):
    """
    The base class for all DataHub Event Sources.

    An Event Source is a producer of DataHub Events which can be acted on using the
    Actions Framework.

    Each Event Source may provide specific semantics, configurations, and processing guarantees.
    Using this interface, the framework can accommodate at-least-once delivery to an individual Action.
    """

    @classmethod
    @abstractmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "EventSource":
        """Factory method to create an instance of an Event Source"""
        pass

    @abstractmethod
    def events(self) -> Iterable[EventEnvelope]:
        """
        Returns an iterable of enveloped events.

        In most cases this should be implemented via a Python generator function which
        can produce a continuous stream of events.
        """

    @abstractmethod
    def ack(self, event: EventEnvelope, processed: bool = True) -> None:
        """
        Acknowledges the processing of an individual event by the Actions Framework
        """
