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
from typing import Optional

from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext


class Transformer(metaclass=ABCMeta):
    """
    A base class for all DataHub Event Transformers.

    A Transformer is responsible for filtering and / or transforming events emitted from an Event Source,
    prior to forwarding them to the configured Action.

    Transformers can be chained together to form a multi-stage "transformer chain". Each Transformer
    may provide its own semantics, configurations, compatibility and guarantees.
    """

    @classmethod
    @abstractmethod
    def create(cls, config: dict, ctx: PipelineContext) -> "Transformer":
        """Factory method to create an instance of a Transformer"""
        pass

    @abstractmethod
    def transform(self, event: EventEnvelope) -> Optional[EventEnvelope]:
        """
        Transform a single Event.

        This method returns an instance of EventEnvelope, or 'None' if the event has been filtered.
        """
