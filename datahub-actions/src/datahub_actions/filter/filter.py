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

from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext


class Filter(metaclass=ABCMeta):
    """
    Base class for all DataHub Pipeline Filters.

    A Filter decides whether an event should continue downstream to transformers
    and the action. Returning False from `matches` drops the event.

    Multiple filters in a pipeline are combined with AND semantics: the event
    must satisfy every filter to proceed.
    """

    @classmethod
    @abstractmethod
    def create(cls, config: dict, ctx: PipelineContext) -> "Filter":
        """Factory method to create an instance of a Filter."""
        pass

    @abstractmethod
    def matches(self, event: EventEnvelope) -> bool:
        """Return True if the event should be forwarded downstream."""
        pass
