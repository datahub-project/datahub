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

from datahub.ingestion.api.closeable import Closeable
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.pipeline.pipeline_context import PipelineContext


class Action(Closeable, metaclass=ABCMeta):
    """
    The base class for all DataHub Actions.

    A DataHub action is a component capable of performing a specific action (notification, auditing, synchronization, & more)
    when important events occur on DataHub.

    Each Action may provide its own semantics, configurations, compatibility and guarantees.
    """

    @classmethod
    @abstractmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "Action":
        """Factory method to create an instance of an Action"""
        pass

    @abstractmethod
    def act(self, event: EventEnvelope) -> bool | None:
        """Take Action on DataHub events, provided an instance of a DataHub event.

        Returns:
            Whether to immediately commit the event as processed successfully.
            Return False if the event has been recorded but should not be acknowledged
            until some later time, e.g. due to batch or asynchronous processing.

            Returning None is equivalent to returning True, i.e. it assumes the event was processed successfully.

        Raises:
            Exception: To indicate the event was not processed successfully and to retry processing the same event.
        """
        pass
