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


class Event(metaclass=ABCMeta):
    """
    A DataHub Event.
    """

    @classmethod
    @abstractmethod
    def from_json(cls, json_str: str) -> "Event":
        """
        Convert from json format into the event object.
        """

    @abstractmethod
    def as_json(self) -> str:
        """
        Convert the event into its JSON representation.
        """


class PlaceholderEvent(Event):
    """An Event that carries no payload and does not support serialization.

    It holds no data; as_json / from_json raise. It exists only to occupy the
    non-optional ``event`` slot of an EventEnvelope when there is no event payload.
    """

    @classmethod
    def from_json(cls, json_str: str) -> "PlaceholderEvent":
        raise NotImplementedError(
            "PlaceholderEvent has no payload and cannot be deserialized"
        )

    def as_json(self) -> str:
        raise NotImplementedError(
            "PlaceholderEvent has no payload and cannot be serialized"
        )
