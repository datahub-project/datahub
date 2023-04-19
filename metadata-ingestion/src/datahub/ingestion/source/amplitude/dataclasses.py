from dataclasses import dataclass
from typing import Dict, List, Optional

from datahub.emitter.mcp_builder import PlatformKey


class ProjectKey(PlatformKey):
    instance: str
    project_id: str
    platform: str


@dataclass
class AmplitudeEventProperties:
    event_property: str
    event_type: str
    description: str
    type: str
    regex: str
    enum_values: str
    is_array_type: bool
    is_required: bool


@dataclass
class AmplitudeEvent:
    event_type: str
    category: Optional[Dict]
    description: Optional[str]
    properties: List[AmplitudeEventProperties]

    def get_properties(self) -> List[AmplitudeEventProperties]:
        return self.properties


@dataclass
class AmplitudeUserProperty:
    user_property: str
    description: str
    type: str
    enum_values: str
    regex: str
    is_array_type: bool


@dataclass
class AmplitudeProject:
    name: str
    description: str
    events: List[AmplitudeEvent]
    user_properties: List[AmplitudeUserProperty]

    def get_events(self) -> List[AmplitudeEvent]:
        return self.events

    def get_user_properties(self) -> List[AmplitudeUserProperty]:
        return self.user_properties
