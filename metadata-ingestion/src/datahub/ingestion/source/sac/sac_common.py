from dataclasses import dataclass
from datetime import datetime
from typing import FrozenSet, Optional


@dataclass(frozen=True)
class ResourceModel:
    namespace: str
    model_id: str
    name: str
    description: str
    system_type: Optional[str]
    connection_id: Optional[str]
    external_id: Optional[str]
    is_import: bool


@dataclass(frozen=True)
class Resource:
    resource_id: str
    resource_type: str
    resource_subtype: str
    story_id: str
    name: str
    description: str
    created_time: datetime
    created_by: Optional[str]
    modified_time: datetime
    modified_by: Optional[str]
    open_url: str
    ancestor_path: Optional[str]
    is_mobile: bool
    resource_models: FrozenSet[ResourceModel]


@dataclass(frozen=True)
class ImportDataModelColumn:
    name: str
    description: str
    property_type: str
    data_type: str
    max_length: Optional[int]
    precision: Optional[int]
    scale: Optional[int]
    is_key: bool
