from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from datahub.metadata.schema_classes import EdgeClass, SchemaFieldDataTypeClass


@dataclass
class Field:
    name: str
    type: SchemaFieldDataTypeClass


@dataclass
class Dataset:
    id: str
    platform: str
    properties: Optional[Dict[Any, Any]] = None
    schema_metadata: Optional[List[Field]] = None


@dataclass
class Task:
    name: str
    upstream_edges: List[EdgeClass]
    downstream_edges: List[EdgeClass]


@dataclass
class Pipeline:
    platform: str
    name: str
    tasks: List[Task]
