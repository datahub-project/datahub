from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Type, Union

from datahub.emitter.mcp_builder import ContainerKey
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    BooleanType,
    BytesType,
    DateType,
    NumberType,
    StringType,
    TimeType,
)

FIELD_TYPE_MAPPING: Dict[
    str,
    Type[
        Union[
            BooleanType,
            BytesType,
            DateType,
            NumberType,
            StringType,
            TimeType,
        ]
    ],
] = {
    "DATE": DateType,
    "TIME": TimeType,
    "DATETIME": DateType,
    "TIMESTAMP": DateType,
    "STRING": StringType,
    "DOUBLE": NumberType,
    "DECIMAL": NumberType,
    "INTEGER": NumberType,
    "BOOLEAN": BooleanType,
    "BINARY": BytesType,
}


class SpaceKey(ContainerKey):
    space: str


class AppKey(ContainerKey):
    app: str


class SpaceType(Enum):
    PERSONAL = "personal"
    SHARED = "shared"
    MANAGED = "managed"
    DATA = "data"


@dataclass
class Space:
    id: str
    name: str
    description: str
    type: SpaceType
    created_at: datetime
    updated_at: datetime
    owner_id: Optional[str] = None


@dataclass
class SchemaField:
    name: str
    data_type: str
    primary_key: bool
    nullable: bool


@dataclass
class Item:
    id: str
    name: str
    qri: str
    description: str
    owner_id: str
    space_id: str
    created_at: datetime
    updated_at: datetime


@dataclass
class App(Item):
    usage: str


@dataclass
class QlikDataset(Item):
    type: str
    size: int
    row_count: int
    schema: List[SchemaField]


@dataclass
class User:
    id: str
    displayName: str
    emailAddress: str
    graphId: str
    principalType: str
    datasetUserAccessRight: Optional[str] = None
    reportUserAccessRight: Optional[str] = None
    dashboardUserAccessRight: Optional[str] = None
    groupUserAccessRight: Optional[str] = None

    def get_urn_part(self, use_email: bool, remove_email_suffix: bool) -> str:
        if use_email:
            if remove_email_suffix:
                return self.emailAddress.split("@")[0]
            else:
                return self.emailAddress
        return f"users.{self.id}"

    def __members(self):
        return (self.id,)

    def __eq__(self, instance):
        return isinstance(instance, User) and self.__members() == instance.__members()

    def __hash__(self):
        return hash(self.__members())
