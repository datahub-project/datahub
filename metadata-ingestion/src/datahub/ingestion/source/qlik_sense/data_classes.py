from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Type, Union

from pydantic import BaseModel, root_validator

from datahub.emitter.mcp_builder import ContainerKey
from datahub.ingestion.source.qlik_sense.config import QLIK_DATETIME_FORMAT, Constant
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


PERSONAL_SPACE_DICT = {
    "id": Constant.PERSONAL_SPACE_ID,
    "name": Constant.PERSONAL_SPACE_NAME,
    "description": "",
    "type": SpaceType.PERSONAL,
    "createdAt": datetime.now().strftime(QLIK_DATETIME_FORMAT),
    "updatedAt": datetime.now().strftime(QLIK_DATETIME_FORMAT),
}


class Space(BaseModel):
    id: str
    name: str
    description: str
    type: SpaceType
    createdAt: datetime
    updatedAt: datetime
    ownerId: Optional[str] = None

    @root_validator(pre=True)
    def update_values(cls, values: Dict) -> Dict:
        values[Constant.CREATEDAT] = datetime.strptime(
            values[Constant.CREATEDAT], QLIK_DATETIME_FORMAT
        )
        values[Constant.UPDATEDAT] = datetime.strptime(
            values[Constant.UPDATEDAT], QLIK_DATETIME_FORMAT
        )
        return values


class Item(BaseModel):
    id: str
    name: str
    qri: str
    description: str
    ownerId: str
    spaceId: str
    createdAt: datetime
    updatedAt: datetime


class Chart(BaseModel):
    qId: str
    qType: str


class Sheet(BaseModel):
    id: str
    title: str
    description: str
    ownerId: str
    createdAt: datetime
    updatedAt: datetime
    charts: List[Chart] = []

    @root_validator(pre=True)
    def update_values(cls, values: Dict) -> Dict:
        values[Constant.CREATEDAT] = datetime.strptime(
            values[Constant.CREATEDDATE], QLIK_DATETIME_FORMAT
        )
        values[Constant.UPDATEDAT] = datetime.strptime(
            values[Constant.MODIFIEDDATE], QLIK_DATETIME_FORMAT
        )
        return values


class App(Item):
    usage: str
    sheets: List[Sheet] = []

    @root_validator(pre=True)
    def update_values(cls, values: Dict) -> Dict:
        values[Constant.CREATEDAT] = datetime.strptime(
            values[Constant.CREATEDDATE], QLIK_DATETIME_FORMAT
        )
        values[Constant.UPDATEDAT] = datetime.strptime(
            values[Constant.MODIFIEDDATE], QLIK_DATETIME_FORMAT
        )
        if not values.get(Constant.SPACEID):
            # spaceId none indicates app present in personal space
            values[Constant.SPACEID] = Constant.PERSONAL_SPACE_ID
        values[Constant.QRI] = f"qri:app:sense://{values['id']}"
        return values


class SchemaField(BaseModel):
    name: str
    dataType: str
    primaryKey: bool
    nullable: bool


class QlikDataset(Item):
    type: str
    size: int
    rowCount: int
    datasetSchema: List[SchemaField]

    @root_validator(pre=True)
    def update_values(cls, values: Dict) -> Dict:
        # Update str time to datetime
        values[Constant.CREATEDAT] = datetime.strptime(
            values[Constant.CREATEDTIME], QLIK_DATETIME_FORMAT
        )
        values[Constant.UPDATEDAT] = datetime.strptime(
            values[Constant.LASTMODIFIEDTIME], QLIK_DATETIME_FORMAT
        )
        if not values.get(Constant.SPACEID):
            # spaceId none indicates dataset present in personal space
            values[Constant.SPACEID] = Constant.PERSONAL_SPACE_ID
        values[Constant.QRI] = values[Constant.SECUREQRI]
        values[Constant.SIZE] = values[Constant.OPERATIONAL].get(Constant.SIZE, 0)
        values[Constant.ROWCOUNT] = values[Constant.OPERATIONAL][Constant.ROWCOUNT]

        values[Constant.DATASETSCHEMA] = [
            {
                Constant.NAME: field[Constant.NAME],
                Constant.DATATYPE: field[Constant.DATATYPE][Constant.TYPE],
                Constant.PRIMARYKEY: field[Constant.PRIMARYKEY],
                Constant.NULLABLE: field[Constant.NULLABLE],
            }
            for field in values[Constant.SCHEMA][Constant.DATAFIELDS]
        ]
        return values
