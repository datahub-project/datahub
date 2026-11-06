from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Type, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator

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

# TODO: Replace with standardized types in sql_types.py
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

KNOWN_DATA_PLATFORM_MAPPING = {
    "gbq": "bigquery",
    "snowflake": "snowflake",
    "databricks": "databricks",
    "sqlserver": "mssql",
    "mssql": "mssql",
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
    STREAM = "stream"  # Qlik Sense Enterprise stream


# Qlik table box type
class BoxType(Enum):
    LOADFILE = "load-file"  # Table extracted from dataset
    BLACKBOX = "blackbox"  # Table extracted from data connection


PERSONAL_SPACE_DICT = {
    "id": Constant.PERSONAL_SPACE_ID,
    "name": Constant.PERSONAL_SPACE_NAME,
    "description": "",
    "type": SpaceType.PERSONAL,
    "createdAt": datetime.now().strftime(QLIK_DATETIME_FORMAT),
    "updatedAt": datetime.now().strftime(QLIK_DATETIME_FORMAT),
}


class _QlikBaseModel(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)


class Space(_QlikBaseModel):
    id: str
    name: str
    description: str
    type: SpaceType
    createdAt: datetime
    updatedAt: datetime
    ownerId: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def update_values(cls, values: Dict) -> Dict:
        # Create a copy to avoid modifying the input dictionary, preventing state contamination in tests
        values = dict(values)

        values[Constant.CREATEDAT] = datetime.strptime(
            values[Constant.CREATEDAT], QLIK_DATETIME_FORMAT
        )
        values[Constant.UPDATEDAT] = datetime.strptime(
            values[Constant.UPDATEDAT], QLIK_DATETIME_FORMAT
        )
        return values


class Item(_QlikBaseModel):
    id: str
    description: str = ""
    ownerId: Optional[str] = None
    spaceId: str
    createdAt: datetime
    updatedAt: datetime


class SchemaField(_QlikBaseModel):
    name: str
    dataType: Optional[str] = None
    primaryKey: Optional[bool] = None
    nullable: Optional[bool] = None

    @model_validator(mode="before")
    @classmethod
    def update_values(cls, values: Dict) -> Dict:
        # Create a copy to avoid modifying the input dictionary, preventing state contamination in tests
        values = dict(values)
        values[Constant.DATATYPE] = values.get(Constant.DATATYPE, {}).get(Constant.TYPE)
        return values


class QlikDataset(Item):
    name: str
    secureQri: str
    type: str
    size: int
    rowCount: int
    itemId: str
    datasetSchema: List[SchemaField]

    @model_validator(mode="before")
    @classmethod
    def update_values(cls, values: Dict) -> Dict:
        # Create a copy to avoid modifying the input dictionary, preventing state contamination in tests
        values = dict(values)
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

        values[Constant.DATASETSCHEMA] = values[Constant.SCHEMA][Constant.DATAFIELDS]
        return values


class AxisProperty(_QlikBaseModel):
    # The Engine leaves these out for some dimensions and measures, such as
    # ones in an error state.
    Title: Optional[str] = Field(default=None, alias="qFallbackTitle")
    Min: Optional[str] = Field(default=None, alias="qMin")
    Max: Optional[str] = Field(default=None, alias="qMax")


class Chart(_QlikBaseModel):
    qId: str
    visualization: str
    title: str = ""
    subtitle: str = ""
    qDimension: List[AxisProperty]
    qMeasure: List[AxisProperty]
    # Field names or expressions read by the chart's dimensions and measures.
    field_expressions: List[str] = []
    # Master measures keep their expression on the app rather than the chart.
    measure_library_ids: List[str] = []
    fields_complete: bool = True

    @model_validator(mode="before")
    @classmethod
    def update_values(cls, values: Dict) -> Dict:
        # Create a copy to avoid modifying the input dictionary, preventing state contamination in tests
        values = dict(values)
        values[Constant.QID] = values[Constant.QINFO][Constant.QID]
        values["qDimension"] = values[Constant.HYPERCUBE]["qDimensionInfo"]
        values["qMeasure"] = values[Constant.HYPERCUBE]["qMeasureInfo"]
        values.setdefault(
            "field_expressions",
            [
                field_def
                for dimension in values["qDimension"]
                for field_def in dimension.get(Constant.QGROUPFIELDDEFS) or []
            ],
        )
        if any(
            not dimension.get(Constant.QGROUPFIELDDEFS)
            for dimension in values["qDimension"]
        ):
            values["fields_complete"] = False
        return values


class Sheet(_QlikBaseModel):
    id: str
    title: str
    description: str
    ownerId: Optional[str] = None
    createdAt: datetime
    updatedAt: datetime
    charts: List[Chart] = []

    @model_validator(mode="before")
    @classmethod
    def update_values(cls, values: Dict) -> Dict:
        # Create a copy to avoid modifying the input dictionary, preventing state contamination in tests
        values = dict(values)
        values[Constant.CREATEDAT] = datetime.strptime(
            values[Constant.CREATEDDATE], QLIK_DATETIME_FORMAT
        )
        values[Constant.UPDATEDAT] = datetime.strptime(
            values[Constant.MODIFIEDDATE], QLIK_DATETIME_FORMAT
        )
        return values


class AppField(BaseModel):
    dataset_urn: str
    field: SchemaField


class QlikTable(_QlikBaseModel):
    tableName: str
    type: BoxType = Field(alias="boxType")
    tableAlias: str
    dataconnectorid: str
    dataconnectorName: str
    dataconnectorPlatform: str
    spaceId: str
    datasetSchema: List[SchemaField] = Field(alias="fields")
    tableQri: Optional[str] = None
    selectStatement: Optional[str] = None
    databaseName: Optional[str] = None
    schemaName: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def update_values(cls, values: Dict) -> Dict:
        # Create a copy to avoid modifying the input dictionary, preventing state contamination in tests
        values = dict(values)
        values[Constant.DATACONNECTORID] = values[Constant.CONNECTIONINFO][Constant.ID]
        values[Constant.DATACONNECTORPLATFORM] = values[Constant.CONNECTIONINFO][
            Constant.SOURCECONNECTORID
        ]
        values[Constant.SPACEID] = values[Constant.CONNECTIONINFO][Constant.SPACE]
        return values


class App(Item):
    qTitle: str
    qri: str
    # Enterprise on Windows has no app usage.
    qUsage: Optional[str] = None
    sheets: List[Sheet] = []
    tables: List[QlikTable] = []
    script: Optional[str] = None
    fingerprint: Optional[str] = None
    script_read_failed: bool = False
    # Engine reads of this app that failed, so its metadata is only partly read.
    read_errors: List[str] = []

    @model_validator(mode="before")
    @classmethod
    def update_values(cls, values: Dict) -> Dict:
        # Create a copy to avoid modifying the input dictionary, preventing state contamination in tests
        values = dict(values)
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

    @property
    def read_complete(self) -> bool:
        return not self.read_errors


class UnreadItem(BaseModel):
    # A space, item or listing left out of the run after a request failed.
    name: str
    cause: str


class UnchangedApp(BaseModel):
    id: str
    fingerprint: str
