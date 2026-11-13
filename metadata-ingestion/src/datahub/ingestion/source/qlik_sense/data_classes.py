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


# Qlik field tags, most specific first: a date is also tagged $numeric.
_FIELD_TAG_TYPES = [
    ("$timestamp", "TIMESTAMP"),
    ("$date", "DATE"),
    ("$integer", "INTEGER"),
    ("$numeric", "DOUBLE"),
    ("$text", "STRING"),
]


class DataModelTable(BaseModel):
    """A table of an app's data model, from the Engine's GetTablesAndKeys."""

    name: str
    fields: List[SchemaField] = []

    @classmethod
    def from_engine(cls, table: Dict) -> "DataModelTable":
        return cls(
            name=table.get("qName") or "",
            fields=[
                SchemaField.model_validate(
                    {
                        "name": field["qName"],
                        "dataType": {
                            "type": next(
                                (
                                    data_type
                                    for tag, data_type in _FIELD_TAG_TYPES
                                    if tag in (field.get("qTags") or [])
                                ),
                                None,
                            )
                        },
                    }
                )
                for field in table.get("qFields") or []
                if field.get("qName") and not field.get("qIsSynthetic")
            ],
        )


class LineageTable(BaseModel):
    """A table node of Qlik Cloud's lineage graph, with the table nodes it is
    loaded or stored from."""

    qri: str
    label: str
    source_qris: List[str] = []
    # The table's field names, and the field nodes each is loaded or stored
    # from, when the graph was read at field level.
    fields: List[str] = []
    field_source_qris: Dict[str, List[str]] = {}
    # The field level could not be read, so the table's column lineage is
    # unknown this run.
    field_lineage_failed: bool = False


class ListedDataset(BaseModel):
    space_id: str
    name: str
    # None when the listing gives none, so the dataset can't be matched to
    # the lineage graph.
    secure_qri: Optional[str] = None
    type: str = ""


class QlikDataset(Item):
    name: str
    secureQri: str
    type: str
    size: int
    rowCount: int
    itemId: str
    datasetSchema: List[SchemaField]
    # App tables that store into this dataset; None when Qlik's lineage graph
    # for it could not be read.
    stored_from_qris: Optional[List[str]] = None
    # The dataset's fields, and the app table field nodes each is stored from.
    stored_field_source_qris: Dict[str, List[str]] = {}
    stored_field_lineage_failed: bool = False

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

        # Qlik keeps fields the file no longer has in the schema, marked as
        # orphans.
        values[Constant.DATASETSCHEMA] = [
            field
            for field in values[Constant.SCHEMA][Constant.DATAFIELDS]
            if not field.get("orphan")
        ]
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
    # A measure could not be read this time, as opposed to one that no longer
    # exists, so the input fields from the last read are kept.
    fields_unread: bool = False

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
    # False when a chart of the sheet could not be read.
    charts_complete: bool = True

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


class ChartInputs(BaseModel):
    """The app fields a chart reads and the tables it takes them from."""

    fields: List[AppField]
    tables: List[str]


class AppFieldIndex(BaseModel):
    """Chart field lookup. Qlik field names are case-sensitive, but the SQL
    parser may change the case of script table columns, so only those also
    match by name in any case."""

    exact: Dict[str, List[AppField]] = {}
    folded: Dict[str, List[AppField]] = {}

    def get(self, name: str) -> List[AppField]:
        exact = self.exact.get(name, [])
        return exact + [
            field for field in self.folded.get(name.lower(), []) if field not in exact
        ]


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
    # Tables of an app without a data model, from Qlik Cloud's lineage graph.
    lineage_tables: List[LineageTable] = []
    # Qlik Cloud answered 404 for the app's lineage graph.
    lineage_graph_missing: bool = False
    # The tables of the app's data model as the last reload left them, read on
    # Qlik Cloud for apps whose tables come from the lineage graph or script.
    data_model_tables: List[DataModelTable] = []
    # Variable definitions by name, used to expand $(name) in chart expressions.
    variables: Dict[str, str] = {}
    variables_read_failed: bool = False
    script: Optional[str] = None
    fingerprint: Optional[str] = None
    script_read_failed: bool = False
    # The data model or the load script could not be read, so the app's tables,
    # and with them the chart inputs, are unknown.
    tables_read_failed: bool = False
    # The app was opened without its data after loading it failed, so the next
    # run must try again rather than skip it as unchanged.
    data_load_failed: bool = False
    # Engine reads of this app that failed, so its metadata is only partly read.
    read_errors: List[str] = []
    # Every sheet of the app was read, even if other parts of the app were not.
    # Off until a read says so, as it lets the app's sheet edges be replaced.
    sheets_complete: bool = False
    # Batch Engine reads that fell back to slower per-object reads.
    read_fallbacks: List[str] = []

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
        values.setdefault(Constant.QRI, f"qri:app:sense://{values['id']}")
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
