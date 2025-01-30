from datetime import datetime
from enum import Enum
from typing import Annotated, List, Literal, Optional, Union

from pydantic import BaseModel, Field


class FieldDataType(str, Enum):
    """Possible data types for a field."""

    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    DATETIME = "DATETIME"
    INTEGER = "INTEGER"
    REAL = "REAL"
    SPATIAL = "SPATIAL"
    STRING = "STRING"
    TABLE = "TABLE"
    TUPLE = "TUPLE"
    UNKNOWN = "UNKNOWN"


class FieldRole(str, Enum):
    """Possible roles of a field."""

    DIMENSION = "DIMENSION"
    MEASURE = "MEASURE"
    UNKNOWN = "UNKNOWN"


class FieldRoleCategory(str, Enum):
    """Possible categories of a field role."""

    NOMINAL = "NOMINAL"
    ORDINAL = "ORDINAL"
    QUANTITATIVE = "QUANTITATIVE"
    UNKNOWN = "UNKNOWN"


class RemoteType(str, Enum):
    """Possible types of remote types

    Types correspond to OLEDB types here: https://referencesource.microsoft.com/#system.data/System/Data/OleDb/OLEDB_Enum.cs,364"

    Types prefixed with 'WDC' correspond to Tableau's Web Data Connector types:
    https://tableau.github.io/webdataconnector/docs/api_ref.html#webdataconnectorapi.datatypeenum
    """

    ARRAY = "ARRAY"
    BOOL = "BOOL"
    BSTR = "BSTR"
    BYREF = "BYREF"
    BYTES = "BYTES"
    CY = "CY"
    DATE = "DATE"
    DBDATE = "DBDATE"
    DBTIME = "DBTIME"
    DBTIMESTAMP = "DBTIMESTAMP"
    DECIMAL = "DECIMAL"
    EMPTY = "EMPTY"
    ERROR = "ERROR"
    FILETIME = "FILETIME"
    GUID = "GUID"
    HCHAPTER = "HCHAPTER"
    I1 = "I1"
    I2 = "I2"
    I4 = "I4"
    I8 = "I8"
    IDISPATCH = "IDISPATCH"
    IUNKNOWN = "IUNKNOWN"
    NULL = "NULL"
    NUMERIC = "NUMERIC"
    PROPVARIANT = "PROPVARIANT"
    R4 = "R4"
    R8 = "R8"
    RESERVED = "RESERVED"
    STR = "STR"
    UDT = "UDT"
    UI1 = "UI1"
    UI2 = "UI2"
    UI4 = "UI4"
    UI8 = "UI8"
    VARIANT = "VARIANT"
    VARNUMERIC = "VARNUMERIC"
    VECTOR = "VECTOR"
    WDC_BOOL = "WDC_BOOL"
    WDC_DATE = "WDC_DATE"
    WDC_DATETIME = "WDC_DATETIME"
    WDC_FLOAT = "WDC_FLOAT"
    WDC_GEOMETRY = "WDC_GEOMETRY"
    WDC_INT = "WDC_INT"
    WDC_STRING = "WDC_STRING"
    WSTR = "WSTR"


class GetItems_databaseTablesConnectionDatabasetablesconnectionNodesColumns(BaseModel):
    """GraphQL type for a table column"""

    typename: Literal["Column"] = Field(alias="__typename", default="Column")
    remote_type: RemoteType = Field(alias="remoteType")
    "Remote type on the database. Types correspond to OLEDB types here: https://referencesource.microsoft.com/#system.data/System/Data/OleDb/OLEDB_Enum.cs,364"
    name: Optional[str] = Field(default=None)
    "Name of column"


class GetItems_databaseTablesConnectionDatabasetablesconnectionNodes(BaseModel):
    """A table that is contained in a database"""

    typename: Literal["DatabaseTable"] = Field(
        alias="__typename", default="DatabaseTable"
    )
    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"
    is_embedded: Optional[bool] = Field(default=None, alias="isEmbedded")
    "True if this table is embedded in Tableau content"
    columns: List[GetItems_databaseTablesConnectionDatabasetablesconnectionNodesColumns]
    "Columns contained in this table"


class GetItems_databaseTablesConnectionDatabasetablesconnectionPageinfo(BaseModel):
    """Information about pagination in a connection"""

    typename: Literal["PageInfo"] = Field(alias="__typename", default="PageInfo")
    has_next_page: bool = Field(alias="hasNextPage")
    "Indicates if there are more objects to fetch"
    end_cursor: Optional[str] = Field(default=None, alias="endCursor")
    "Cursor to use in subsequent query to fetch next page of objects"


class GetItems_databaseTablesConnectionDatabasetablesconnection(BaseModel):
    """Connection Type for DatabaseTable"""

    typename: Literal["DatabaseTablesConnection"] = Field(
        alias="__typename", default="DatabaseTablesConnection"
    )
    nodes: List[GetItems_databaseTablesConnectionDatabasetablesconnectionNodes]
    "List of nodes"
    page_info: GetItems_databaseTablesConnectionDatabasetablesconnectionPageinfo = (
        Field(alias="pageInfo")
    )
    "Information for pagination"


class GetItems_databaseTablesConnection(BaseModel):
    database_tables_connection: GetItems_databaseTablesConnectionDatabasetablesconnection = Field(
        alias="databaseTablesConnection"
    )
    "Fetch DatabaseTables with support for pagination"

    class Arguments(BaseModel):
        first: Optional[int] = Field(default=None)
        after: Optional[str] = Field(default=None)

    class Meta:
        document = 'query GetItems_databaseTablesConnection($first: Int, $after: String) {\n  databaseTablesConnection(\n    first: $first\n    after: $after\n    filter: {idWithin: ["76e2151b-124f-7ec8-896b-dd107eafca05", "a1b165ad-c7c2-282d-94c5-1b8a877936ee", "2d3bdb4e-08da-a6da-fecb-a3c10abba357", "92b0a3ae-2fc9-1b42-47e0-c17d0f0b615a", "63ffbbfe-8c2d-c4f3-7a28-e11f247227c7", "06d776e1-9376-bc06-84d5-c7a5c4253bf5", "159ed86e-796f-3f13-8f07-3e63c271015e", "2f2ccb48-edd5-0e02-4d51-eb3b0819fbf1", "2fe499f7-9c5a-81ef-f32d-9553dcc86044", "4a34ada9-ed4a-089f-01d0-4a1f230ee2e6"]}\n  ) {\n    nodes {\n      id\n      isEmbedded\n      columns {\n        remoteType\n        name\n        __typename\n      }\n      __typename\n    }\n    pageInfo {\n      hasNextPage\n      endCursor\n      __typename\n    }\n    __typename\n  }\n}'


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceUpstreamtablesDatabaseBase(
    BaseModel
):
    """A database containing tables"""

    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    id: str
    "Unique identifier used by the Metadata API.  Not the same as the numeric ID used on server"


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceUpstreamtablesDatabaseBaseCloudFile(
    GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceUpstreamtablesDatabaseBase,
    BaseModel,
):
    typename: Literal["CloudFile"] = Field(alias="__typename", default="CloudFile")


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceUpstreamtablesDatabaseBaseDataCloud(
    GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceUpstreamtablesDatabaseBase,
    BaseModel,
):
    typename: Literal["DataCloud"] = Field(alias="__typename", default="DataCloud")


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceUpstreamtablesDatabaseBaseDatabaseServer(
    GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceUpstreamtablesDatabaseBase,
    BaseModel,
):
    typename: Literal["DatabaseServer"] = Field(
        alias="__typename", default="DatabaseServer"
    )


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceUpstreamtablesDatabaseBaseFile(
    GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceUpstreamtablesDatabaseBase,
    BaseModel,
):
    typename: Literal["File"] = Field(alias="__typename", default="File")


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceUpstreamtablesDatabaseBaseWebDataConnector(
    GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceUpstreamtablesDatabaseBase,
    BaseModel,
):
    typename: Literal["WebDataConnector"] = Field(
        alias="__typename", default="WebDataConnector"
    )


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceUpstreamtables(
    BaseModel
):
    """A table that is contained in a database"""

    typename: Literal["DatabaseTable"] = Field(
        alias="__typename", default="DatabaseTable"
    )
    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"
    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    database: Optional[
        Annotated[
            Union[
                GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceUpstreamtablesDatabaseBaseCloudFile,
                GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceUpstreamtablesDatabaseBaseDataCloud,
                GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceUpstreamtablesDatabaseBaseDatabaseServer,
                GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceUpstreamtablesDatabaseBaseFile,
                GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceUpstreamtablesDatabaseBaseWebDataConnector,
            ],
            Field(discriminator="typename"),
        ]
    ] = Field(default=None)
    "The database to which this table belongs"
    the_schema: Optional[str] = Field(
        default=None, alias="schema"
    )  # !!! schema is a reserved word
    "Name of table schema.\n    \nNote: For some databases, such as Amazon Athena and Exasol, the schema attribute may not return the correct schema name for the table. For more information, see https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_model.html#schema_attribute."
    full_name: Optional[str] = Field(default=None, alias="fullName")
    "Fully qualified table name"
    connection_type: Optional[str] = Field(default=None, alias="connectionType")
    "Connection type of parent database"


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceBase(
    BaseModel
):
    """Root GraphQL type for embedded and published data sources

    Data sources are a way to represent how Tableau Desktop and Tableau Server model and connect to data. Data sources can be published separately, as a published data source, or may be contained in a workbook as an embedded data source.

    See https://onlinehelp.tableau.com/current/server/en-us/datasource.htm"""

    id: str
    "Unique identifier used by the metadata API. Not the same as the numeric ID used on server"
    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    upstream_tables: List[
        GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceUpstreamtables
    ] = Field(alias="upstreamTables")
    "Tables upstream from this data source"


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceBaseEmbeddedDatasource(
    GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceBase,
    BaseModel,
):
    typename: Literal["EmbeddedDatasource"] = Field(
        alias="__typename", default="EmbeddedDatasource"
    )


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceBasePublishedDatasource(
    GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceBase,
    BaseModel,
):
    typename: Literal["PublishedDatasource"] = Field(
        alias="__typename", default="PublishedDatasource"
    )


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfields(
    BaseModel
):
    """ColumnFields are a type of field which directly connects to a column in some type of table."""

    typename: Literal["ColumnField"] = Field(alias="__typename", default="ColumnField")
    datasource: Optional[
        Annotated[
            Union[
                GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceBaseEmbeddedDatasource,
                GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfieldsDatasourceBasePublishedDatasource,
            ],
            Field(discriminator="typename"),
        ]
    ] = Field(default=None)
    "Data source that contains this field"


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumns(
    BaseModel
):
    """GraphQL type for a table column"""

    typename: Literal["Column"] = Field(alias="__typename", default="Column")
    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"
    name: Optional[str] = Field(default=None)
    "Name of column"
    remote_type: RemoteType = Field(alias="remoteType")
    "Remote type on the database. Types correspond to OLEDB types here: https://referencesource.microsoft.com/#system.data/System/Data/OleDb/OLEDB_Enum.cs,364"
    description: Optional[str] = Field(default=None)
    "User modifiable description of this column"
    referenced_by_fields: Optional[
        List[
            Optional[
                GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumnsReferencedbyfields
            ]
        ]
    ] = Field(default=None, alias="referencedByFields")
    "The column field that references this column"


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTablesDatabaseBase(
    BaseModel
):
    """A database containing tables"""

    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    id: str
    "Unique identifier used by the Metadata API.  Not the same as the numeric ID used on server"


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTablesDatabaseBaseCloudFile(
    GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTablesDatabaseBase,
    BaseModel,
):
    typename: Literal["CloudFile"] = Field(alias="__typename", default="CloudFile")


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTablesDatabaseBaseDataCloud(
    GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTablesDatabaseBase,
    BaseModel,
):
    typename: Literal["DataCloud"] = Field(alias="__typename", default="DataCloud")


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTablesDatabaseBaseDatabaseServer(
    GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTablesDatabaseBase,
    BaseModel,
):
    typename: Literal["DatabaseServer"] = Field(
        alias="__typename", default="DatabaseServer"
    )


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTablesDatabaseBaseFile(
    GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTablesDatabaseBase,
    BaseModel,
):
    typename: Literal["File"] = Field(alias="__typename", default="File")


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTablesDatabaseBaseWebDataConnector(
    GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTablesDatabaseBase,
    BaseModel,
):
    typename: Literal["WebDataConnector"] = Field(
        alias="__typename", default="WebDataConnector"
    )


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTablesColumnsconnection(
    BaseModel
):
    """Connection Type for Column"""

    typename: Literal["ColumnsConnection"] = Field(
        alias="__typename", default="ColumnsConnection"
    )
    total_count: int = Field(alias="totalCount")
    "Total number of objects in connection"


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTables(BaseModel):
    """A table that is contained in a database"""

    typename: Literal["DatabaseTable"] = Field(
        alias="__typename", default="DatabaseTable"
    )
    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"
    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    database: Optional[
        Annotated[
            Union[
                GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTablesDatabaseBaseCloudFile,
                GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTablesDatabaseBaseDataCloud,
                GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTablesDatabaseBaseDatabaseServer,
                GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTablesDatabaseBaseFile,
                GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTablesDatabaseBaseWebDataConnector,
            ],
            Field(discriminator="typename"),
        ]
    ] = Field(default=None)
    "The database to which this table belongs"
    the_schema: Optional[str] = Field(
        default=None, alias="schema"
    )  # !!! schema is a reserved word
    "Name of table schema.\n    \nNote: For some databases, such as Amazon Athena and Exasol, the schema attribute may not return the correct schema name for the table. For more information, see https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_model.html#schema_attribute."
    full_name: Optional[str] = Field(default=None, alias="fullName")
    "Fully qualified table name"
    connection_type: Optional[str] = Field(default=None, alias="connectionType")
    "Connection type of parent database"
    description: Optional[str] = Field(default=None)
    "User modifiable description of this table"
    columns_connection: Optional[
        GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTablesColumnsconnection
    ] = Field(default=None, alias="columnsConnection")
    "Columns contained in this table"


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesDatabaseBase(
    BaseModel
):
    """A database containing tables"""

    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    id: str
    "Unique identifier used by the Metadata API.  Not the same as the numeric ID used on server"
    connection_type: Optional[str] = Field(default=None, alias="connectionType")
    "Connection type shortname"


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesDatabaseBaseCloudFile(
    GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesDatabaseBase,
    BaseModel,
):
    typename: Literal["CloudFile"] = Field(alias="__typename", default="CloudFile")


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesDatabaseBaseDataCloud(
    GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesDatabaseBase,
    BaseModel,
):
    typename: Literal["DataCloud"] = Field(alias="__typename", default="DataCloud")


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesDatabaseBaseDatabaseServer(
    GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesDatabaseBase,
    BaseModel,
):
    typename: Literal["DatabaseServer"] = Field(
        alias="__typename", default="DatabaseServer"
    )


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesDatabaseBaseFile(
    GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesDatabaseBase,
    BaseModel,
):
    typename: Literal["File"] = Field(alias="__typename", default="File")


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesDatabaseBaseWebDataConnector(
    GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesDatabaseBase,
    BaseModel,
):
    typename: Literal["WebDataConnector"] = Field(
        alias="__typename", default="WebDataConnector"
    )


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodes(BaseModel):
    """A table that represents the result of evaluating a custom SQL query. These "tables" are owned by the Tableau data source (embedded or published) which contains the SQL query, so they only exist within that data source."""

    typename: Literal["CustomSQLTable"] = Field(
        alias="__typename", default="CustomSQLTable"
    )
    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"
    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    query: Optional[str] = Field(default=None)
    "Text of the query"
    columns: List[
        GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesColumns
    ]
    "Columns contained in this table"
    tables: List[GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesTables]
    "Actual tables that this query references."
    connection_type: Optional[str] = Field(default=None, alias="connectionType")
    "Connection type shortname"
    database: Optional[
        Annotated[
            Union[
                GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesDatabaseBaseCloudFile,
                GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesDatabaseBaseDataCloud,
                GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesDatabaseBaseDatabaseServer,
                GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesDatabaseBaseFile,
                GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodesDatabaseBaseWebDataConnector,
            ],
            Field(discriminator="typename"),
        ]
    ] = Field(default=None)
    "Database this query is executed on"


class GetItems_customSQLTablesConnectionCustomsqltablesconnectionPageinfo(BaseModel):
    """Information about pagination in a connection"""

    typename: Literal["PageInfo"] = Field(alias="__typename", default="PageInfo")
    has_next_page: bool = Field(alias="hasNextPage")
    "Indicates if there are more objects to fetch"
    end_cursor: Optional[str] = Field(default=None, alias="endCursor")
    "Cursor to use in subsequent query to fetch next page of objects"


class GetItems_customSQLTablesConnectionCustomsqltablesconnection(BaseModel):
    """Connection Type for CustomSQLTable"""

    typename: Literal["CustomSQLTablesConnection"] = Field(
        alias="__typename", default="CustomSQLTablesConnection"
    )
    nodes: List[GetItems_customSQLTablesConnectionCustomsqltablesconnectionNodes]
    "List of nodes"
    page_info: GetItems_customSQLTablesConnectionCustomsqltablesconnectionPageinfo = (
        Field(alias="pageInfo")
    )
    "Information for pagination"


class GetItems_customSQLTablesConnection(BaseModel):
    custom_sql_tables_connection: GetItems_customSQLTablesConnectionCustomsqltablesconnection = Field(
        alias="customSQLTablesConnection"
    )
    "Fetch CustomSQLTables with support for pagination"

    class Arguments(BaseModel):
        first: Optional[int] = Field(default=None)
        after: Optional[str] = Field(default=None)

    class Meta:
        document = 'query GetItems_customSQLTablesConnection($first: Int, $after: String) {\n  customSQLTablesConnection(\n    first: $first\n    after: $after\n    filter: {idWithin: ["81335c49-5edc-bbfa-77c9-c4a1cd444501", "48c19c5f-4300-07bb-17ee-1fbdf6824ff6"]}\n  ) {\n    nodes {\n      id\n      name\n      query\n      columns {\n        id\n        name\n        remoteType\n        description\n        referencedByFields {\n          datasource {\n            __typename\n            id\n            name\n            upstreamTables {\n              id\n              name\n              database {\n                name\n                id\n                __typename\n              }\n              schema\n              fullName\n              connectionType\n              __typename\n            }\n            ... on PublishedDatasource {\n              projectName\n              luid\n            }\n            ... on EmbeddedDatasource {\n              workbook {\n                id\n                name\n                projectName\n                luid\n              }\n            }\n          }\n          __typename\n        }\n        __typename\n      }\n      tables {\n        id\n        name\n        database {\n          name\n          id\n          __typename\n        }\n        schema\n        fullName\n        connectionType\n        description\n        columnsConnection {\n          totalCount\n          __typename\n        }\n        __typename\n      }\n      connectionType\n      database {\n        name\n        id\n        connectionType\n        __typename\n      }\n      __typename\n    }\n    pageInfo {\n      hasNextPage\n      endCursor\n      __typename\n    }\n    __typename\n  }\n}'


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtablesDatabaseBase(
    BaseModel
):
    """A database containing tables"""

    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    id: str
    "Unique identifier used by the Metadata API.  Not the same as the numeric ID used on server"


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtablesDatabaseBaseCloudFile(
    GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtablesDatabaseBase,
    BaseModel,
):
    typename: Literal["CloudFile"] = Field(alias="__typename", default="CloudFile")


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtablesDatabaseBaseDataCloud(
    GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtablesDatabaseBase,
    BaseModel,
):
    typename: Literal["DataCloud"] = Field(alias="__typename", default="DataCloud")


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtablesDatabaseBaseDatabaseServer(
    GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtablesDatabaseBase,
    BaseModel,
):
    typename: Literal["DatabaseServer"] = Field(
        alias="__typename", default="DatabaseServer"
    )


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtablesDatabaseBaseFile(
    GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtablesDatabaseBase,
    BaseModel,
):
    typename: Literal["File"] = Field(alias="__typename", default="File")


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtablesDatabaseBaseWebDataConnector(
    GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtablesDatabaseBase,
    BaseModel,
):
    typename: Literal["WebDataConnector"] = Field(
        alias="__typename", default="WebDataConnector"
    )


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtablesColumnsconnection(
    BaseModel
):
    """Connection Type for Column"""

    typename: Literal["ColumnsConnection"] = Field(
        alias="__typename", default="ColumnsConnection"
    )
    total_count: int = Field(alias="totalCount")
    "Total number of objects in connection"


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtables(
    BaseModel
):
    """A table that is contained in a database"""

    typename: Literal["DatabaseTable"] = Field(
        alias="__typename", default="DatabaseTable"
    )
    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"
    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    database: Optional[
        Annotated[
            Union[
                GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtablesDatabaseBaseCloudFile,
                GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtablesDatabaseBaseDataCloud,
                GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtablesDatabaseBaseDatabaseServer,
                GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtablesDatabaseBaseFile,
                GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtablesDatabaseBaseWebDataConnector,
            ],
            Field(discriminator="typename"),
        ]
    ] = Field(default=None)
    "The database to which this table belongs"
    the_schema: Optional[str] = Field(
        default=None, alias="schema"
    )  # !!! schema is a reserved word
    "Name of table schema.\n    \nNote: For some databases, such as Amazon Athena and Exasol, the schema attribute may not return the correct schema name for the table. For more information, see https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_model.html#schema_attribute."
    full_name: Optional[str] = Field(default=None, alias="fullName")
    "Fully qualified table name"
    connection_type: Optional[str] = Field(default=None, alias="connectionType")
    "Connection type of parent database"
    description: Optional[str] = Field(default=None)
    "User modifiable description of this table"
    columns_connection: Optional[
        GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtablesColumnsconnection
    ] = Field(default=None, alias="columnsConnection")
    "Columns contained in this table"


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBase(
    BaseModel
):
    """Base GraphQL type for a field"""

    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"
    name: Optional[str] = Field(default=None)
    "Name shown in server"
    description: Optional[str] = Field(default=None)
    "Description of field shown in server and desktop clients"
    is_hidden: Optional[bool] = Field(default=None, alias="isHidden")
    "True if the field is hidden"
    folder_name: Optional[str] = Field(default=None, alias="folderName")
    "Name of folder if the field is in a folder. See https://onlinehelp.tableau.com/current/pro/desktop/en-us/datafields_dwfeatures.html#Organize"


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBaseBinField(
    GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBase,
    BaseModel,
):
    typename: Literal["BinField"] = Field(alias="__typename", default="BinField")


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBaseCalculatedField(
    GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBase,
    BaseModel,
):
    typename: Literal["CalculatedField"] = Field(
        alias="__typename", default="CalculatedField"
    )


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBaseColumnField(
    GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBase,
    BaseModel,
):
    typename: Literal["ColumnField"] = Field(alias="__typename", default="ColumnField")


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBaseCombinedField(
    GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBase,
    BaseModel,
):
    typename: Literal["CombinedField"] = Field(
        alias="__typename", default="CombinedField"
    )


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBaseCombinedSetField(
    GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBase,
    BaseModel,
):
    typename: Literal["CombinedSetField"] = Field(
        alias="__typename", default="CombinedSetField"
    )


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBaseDatasourceField(
    GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBase,
    BaseModel,
):
    typename: Literal["DatasourceField"] = Field(
        alias="__typename", default="DatasourceField"
    )


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBaseGroupField(
    GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBase,
    BaseModel,
):
    typename: Literal["GroupField"] = Field(alias="__typename", default="GroupField")


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBaseHierarchyField(
    GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBase,
    BaseModel,
):
    typename: Literal["HierarchyField"] = Field(
        alias="__typename", default="HierarchyField"
    )


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBaseSetField(
    GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBase,
    BaseModel,
):
    typename: Literal["SetField"] = Field(alias="__typename", default="SetField")


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesOwner(
    BaseModel
):
    """User on a site on Tableau server"""

    typename: Literal["TableauUser"] = Field(alias="__typename", default="TableauUser")
    username: Optional[str] = Field(default=None)
    "Username of this user"


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesTags(
    BaseModel
):
    """A tag associated with content items"""

    typename: Literal["Tag"] = Field(alias="__typename", default="Tag")
    name: Optional[str] = Field(default=None)
    "The name of the tag"


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodes(
    BaseModel
):
    """A Tableau data source that has been published separately to Tableau Server. It can be used by multiple workbooks."""

    typename: Literal["PublishedDatasource"] = Field(
        alias="__typename", default="PublishedDatasource"
    )
    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"
    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    luid: str
    "Locally unique identifier used for the REST API on the Tableau Server"
    has_extracts: Optional[bool] = Field(default=None, alias="hasExtracts")
    "True if datasource contains extracted data"
    extract_last_refresh_time: Optional[datetime] = Field(
        default=None, alias="extractLastRefreshTime"
    )
    "Time an extract was last fully refreshed"
    extract_last_incremental_update_time: Optional[datetime] = Field(
        default=None, alias="extractLastIncrementalUpdateTime"
    )
    "Time an extract was last incrementally updated"
    extract_last_update_time: Optional[datetime] = Field(
        default=None, alias="extractLastUpdateTime"
    )
    "Time an extract was last updated by either a full refresh, incremental update, or creation"
    upstream_tables: List[
        GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesUpstreamtables
    ] = Field(alias="upstreamTables")
    "Tables upstream from this data source"
    fields: List[
        Annotated[
            Union[
                GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBaseBinField,
                GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBaseCalculatedField,
                GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBaseColumnField,
                GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBaseCombinedField,
                GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBaseCombinedSetField,
                GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBaseDatasourceField,
                GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBaseGroupField,
                GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBaseHierarchyField,
                GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesFieldsBaseSetField,
            ],
            Field(discriminator="typename"),
        ]
    ]
    "Fields usable in workbooks connected to this data source"
    owner: (
        GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesOwner
    )
    "User who owns this data source"
    description: Optional[str] = Field(default=None)
    "Description of the datasource"
    uri: Optional[str] = Field(default=None)
    "Uri of the datasource"
    project_name: Optional[str] = Field(default=None, alias="projectName")
    "The name of the project that contains this published data source."
    tags: List[
        GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodesTags
    ]
    "Tags associated with the published datasource"


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionPageinfo(
    BaseModel
):
    """Information about pagination in a connection"""

    typename: Literal["PageInfo"] = Field(alias="__typename", default="PageInfo")
    has_next_page: bool = Field(alias="hasNextPage")
    "Indicates if there are more objects to fetch"
    end_cursor: Optional[str] = Field(default=None, alias="endCursor")
    "Cursor to use in subsequent query to fetch next page of objects"


class GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnection(BaseModel):
    """Connection Type for PublishedDatasource"""

    typename: Literal["PublishedDatasourcesConnection"] = Field(
        alias="__typename", default="PublishedDatasourcesConnection"
    )
    nodes: List[
        GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionNodes
    ]
    "List of nodes"
    page_info: GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnectionPageinfo = Field(
        alias="pageInfo"
    )
    "Information for pagination"


class GetItems_publishedDatasourcesConnection(BaseModel):
    published_datasources_connection: GetItems_publishedDatasourcesConnectionPublisheddatasourcesconnection = Field(
        alias="publishedDatasourcesConnection"
    )
    "Fetch PublishedDatasources with support for pagination"

    class Arguments(BaseModel):
        first: Optional[int] = Field(default=None)
        after: Optional[str] = Field(default=None)

    class Meta:
        document = 'query GetItems_publishedDatasourcesConnection($first: Int, $after: String) {\n  publishedDatasourcesConnection(\n    first: $first\n    after: $after\n    filter: {idWithin: ["35d62018-68e1-6ad4-2cf0-d3ede4ee9a67", "87d9d9d8-59a8-adc3-3e06-f75c5b98ec52", "ae8b52c9-1481-06fd-fe96-e6d4938f9fcb"]}\n  ) {\n    nodes {\n      __typename\n      id\n      name\n      luid\n      hasExtracts\n      extractLastRefreshTime\n      extractLastIncrementalUpdateTime\n      extractLastUpdateTime\n      upstreamTables {\n        id\n        name\n        database {\n          name\n          id\n          __typename\n        }\n        schema\n        fullName\n        connectionType\n        description\n        columnsConnection {\n          totalCount\n          __typename\n        }\n        __typename\n      }\n      fields {\n        __typename\n        id\n        name\n        description\n        isHidden\n        folderName\n        ... on ColumnField {\n          dataCategory\n          role\n          dataType\n          defaultFormat\n          aggregation\n        }\n        ... on CalculatedField {\n          role\n          dataType\n          defaultFormat\n          aggregation\n          formula\n        }\n        ... on GroupField {\n          role\n          dataType\n        }\n      }\n      owner {\n        username\n        __typename\n      }\n      description\n      uri\n      projectName\n      tags {\n        name\n        __typename\n      }\n    }\n    pageInfo {\n      hasNextPage\n      endCursor\n      __typename\n    }\n    __typename\n  }\n}'


class GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsDatasourceBase(
    BaseModel
):
    """Root GraphQL type for embedded and published data sources

    Data sources are a way to represent how Tableau Desktop and Tableau Server model and connect to data. Data sources can be published separately, as a published data source, or may be contained in a workbook as an embedded data source.

    See https://onlinehelp.tableau.com/current/server/en-us/datasource.htm"""

    id: str
    "Unique identifier used by the metadata API. Not the same as the numeric ID used on server"


class GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsDatasourceBaseEmbeddedDatasource(
    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsDatasourceBase,
    BaseModel,
):
    typename: Literal["EmbeddedDatasource"] = Field(
        alias="__typename", default="EmbeddedDatasource"
    )


class GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsDatasourceBasePublishedDatasource(
    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsDatasourceBase,
    BaseModel,
):
    typename: Literal["PublishedDatasource"] = Field(
        alias="__typename", default="PublishedDatasource"
    )


class GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBase(BaseModel):
    """Base GraphQL type for a field"""

    name: Optional[str] = Field(default=None)
    "Name shown in server"
    datasource: Optional[
        Annotated[
            Union[
                GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsDatasourceBaseEmbeddedDatasource,
                GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsDatasourceBasePublishedDatasource,
            ],
            Field(discriminator="typename"),
        ]
    ] = Field(default=None)
    "Data source that contains this field"


class GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBaseBinField(
    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBase, BaseModel
):
    typename: Literal["BinField"] = Field(alias="__typename", default="BinField")


class GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBaseCalculatedField(
    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBase, BaseModel
):
    typename: Literal["CalculatedField"] = Field(
        alias="__typename", default="CalculatedField"
    )


class GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBaseColumnField(
    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBase, BaseModel
):
    typename: Literal["ColumnField"] = Field(alias="__typename", default="ColumnField")


class GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBaseCombinedField(
    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBase, BaseModel
):
    typename: Literal["CombinedField"] = Field(
        alias="__typename", default="CombinedField"
    )


class GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBaseCombinedSetField(
    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBase, BaseModel
):
    typename: Literal["CombinedSetField"] = Field(
        alias="__typename", default="CombinedSetField"
    )


class GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBaseDatasourceField(
    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBase, BaseModel
):
    typename: Literal["DatasourceField"] = Field(
        alias="__typename", default="DatasourceField"
    )


class GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBaseGroupField(
    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBase, BaseModel
):
    typename: Literal["GroupField"] = Field(alias="__typename", default="GroupField")


class GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBaseHierarchyField(
    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBase, BaseModel
):
    typename: Literal["HierarchyField"] = Field(
        alias="__typename", default="HierarchyField"
    )


class GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBaseSetField(
    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBase, BaseModel
):
    typename: Literal["SetField"] = Field(alias="__typename", default="SetField")


class GetItems_fieldsConnectionFieldsconnectionNodesUpstreamcolumnsTableBase(BaseModel):
    """A table containing columns"""

    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"


class GetItems_fieldsConnectionFieldsconnectionNodesUpstreamcolumnsTableBaseCustomSQLTable(
    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamcolumnsTableBase, BaseModel
):
    typename: Literal["CustomSQLTable"] = Field(
        alias="__typename", default="CustomSQLTable"
    )


class GetItems_fieldsConnectionFieldsconnectionNodesUpstreamcolumnsTableBaseDatabaseTable(
    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamcolumnsTableBase, BaseModel
):
    typename: Literal["DatabaseTable"] = Field(
        alias="__typename", default="DatabaseTable"
    )


class GetItems_fieldsConnectionFieldsconnectionNodesUpstreamcolumnsTableBaseVirtualConnectionTable(
    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamcolumnsTableBase, BaseModel
):
    typename: Literal["VirtualConnectionTable"] = Field(
        alias="__typename", default="VirtualConnectionTable"
    )


class GetItems_fieldsConnectionFieldsconnectionNodesUpstreamcolumns(BaseModel):
    """GraphQL type for a table column"""

    typename: Literal["Column"] = Field(alias="__typename", default="Column")
    name: Optional[str] = Field(default=None)
    "Name of column"
    table: Optional[
        Annotated[
            Union[
                GetItems_fieldsConnectionFieldsconnectionNodesUpstreamcolumnsTableBaseCustomSQLTable,
                GetItems_fieldsConnectionFieldsconnectionNodesUpstreamcolumnsTableBaseDatabaseTable,
                GetItems_fieldsConnectionFieldsconnectionNodesUpstreamcolumnsTableBaseVirtualConnectionTable,
            ],
            Field(discriminator="typename"),
        ]
    ] = Field(default=None)
    "The table that this column belongs to"


class GetItems_fieldsConnectionFieldsconnectionNodesBase(BaseModel):
    """Base GraphQL type for a field"""

    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"
    upstream_fields: List[
        Optional[
            Annotated[
                Union[
                    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBaseBinField,
                    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBaseCalculatedField,
                    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBaseColumnField,
                    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBaseCombinedField,
                    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBaseCombinedSetField,
                    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBaseDatasourceField,
                    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBaseGroupField,
                    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBaseHierarchyField,
                    GetItems_fieldsConnectionFieldsconnectionNodesUpstreamfieldsBaseSetField,
                ],
                Field(discriminator="typename"),
            ]
        ]
    ] = Field(alias="upstreamFields")
    "fields that are upstream of this field"
    upstream_columns: List[
        Optional[GetItems_fieldsConnectionFieldsconnectionNodesUpstreamcolumns]
    ] = Field(alias="upstreamColumns")
    "All upstream columns this field references"


class GetItems_fieldsConnectionFieldsconnectionNodesBaseBinField(
    GetItems_fieldsConnectionFieldsconnectionNodesBase, BaseModel
):
    typename: Literal["BinField"] = Field(alias="__typename", default="BinField")


class GetItems_fieldsConnectionFieldsconnectionNodesBaseCalculatedField(
    GetItems_fieldsConnectionFieldsconnectionNodesBase, BaseModel
):
    typename: Literal["CalculatedField"] = Field(
        alias="__typename", default="CalculatedField"
    )


class GetItems_fieldsConnectionFieldsconnectionNodesBaseColumnField(
    GetItems_fieldsConnectionFieldsconnectionNodesBase, BaseModel
):
    typename: Literal["ColumnField"] = Field(alias="__typename", default="ColumnField")


class GetItems_fieldsConnectionFieldsconnectionNodesBaseCombinedField(
    GetItems_fieldsConnectionFieldsconnectionNodesBase, BaseModel
):
    typename: Literal["CombinedField"] = Field(
        alias="__typename", default="CombinedField"
    )


class GetItems_fieldsConnectionFieldsconnectionNodesBaseCombinedSetField(
    GetItems_fieldsConnectionFieldsconnectionNodesBase, BaseModel
):
    typename: Literal["CombinedSetField"] = Field(
        alias="__typename", default="CombinedSetField"
    )


class GetItems_fieldsConnectionFieldsconnectionNodesBaseDatasourceField(
    GetItems_fieldsConnectionFieldsconnectionNodesBase, BaseModel
):
    typename: Literal["DatasourceField"] = Field(
        alias="__typename", default="DatasourceField"
    )


class GetItems_fieldsConnectionFieldsconnectionNodesBaseGroupField(
    GetItems_fieldsConnectionFieldsconnectionNodesBase, BaseModel
):
    typename: Literal["GroupField"] = Field(alias="__typename", default="GroupField")


class GetItems_fieldsConnectionFieldsconnectionNodesBaseHierarchyField(
    GetItems_fieldsConnectionFieldsconnectionNodesBase, BaseModel
):
    typename: Literal["HierarchyField"] = Field(
        alias="__typename", default="HierarchyField"
    )


class GetItems_fieldsConnectionFieldsconnectionNodesBaseSetField(
    GetItems_fieldsConnectionFieldsconnectionNodesBase, BaseModel
):
    typename: Literal["SetField"] = Field(alias="__typename", default="SetField")


class GetItems_fieldsConnectionFieldsconnectionPageinfo(BaseModel):
    """Information about pagination in a connection"""

    typename: Literal["PageInfo"] = Field(alias="__typename", default="PageInfo")
    has_next_page: bool = Field(alias="hasNextPage")
    "Indicates if there are more objects to fetch"
    end_cursor: Optional[str] = Field(default=None, alias="endCursor")
    "Cursor to use in subsequent query to fetch next page of objects"


class GetItems_fieldsConnectionFieldsconnection(BaseModel):
    """Connection Type for Field"""

    typename: Literal["FieldsConnection"] = Field(
        alias="__typename", default="FieldsConnection"
    )
    nodes: List[
        Annotated[
            Union[
                GetItems_fieldsConnectionFieldsconnectionNodesBaseBinField,
                GetItems_fieldsConnectionFieldsconnectionNodesBaseCalculatedField,
                GetItems_fieldsConnectionFieldsconnectionNodesBaseColumnField,
                GetItems_fieldsConnectionFieldsconnectionNodesBaseCombinedField,
                GetItems_fieldsConnectionFieldsconnectionNodesBaseCombinedSetField,
                GetItems_fieldsConnectionFieldsconnectionNodesBaseDatasourceField,
                GetItems_fieldsConnectionFieldsconnectionNodesBaseGroupField,
                GetItems_fieldsConnectionFieldsconnectionNodesBaseHierarchyField,
                GetItems_fieldsConnectionFieldsconnectionNodesBaseSetField,
            ],
            Field(discriminator="typename"),
        ]
    ]
    "List of nodes"
    page_info: GetItems_fieldsConnectionFieldsconnectionPageinfo = Field(
        alias="pageInfo"
    )
    "Information for pagination"


class GetItems_fieldsConnection(BaseModel):
    fields_connection: GetItems_fieldsConnectionFieldsconnection = Field(
        alias="fieldsConnection"
    )
    "Fetch Fields with support for pagination"

    class Arguments(BaseModel):
        first: Optional[int] = Field(default=None)
        after: Optional[str] = Field(default=None)

    class Meta:
        document = 'query GetItems_fieldsConnection($first: Int, $after: String) {\n  fieldsConnection(\n    first: $first\n    after: $after\n    filter: {idWithin: ["668470bc-f0c9-ec5d-7a4b-172370d6ef45", "66bd48ba-a895-4951-a4e1-d26fc191eae9", "672a63ee-c34b-8cb4-506b-163fcb131a52", "72684adf-d214-0a9a-946c-dd7b3879b51f", "76636a30-dfbf-0107-0190-a0fc6ce201be", "77aa21d6-3b79-0be0-d309-3d36ef1efe71", "8fbf178a-ceef-2c97-3ac8-26f95a004d67", "968608d1-5da4-1a97-e96a-269010d4ef5b", "b5ae252a-c3c0-3a2d-c147-7a39a8de59ef", "bc1641b9-525d-00c5-f163-02519d46f0fd"]}\n  ) {\n    nodes {\n      id\n      upstreamFields {\n        name\n        datasource {\n          id\n          __typename\n        }\n        __typename\n      }\n      upstreamColumns {\n        name\n        table {\n          __typename\n          id\n        }\n        __typename\n      }\n      __typename\n    }\n    pageInfo {\n      hasNextPage\n      endCursor\n      __typename\n    }\n    __typename\n  }\n}'


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesDownstreamsheets(
    BaseModel
):
    """A sheet contained in a published workbook."""

    typename: Literal["Sheet"] = Field(alias="__typename", default="Sheet")
    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtablesDatabaseBase(
    BaseModel
):
    """A database containing tables"""

    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    id: str
    "Unique identifier used by the Metadata API.  Not the same as the numeric ID used on server"


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtablesDatabaseBaseCloudFile(
    GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtablesDatabaseBase,
    BaseModel,
):
    typename: Literal["CloudFile"] = Field(alias="__typename", default="CloudFile")


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtablesDatabaseBaseDataCloud(
    GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtablesDatabaseBase,
    BaseModel,
):
    typename: Literal["DataCloud"] = Field(alias="__typename", default="DataCloud")


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtablesDatabaseBaseDatabaseServer(
    GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtablesDatabaseBase,
    BaseModel,
):
    typename: Literal["DatabaseServer"] = Field(
        alias="__typename", default="DatabaseServer"
    )


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtablesDatabaseBaseFile(
    GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtablesDatabaseBase,
    BaseModel,
):
    typename: Literal["File"] = Field(alias="__typename", default="File")


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtablesDatabaseBaseWebDataConnector(
    GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtablesDatabaseBase,
    BaseModel,
):
    typename: Literal["WebDataConnector"] = Field(
        alias="__typename", default="WebDataConnector"
    )


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtablesColumnsconnection(
    BaseModel
):
    """Connection Type for Column"""

    typename: Literal["ColumnsConnection"] = Field(
        alias="__typename", default="ColumnsConnection"
    )
    total_count: int = Field(alias="totalCount")
    "Total number of objects in connection"


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtables(
    BaseModel
):
    """A table that is contained in a database"""

    typename: Literal["DatabaseTable"] = Field(
        alias="__typename", default="DatabaseTable"
    )
    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"
    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    database: Optional[
        Annotated[
            Union[
                GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtablesDatabaseBaseCloudFile,
                GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtablesDatabaseBaseDataCloud,
                GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtablesDatabaseBaseDatabaseServer,
                GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtablesDatabaseBaseFile,
                GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtablesDatabaseBaseWebDataConnector,
            ],
            Field(discriminator="typename"),
        ]
    ] = Field(default=None)
    "The database to which this table belongs"
    the_schema: Optional[str] = Field(
        default=None, alias="schema"
    )  # !!! schema is a reserved keyword
    "Name of table schema.\n    \nNote: For some databases, such as Amazon Athena and Exasol, the schema attribute may not return the correct schema name for the table. For more information, see https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_model.html#schema_attribute."
    full_name: Optional[str] = Field(default=None, alias="fullName")
    "Fully qualified table name"
    connection_type: Optional[str] = Field(default=None, alias="connectionType")
    "Connection type of parent database"
    description: Optional[str] = Field(default=None)
    "User modifiable description of this table"
    columns_connection: Optional[
        GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtablesColumnsconnection
    ] = Field(default=None, alias="columnsConnection")
    "Columns contained in this table"


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBase(
    BaseModel
):
    """Base GraphQL type for a field"""

    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"
    name: Optional[str] = Field(default=None)
    "Name shown in server"
    description: Optional[str] = Field(default=None)
    "Description of field shown in server and desktop clients"
    is_hidden: Optional[bool] = Field(default=None, alias="isHidden")
    "True if the field is hidden"
    folder_name: Optional[str] = Field(default=None, alias="folderName")
    "Name of folder if the field is in a folder. See https://onlinehelp.tableau.com/current/pro/desktop/en-us/datafields_dwfeatures.html#Organize"


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBaseBinField(
    GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBase,
    BaseModel,
):
    typename: Literal["BinField"] = Field(alias="__typename", default="BinField")


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBaseCalculatedField(
    GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBase,
    BaseModel,
):
    typename: Literal["CalculatedField"] = Field(
        alias="__typename", default="CalculatedField"
    )


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBaseColumnField(
    GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBase,
    BaseModel,
):
    typename: Literal["ColumnField"] = Field(alias="__typename", default="ColumnField")


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBaseCombinedField(
    GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBase,
    BaseModel,
):
    typename: Literal["CombinedField"] = Field(
        alias="__typename", default="CombinedField"
    )


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBaseCombinedSetField(
    GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBase,
    BaseModel,
):
    typename: Literal["CombinedSetField"] = Field(
        alias="__typename", default="CombinedSetField"
    )


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBaseDatasourceField(
    GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBase,
    BaseModel,
):
    typename: Literal["DatasourceField"] = Field(
        alias="__typename", default="DatasourceField"
    )


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBaseGroupField(
    GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBase,
    BaseModel,
):
    typename: Literal["GroupField"] = Field(alias="__typename", default="GroupField")


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBaseHierarchyField(
    GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBase,
    BaseModel,
):
    typename: Literal["HierarchyField"] = Field(
        alias="__typename", default="HierarchyField"
    )


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBaseSetField(
    GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBase,
    BaseModel,
):
    typename: Literal["SetField"] = Field(alias="__typename", default="SetField")


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamdatasources(
    BaseModel
):
    """A Tableau data source that has been published separately to Tableau Server. It can be used by multiple workbooks."""

    typename: Literal["PublishedDatasource"] = Field(
        alias="__typename", default="PublishedDatasource"
    )
    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"
    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesWorkbookOwner(
    BaseModel
):
    """User on a site on Tableau server"""

    typename: Literal["TableauUser"] = Field(alias="__typename", default="TableauUser")
    username: Optional[str] = Field(default=None)
    "Username of this user"


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesWorkbook(
    BaseModel
):
    """Workbooks are used to package up Tableau visualizations (which are called "sheets" in the Metadata API) and data models (which are called "embedded data sources" when they are owned by a workbook)."""

    typename: Literal["Workbook"] = Field(alias="__typename", default="Workbook")
    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"
    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    project_name: Optional[str] = Field(default=None, alias="projectName")
    "The name of the project in which the workbook is visible and usable."
    luid: str
    "Locally unique identifier used for the REST API on the Tableau Server"
    owner: GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesWorkbookOwner
    "User who owns this workbook"


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodes(
    BaseModel
):
    """A data source embedded in a workbook"""

    typename: Literal["EmbeddedDatasource"] = Field(
        alias="__typename", default="EmbeddedDatasource"
    )
    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"
    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    has_extracts: Optional[bool] = Field(default=None, alias="hasExtracts")
    "True if datasource contains extracted data"
    extract_last_refresh_time: Optional[datetime] = Field(
        default=None, alias="extractLastRefreshTime"
    )
    "Time an extract was last fully refreshed"
    extract_last_incremental_update_time: Optional[datetime] = Field(
        default=None, alias="extractLastIncrementalUpdateTime"
    )
    "Time an extract was last incrementally updated"
    extract_last_update_time: Optional[datetime] = Field(
        default=None, alias="extractLastUpdateTime"
    )
    "Time an extract was last updated by either a full refresh, incremental update, or creation"
    downstream_sheets: List[
        GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesDownstreamsheets
    ] = Field(alias="downstreamSheets")
    "Sheets downstream from this data source"
    upstream_tables: List[
        GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamtables
    ] = Field(alias="upstreamTables")
    "Tables upstream from this data source"
    fields: List[
        Annotated[
            Union[
                GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBaseBinField,
                GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBaseCalculatedField,
                GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBaseColumnField,
                GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBaseCombinedField,
                GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBaseCombinedSetField,
                GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBaseDatasourceField,
                GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBaseGroupField,
                GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBaseHierarchyField,
                GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesFieldsBaseSetField,
            ],
            Field(discriminator="typename"),
        ]
    ]
    "Fields, usually measures or dimensions, contained in the data source"
    upstream_datasources: List[
        GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesUpstreamdatasources
    ] = Field(alias="upstreamDatasources")
    "Datasources upstream from this data source"
    workbook: Optional[
        GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodesWorkbook
    ] = Field(default=None)
    "Workbook that contains these embedded datasources"


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionPageinfo(
    BaseModel
):
    """Information about pagination in a connection"""

    typename: Literal["PageInfo"] = Field(alias="__typename", default="PageInfo")
    has_next_page: bool = Field(alias="hasNextPage")
    "Indicates if there are more objects to fetch"
    end_cursor: Optional[str] = Field(default=None, alias="endCursor")
    "Cursor to use in subsequent query to fetch next page of objects"


class GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnection(BaseModel):
    """Connection Type for EmbeddedDatasource"""

    typename: Literal["EmbeddedDatasourcesConnection"] = Field(
        alias="__typename", default="EmbeddedDatasourcesConnection"
    )
    nodes: List[
        GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionNodes
    ]
    "List of nodes"
    page_info: GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnectionPageinfo = Field(
        alias="pageInfo"
    )
    "Information for pagination"


class GetItems_embeddedDatasourcesConnection(BaseModel):
    embedded_datasources_connection: GetItems_embeddedDatasourcesConnectionEmbeddeddatasourcesconnection = Field(
        alias="embeddedDatasourcesConnection"
    )
    "Fetch EmbeddedDatasources with support for pagination"

    class Arguments(BaseModel):
        first: Optional[int] = Field(default=None)
        after: Optional[str] = Field(default=None)

    class Meta:
        document = 'query GetItems_embeddedDatasourcesConnection($first: Int, $after: String) {\n  embeddedDatasourcesConnection(\n    first: $first\n    after: $after\n    filter: {idWithin: ["7437c561-4e94-0283-3462-c6205c2288cd", "797a69b1-c32a-2fd4-62aa-99989c1858a6", "79871b17-d526-17be-df59-e001f7140924", "8f5e3006-6756-8eea-4fd3-44781cea1493", "1c5653d6-c448-0850-108b-5c78aeaf6b51", "6731f648-b756-31ea-fcdd-77309dd3b0c3", "6bd53e72-9fe4-ea86-3d23-14b826c13fa5", "3f46592d-f789-8f48-466e-69606990d589", "415ddd3e-be04-b466-bc7c-5678a4b0a733", "e9b47a00-12ff-72cc-9dfb-d34e0c65095d"]}\n  ) {\n    nodes {\n      __typename\n      id\n      name\n      hasExtracts\n      extractLastRefreshTime\n      extractLastIncrementalUpdateTime\n      extractLastUpdateTime\n      downstreamSheets {\n        name\n        id\n        __typename\n      }\n      upstreamTables {\n        id\n        name\n        database {\n          name\n          id\n          __typename\n        }\n        schema\n        fullName\n        connectionType\n        description\n        columnsConnection {\n          totalCount\n          __typename\n        }\n        __typename\n      }\n      fields {\n        __typename\n        id\n        name\n        description\n        isHidden\n        folderName\n        ... on ColumnField {\n          dataCategory\n          role\n          dataType\n          defaultFormat\n          aggregation\n        }\n        ... on CalculatedField {\n          role\n          dataType\n          defaultFormat\n          aggregation\n          formula\n        }\n        ... on GroupField {\n          role\n          dataType\n        }\n      }\n      upstreamDatasources {\n        id\n        name\n        __typename\n      }\n      workbook {\n        id\n        name\n        projectName\n        luid\n        owner {\n          username\n          __typename\n        }\n        __typename\n      }\n    }\n    pageInfo {\n      hasNextPage\n      endCursor\n      __typename\n    }\n    __typename\n  }\n}'


class GetItems_sheetsConnectionSheetsconnectionNodesTags(BaseModel):
    """A tag associated with content items"""

    typename: Literal["Tag"] = Field(alias="__typename", default="Tag")
    name: Optional[str] = Field(default=None)
    "The name of the tag"


class GetItems_sheetsConnectionSheetsconnectionNodesContainedindashboards(BaseModel):
    """A dashboard contained in a published workbook."""

    typename: Literal["Dashboard"] = Field(alias="__typename", default="Dashboard")
    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    path: Optional[str] = Field(default=None)
    "Server path to dashboard"


class GetItems_sheetsConnectionSheetsconnectionNodesWorkbookOwner(BaseModel):
    """User on a site on Tableau server"""

    typename: Literal["TableauUser"] = Field(alias="__typename", default="TableauUser")
    username: Optional[str] = Field(default=None)
    "Username of this user"


class GetItems_sheetsConnectionSheetsconnectionNodesWorkbook(BaseModel):
    """Workbooks are used to package up Tableau visualizations (which are called "sheets" in the Metadata API) and data models (which are called "embedded data sources" when they are owned by a workbook)."""

    typename: Literal["Workbook"] = Field(alias="__typename", default="Workbook")
    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"
    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    project_name: Optional[str] = Field(default=None, alias="projectName")
    "The name of the project in which the workbook is visible and usable."
    luid: str
    "Locally unique identifier used for the REST API on the Tableau Server"
    owner: GetItems_sheetsConnectionSheetsconnectionNodesWorkbookOwner
    "User who owns this workbook"


class GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsDatasourceBase(
    BaseModel
):
    """Root GraphQL type for embedded and published data sources

    Data sources are a way to represent how Tableau Desktop and Tableau Server model and connect to data. Data sources can be published separately, as a published data source, or may be contained in a workbook as an embedded data source.

    See https://onlinehelp.tableau.com/current/server/en-us/datasource.htm"""

    id: str
    "Unique identifier used by the metadata API. Not the same as the numeric ID used on server"
    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"


class GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsDatasourceBaseEmbeddedDatasource(
    GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsDatasourceBase,
    BaseModel,
):
    typename: Literal["EmbeddedDatasource"] = Field(
        alias="__typename", default="EmbeddedDatasource"
    )


class GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsDatasourceBasePublishedDatasource(
    GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsDatasourceBase,
    BaseModel,
):
    typename: Literal["PublishedDatasource"] = Field(
        alias="__typename", default="PublishedDatasource"
    )


class GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBase(BaseModel):
    """Base GraphQL type for a field"""

    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"
    name: Optional[str] = Field(default=None)
    "Name shown in server"
    description: Optional[str] = Field(default=None)
    "Description of field shown in server and desktop clients"
    datasource: Optional[
        Annotated[
            Union[
                GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsDatasourceBaseEmbeddedDatasource,
                GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsDatasourceBasePublishedDatasource,
            ],
            Field(discriminator="typename"),
        ]
    ] = Field(default=None)
    "Data source that contains this field"


class GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBaseBinField(
    GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBase, BaseModel
):
    typename: Literal["BinField"] = Field(alias="__typename", default="BinField")


class GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBaseCalculatedField(
    GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBase, BaseModel
):
    typename: Literal["CalculatedField"] = Field(
        alias="__typename", default="CalculatedField"
    )


class GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBaseColumnField(
    GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBase, BaseModel
):
    typename: Literal["ColumnField"] = Field(alias="__typename", default="ColumnField")


class GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBaseCombinedField(
    GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBase, BaseModel
):
    typename: Literal["CombinedField"] = Field(
        alias="__typename", default="CombinedField"
    )


class GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBaseCombinedSetField(
    GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBase, BaseModel
):
    typename: Literal["CombinedSetField"] = Field(
        alias="__typename", default="CombinedSetField"
    )


class GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBaseDatasourceField(
    GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBase, BaseModel
):
    typename: Literal["DatasourceField"] = Field(
        alias="__typename", default="DatasourceField"
    )


class GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBaseGroupField(
    GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBase, BaseModel
):
    typename: Literal["GroupField"] = Field(alias="__typename", default="GroupField")


class GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBaseHierarchyField(
    GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBase, BaseModel
):
    typename: Literal["HierarchyField"] = Field(
        alias="__typename", default="HierarchyField"
    )


class GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBaseSetField(
    GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBase, BaseModel
):
    typename: Literal["SetField"] = Field(alias="__typename", default="SetField")


class GetItems_sheetsConnectionSheetsconnectionNodes(BaseModel):
    """A sheet contained in a published workbook."""

    typename: Literal["Sheet"] = Field(alias="__typename", default="Sheet")
    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"
    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    path: Optional[str] = Field(default=None)
    "Server path to sheet"
    luid: str
    "Locally unique identifier used for the REST API on the Tableau Server (Blank if worksheet is hidden in Workbook)"
    created_at: datetime = Field(alias="createdAt")
    "Time the sheet was created"
    updated_at: datetime = Field(alias="updatedAt")
    "Time the sheet was updated"
    tags: List[GetItems_sheetsConnectionSheetsconnectionNodesTags]
    "Tags associated with the view"
    contained_in_dashboards: Optional[
        List[
            Optional[
                GetItems_sheetsConnectionSheetsconnectionNodesContainedindashboards
            ]
        ]
    ] = Field(default=None, alias="containedInDashboards")
    "Dashboards that contain this sheet"
    workbook: Optional[GetItems_sheetsConnectionSheetsconnectionNodesWorkbook] = Field(
        default=None
    )
    "The workbook that contains this view"
    datasource_fields: Optional[
        List[
            Optional[
                Annotated[
                    Union[
                        GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBaseBinField,
                        GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBaseCalculatedField,
                        GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBaseColumnField,
                        GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBaseCombinedField,
                        GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBaseCombinedSetField,
                        GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBaseDatasourceField,
                        GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBaseGroupField,
                        GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBaseHierarchyField,
                        GetItems_sheetsConnectionSheetsconnectionNodesDatasourcefieldsBaseSetField,
                    ],
                    Field(discriminator="typename"),
                ]
            ]
        ]
    ] = Field(default=None, alias="datasourceFields")
    "Fields that are contained in an embedded data source and are also referenced by the worksheet. If a worksheet uses calculated fields (or any other FieldReferencingField), this list will also include all of the referenced fields."


class GetItems_sheetsConnectionSheetsconnectionPageinfo(BaseModel):
    """Information about pagination in a connection"""

    typename: Literal["PageInfo"] = Field(alias="__typename", default="PageInfo")
    has_next_page: bool = Field(alias="hasNextPage")
    "Indicates if there are more objects to fetch"
    end_cursor: Optional[str] = Field(default=None, alias="endCursor")
    "Cursor to use in subsequent query to fetch next page of objects"


class GetItems_sheetsConnectionSheetsconnection(BaseModel):
    """Connection Type for Sheet"""

    typename: Literal["SheetsConnection"] = Field(
        alias="__typename", default="SheetsConnection"
    )
    nodes: List[GetItems_sheetsConnectionSheetsconnectionNodes]
    "List of nodes"
    page_info: GetItems_sheetsConnectionSheetsconnectionPageinfo = Field(
        alias="pageInfo"
    )
    "Information for pagination"


class GetItems_sheetsConnection(BaseModel):
    sheets_connection: GetItems_sheetsConnectionSheetsconnection = Field(
        alias="sheetsConnection"
    )
    "Fetch Sheets with support for pagination"

    class Arguments(BaseModel):
        first: Optional[int] = Field(default=None)
        after: Optional[str] = Field(default=None)

    class Meta:
        document = 'query GetItems_sheetsConnection($first: Int, $after: String) {\n  sheetsConnection(\n    first: $first\n    after: $after\n    filter: {idWithin: ["fa9e30e8-645e-1105-642d-c292c70a921c", "cea027c2-24a2-d009-4ccf-ac172a3fac6e", "b4f94b9f-26dc-3fb3-1973-796e4c91cb21", "f6682a87-7396-f12e-2fd1-0424157c6ceb", "8fb398c1-0b18-528a-c3bd-2a03c35528f5", "ffe3435f-3e0b-9618-389c-055cbed13ac9", "4e51108f-3ba4-0749-6518-8104fc62c202", "67f86c94-c102-447a-8752-b1e497bf4551", "cbb0b196-5f2a-ecd4-0b2b-e87db220ff47", "1359177d-c634-2cff-4408-1751152c7fc2"]}\n  ) {\n    nodes {\n      id\n      name\n      path\n      luid\n      createdAt\n      updatedAt\n      tags {\n        name\n        __typename\n      }\n      containedInDashboards {\n        name\n        path\n        __typename\n      }\n      workbook {\n        id\n        name\n        projectName\n        luid\n        owner {\n          username\n          __typename\n        }\n        __typename\n      }\n      datasourceFields {\n        __typename\n        id\n        name\n        description\n        datasource {\n          id\n          name\n          __typename\n        }\n        ... on ColumnField {\n          dataCategory\n          role\n          dataType\n          aggregation\n        }\n        ... on CalculatedField {\n          role\n          dataType\n          aggregation\n          formula\n        }\n        ... on GroupField {\n          role\n          dataType\n        }\n        ... on DatasourceField {\n          remoteField {\n            __typename\n            id\n            name\n            description\n            folderName\n            ... on ColumnField {\n              dataCategory\n              role\n              dataType\n              aggregation\n            }\n            ... on CalculatedField {\n              role\n              dataType\n              aggregation\n              formula\n            }\n            ... on GroupField {\n              role\n              dataType\n            }\n          }\n        }\n      }\n      __typename\n    }\n    pageInfo {\n      hasNextPage\n      endCursor\n      __typename\n    }\n    __typename\n  }\n}'


class GetItems_workbooksConnectionWorkbooksconnectionNodesOwner(BaseModel):
    """User on a site on Tableau server"""

    typename: Literal["TableauUser"] = Field(alias="__typename", default="TableauUser")
    username: Optional[str] = Field(default=None)
    "Username of this user"


class GetItems_workbooksConnectionWorkbooksconnectionNodesTags(BaseModel):
    """A tag associated with content items"""

    typename: Literal["Tag"] = Field(alias="__typename", default="Tag")
    name: Optional[str] = Field(default=None)
    "The name of the tag"


class GetItems_workbooksConnectionWorkbooksconnectionNodesSheets(BaseModel):
    """A sheet contained in a published workbook."""

    typename: Literal["Sheet"] = Field(alias="__typename", default="Sheet")
    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"


class GetItems_workbooksConnectionWorkbooksconnectionNodesDashboards(BaseModel):
    """A dashboard contained in a published workbook."""

    typename: Literal["Dashboard"] = Field(alias="__typename", default="Dashboard")
    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"


class GetItems_workbooksConnectionWorkbooksconnectionNodesEmbeddeddatasources(
    BaseModel
):
    """A data source embedded in a workbook"""

    typename: Literal["EmbeddedDatasource"] = Field(
        alias="__typename", default="EmbeddedDatasource"
    )
    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"


class GetItems_workbooksConnectionWorkbooksconnectionNodes(BaseModel):
    """Workbooks are used to package up Tableau visualizations (which are called "sheets" in the Metadata API) and data models (which are called "embedded data sources" when they are owned by a workbook)."""

    typename: Literal["Workbook"] = Field(alias="__typename", default="Workbook")
    id: str
    "Unique identifier used by the metadata API.  Not the same as the numeric ID used on server"
    name: Optional[str] = Field(default=None)
    "Name shown in server and desktop clients"
    luid: str
    "Locally unique identifier used for the REST API on the Tableau Server"
    uri: Optional[str] = Field(default=None)
    "Uri of the workbook"
    project_name: Optional[str] = Field(default=None, alias="projectName")
    "The name of the project in which the workbook is visible and usable."
    owner: GetItems_workbooksConnectionWorkbooksconnectionNodesOwner
    "User who owns this workbook"
    description: Optional[str] = Field(default=None)
    "Description of the workbook"
    uri: Optional[str] = Field(default=None)
    "Uri of the workbook"
    created_at: datetime = Field(alias="createdAt")
    "Time the workbook was created"
    updated_at: datetime = Field(alias="updatedAt")
    "Time the workbook was updated"
    tags: List[GetItems_workbooksConnectionWorkbooksconnectionNodesTags]
    "Tags associated with the workbook"
    sheets: List[GetItems_workbooksConnectionWorkbooksconnectionNodesSheets]
    "Worksheets that are contained in this workbook"
    dashboards: List[GetItems_workbooksConnectionWorkbooksconnectionNodesDashboards]
    "Dashboards that are contained in this workbook"
    embedded_datasources: List[
        GetItems_workbooksConnectionWorkbooksconnectionNodesEmbeddeddatasources
    ] = Field(alias="embeddedDatasources")
    "Data sources that are embedded in this workbook"


class GetItems_workbooksConnectionWorkbooksconnectionPageinfo(BaseModel):
    """Information about pagination in a connection"""

    typename: Literal["PageInfo"] = Field(alias="__typename", default="PageInfo")
    has_next_page: bool = Field(alias="hasNextPage")
    "Indicates if there are more objects to fetch"
    end_cursor: Optional[str] = Field(default=None, alias="endCursor")
    "Cursor to use in subsequent query to fetch next page of objects"


class GetItems_workbooksConnectionWorkbooksconnection(BaseModel):
    """Connection Type for Workbook"""

    typename: Literal["WorkbooksConnection"] = Field(
        alias="__typename", default="WorkbooksConnection"
    )
    nodes: List[GetItems_workbooksConnectionWorkbooksconnectionNodes]
    "List of nodes"
    page_info: GetItems_workbooksConnectionWorkbooksconnectionPageinfo = Field(
        alias="pageInfo"
    )
    "Information for pagination"


class GetItems_workbooksConnection(BaseModel):
    workbooks_connection: GetItems_workbooksConnectionWorkbooksconnection = Field(
        alias="workbooksConnection"
    )
    "Fetch Workbooks with support for pagination"

    class Arguments(BaseModel):
        first: Optional[int] = Field(default=None)
        after: Optional[str] = Field(default=None)

    class Meta:
        document = 'query GetItems_workbooksConnection($first: Int, $after: String) {\n  workbooksConnection(\n    first: $first\n    after: $after\n    filter: {projectNameWithin: ["project_test2"]}\n  ) {\n    nodes {\n      id\n      name\n      luid\n      uri\n      projectName\n      owner {\n        username\n        __typename\n      }\n      description\n      uri\n      createdAt\n      updatedAt\n      tags {\n        name\n        __typename\n      }\n      sheets {\n        id\n        __typename\n      }\n      dashboards {\n        id\n        __typename\n      }\n      embeddedDatasources {\n        id\n        __typename\n      }\n      __typename\n    }\n    pageInfo {\n      hasNextPage\n      endCursor\n      __typename\n    }\n    __typename\n  }\n}'
