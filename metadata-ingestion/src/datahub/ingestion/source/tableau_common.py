import html
from functools import lru_cache
from typing import List

import datahub.emitter.mce_builder as builder
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    ArrayTypeClass,
    BooleanTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.metadata.schema_classes import (
    BytesTypeClass,
    GlobalTagsClass,
    TagAssociationClass,
)


class MetadataQueryException(Exception):
    pass


workbook_graphql_query = """
    {
      id
      name
      luid
      uri
      projectName
      owner {
        username
      }
      description
      uri
      createdAt
      updatedAt
      sheets {
        id
        name
        path
        createdAt
        updatedAt
        tags {
          name
        }
        containedInDashboards {
          name
          path
        }
        datasourceFields {
          __typename
          id
          name
          description
          upstreamColumns {
            name
          }
          ... on ColumnField {
            dataCategory
            role
            dataType
            aggregation
          }
          ... on CalculatedField {
            role
            dataType
            aggregation
            formula
          }
          ... on GroupField {
            role
            dataType
          }
          ... on DatasourceField {
            remoteField {
              __typename
              id
              name
              description
              folderName
              ... on ColumnField {
                dataCategory
                role
                dataType
                aggregation
              }
              ... on CalculatedField {
                role
                dataType
                aggregation
                formula
              }
              ... on GroupField {
                role
                dataType
              }
            }
          }
        }
      }
      dashboards {
        id
        name
        path
        createdAt
        updatedAt
        sheets {
          id
          name
        }
      }
      embeddedDatasources {
        __typename
        id
        name
        hasExtracts
        extractLastRefreshTime
        extractLastIncrementalUpdateTime
        extractLastUpdateTime
        downstreamSheets {
          name
          id
        }
        upstreamDatabases {
          id
          name
          connectionType
          isEmbedded
        }
        upstreamTables {
          id
          name
          database {
            name
          }
          schema
          fullName
          connectionType
          description
          columns {
            name
            remoteType
          }
        }
        fields {
          __typename
          id
          name
          description
          isHidden
          folderName
          ... on ColumnField {
            dataCategory
            role
            dataType
            defaultFormat
            aggregation
            columns {
              table {
                ... on CustomSQLTable {
                  id
                  name
                }
              }
            }
          }
          ... on CalculatedField {
            role
            dataType
            defaultFormat
            aggregation
            formula
          }
          ... on GroupField {
            role
            dataType
          }
        }
        upstreamDatasources {
          id
          name
        }
        workbook {
          name
          projectName
        }
      }
      upstreamDatasources {
        name
        id
        downstreamSheets {
          name
          id
        }
      }
    }
"""

custom_sql_graphql_query = """
{
      id
      name
      query
      columns {
        id
        name
        remoteType
        description
        referencedByFields {
          datasource {
            __typename
            id
            name
            upstreamDatabases {
              id
              name
            }
            upstreamTables {
              id
              name
              database {
                name
              }
              schema
              fullName
              connectionType
            }
            ... on PublishedDatasource {
              projectName
            }
            ... on EmbeddedDatasource {
              workbook {
                name
                projectName
              }
            }
          }
        }
      }
      tables {
        name
        schema
        fullName
        database {
          name
        }
        connectionType
      }
}
"""

published_datasource_graphql_query = """
{
    __typename
    id
    name
    hasExtracts
    extractLastRefreshTime
    extractLastIncrementalUpdateTime
    extractLastUpdateTime
    upstreamDatabases {
      id
      name
      connectionType
      isEmbedded
    }
    upstreamTables {
      id
      name
      database {
        name
      }
      schema
      fullName
      connectionType
      description
      columns {
        name
        remoteType
      }
    }
    fields {
        __typename
        id
        name
        description
        isHidden
        folderName
        ... on ColumnField {
            dataCategory
            role
            dataType
            defaultFormat
            aggregation
            columns {
                table {
                    ... on CustomSQLTable {
                        id
                        name
                        }
                    }
                }
            }
        ... on CalculatedField {
            role
            dataType
            defaultFormat
            aggregation
            formula
            }
        ... on GroupField {
            role
            dataType
            }
    }
    owner {username}
    description
    uri
    projectName
}
        """

# https://referencesource.microsoft.com/#system.data/System/Data/OleDb/OLEDB_Enum.cs,364
FIELD_TYPE_MAPPING = {
    "INTEGER": NumberTypeClass,
    "REAL": NumberTypeClass,
    "STRING": StringTypeClass,
    "DATETIME": TimeTypeClass,
    "DATE": DateTypeClass,
    "TUPLE": ArrayTypeClass,
    "SPATIAL": NullTypeClass,
    "BOOLEAN": BooleanTypeClass,
    "TABLE": ArrayTypeClass,
    "UNKNOWN": NullTypeClass,
    "EMPTY": NullTypeClass,
    "NULL": NullTypeClass,
    "I2": NumberTypeClass,
    "I4": NumberTypeClass,
    "R4": NumberTypeClass,
    "R8": NumberTypeClass,
    "CY": NumberTypeClass,
    "BSTR": StringTypeClass,
    "IDISPATCH": NullTypeClass,
    "ERROR": NullTypeClass,
    "BOOL": BooleanTypeClass,
    "VARIANT": NullTypeClass,
    "IUNKNOWN": NullTypeClass,
    "DECIMAL": NumberTypeClass,
    "UI1": NumberTypeClass,
    "ARRAY": ArrayTypeClass,
    "BYREF": StringTypeClass,
    "I1": NumberTypeClass,
    "UI2": NumberTypeClass,
    "UI4": NumberTypeClass,
    "I8": NumberTypeClass,
    "UI8": NumberTypeClass,
    "GUID": StringTypeClass,
    "VECTOR": ArrayTypeClass,
    "FILETIME": TimeTypeClass,
    "RESERVED": NullTypeClass,
    "BYTES": BytesTypeClass,
    "STR": StringTypeClass,
    "WSTR": StringTypeClass,
    "NUMERIC": NumberTypeClass,
    "UDT": StringTypeClass,
    "DBDATE": DateTypeClass,
    "DBTIME": TimeTypeClass,
    "DBTIMESTAMP": TimeTypeClass,
    "HCHAPTER": NullTypeClass,
    "PROPVARIANT": NullTypeClass,
    "VARNUMERIC": NumberTypeClass,
    "WDC_INT": NumberTypeClass,
    "WDC_FLOAT": NumberTypeClass,
    "WDC_STRING": StringTypeClass,
    "WDC_DATETIME": TimeTypeClass,
    "WDC_BOOL": BooleanTypeClass,
    "WDC_DATE": DateTypeClass,
    "WDC_GEOMETRY": NullTypeClass,
}


def get_tags_from_params(params: List[str] = []) -> GlobalTagsClass:
    tags = [
        TagAssociationClass(tag=builder.make_tag_urn(tag.upper()))
        for tag in params
        if tag
    ]
    return GlobalTagsClass(tags=tags)


@lru_cache(maxsize=None)
def make_table_urn(
    env: str, upstream_db: str, connection_type: str, schema: str, full_name: str
) -> str:
    # connection_type taken from
    # https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_connectiontype.htm
    #  datahub platform mapping is found here
    # https://github.com/datahub-project/datahub/blob/master/metadata-service/war/src/main/resources/boot/data_platforms.json

    final_name = full_name.replace("[", "").replace("]", "")
    if connection_type in ("textscan", "textclean", "excel-direct", "excel", "csv"):
        platform = "external"
    elif connection_type in (
        "hadoophive",
        "hive",
        "hortonworkshadoophive",
        "maprhadoophive",
        "awshadoophive",
    ):
        platform = "hive"
    elif connection_type in ("mysql_odbc", "mysql"):
        platform = "mysql"
    elif connection_type in ("webdata-direct:oracle-eloqua", "oracle"):
        platform = "oracle"
    elif connection_type in ("tbio", "teradata"):
        platform = "teradata"
    elif connection_type in ("sqlserver"):
        platform = "mssql"
    else:
        platform = connection_type

    database_name = f"{upstream_db}." if upstream_db else ""
    schema_name = f"{schema}." if schema else ""

    urn = builder.make_dataset_urn(
        platform, f"{database_name}{schema_name}{final_name}", env
    )
    return urn


def make_description_from_params(description, formula):
    """
    Generate column description
    """
    final_description = ""
    if description:
        final_description += f"{description}\n\n"
    if formula:
        final_description += f"formula: {formula}"
    return final_description


def get_field_value_in_sheet(field, field_name):
    if field.get("__typename", "") == "DatasourceField":
        field = field.get("remoteField") if field.get("remoteField") else {}

    field_value = field.get(field_name, "")
    return field_value


def get_unique_custom_sql(custom_sql_list: List[dict]) -> List[dict]:
    unique_custom_sql = []
    for custom_sql in custom_sql_list:
        unique_csql = {
            "id": custom_sql.get("id"),
            "name": custom_sql.get("name"),
            "query": custom_sql.get("query"),
            "columns": custom_sql.get("columns"),
            "tables": custom_sql.get("tables"),
        }
        datasource_for_csql = []
        for column in custom_sql.get("columns", []):
            for field in column.get("referencedByFields", []):
                datasource = field.get("datasource")
                if datasource not in datasource_for_csql:
                    datasource_for_csql.append(datasource)

        unique_csql["datasources"] = datasource_for_csql
        unique_custom_sql.append(unique_csql)
    return unique_custom_sql


def clean_query(query):
    """
    Clean special chars in query
    """
    query = query.replace("<<", "<").replace(">>", ">").replace("\n\n", "\n")
    query = html.unescape(query)
    return query


def query_metadata(server, main_query, connection_name, first, offset, qry_filter=""):
    query = """{{
        {connection_name} (first:{first}, offset:{offset}, filter:{{{filter}}})
        {{
            nodes {main_query}
            pageInfo {{
                hasNextPage
                endCursor
            }}
            totalCount
        }}
    }}""".format(
        connection_name=connection_name,
        first=first,
        offset=offset,
        filter=qry_filter,
        main_query=main_query,
    )
    query_result = server.metadata.query(query)

    return query_result
