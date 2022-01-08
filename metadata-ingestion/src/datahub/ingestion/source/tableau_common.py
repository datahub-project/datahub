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
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass

workbook_graphql_query = """
    {
      id
      name
      luid
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
        }
        upstreamDatasources {
          id,
          name
        }
        upstreamTables {
          id,
          name
        }
        datasourceFields {
          __typename
          id
          name
          description
          upstreamColumns {
            id,
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
        upstreamDatabases {
          name
        }
        upstreamTables {
          name
          schema
          fullName
          connectionType
          description
          contact {
            name
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
          name
        }
        workbook {
          name
          projectName
        }
      }
      upstreamDatasources {
        __typename
        id
        name
        hasExtracts
        extractLastRefreshTime
        extractLastIncrementalUpdateTime
        extractLastUpdateTime
        upstreamDatabases {
          name
        }
        upstreamTables {
          name
          schema
          fullName
          connectionType
          description
          contact {
            name
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
          name
        }
        owner {
          username
        }
        description
        uri
        projectName
      }
    }
"""
datasource_graphql_query = """
{
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
        workbook {
            name
            projectName
            }
        }
    upstreamTables {
        name
        schema
        fullName
        connectionType
        description
        contact {name}
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
    upstreamDatasources {name}
    owner {username}
    description
    uri
    projectName
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
    connectionType
    description
    contact {
        name
        }
    }
}
"""

SOURCE_FIELD_TYPE_MAPPING = {
    "EMPTY": NullTypeClass,
    "NULL": NullTypeClass,
    "I2": NumberTypeClass,
    "I4": NumberTypeClass,
    "R4": NumberTypeClass,
    "R8": NumberTypeClass,
    "CY": NumberTypeClass,
    "DATE": DateTypeClass,
    "BSTR": StringTypeClass,
    "IDISPATCH": NullTypeClass,
    "ERROR": NullTypeClass,
    "BOOL": BooleanTypeClass,
    "VARIANT": NullTypeClass,
    "IUNKNOWN": NullTypeClass,
    "DECIMAL": NumberTypeClass,
    "UI1": NumberTypeClass,
    "ARRAY": ArrayTypeClass,
    "BYREF": NullTypeClass,
    "I1": NumberTypeClass,
    "UI2": NumberTypeClass,
    "UI4": NumberTypeClass,
    "I8": NumberTypeClass,
    "UI8": NumberTypeClass,
    "GUID": NullTypeClass,
    "VECTOR": NullTypeClass,
    "FILETIME": TimeTypeClass,
    "RESERVED": NullTypeClass,
    "BYTES": NullTypeClass,
    "STR": StringTypeClass,
    "WSTR": StringTypeClass,
    "NUMERIC": NumberTypeClass,
    "UDT": NullTypeClass,
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

TARGET_FIELD_TYPE_MAPPING = {
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
}


# TODO list comprehension
def get_tags_from_params(params: List[str] = []) -> GlobalTagsClass:
    """
    generate tags
    """
    tags = []
    for tag in params:
        if tag:
            tags.append(TagAssociationClass(tag=builder.make_tag_urn(tag)))

    return GlobalTagsClass(tags=tags)


@lru_cache(maxsize=None)
# TODO how to make this generic. May be have overrides ?
# TODO take care of datahub friendly mapping
def make_table_urn(
    env: str, upstream_db: str, connection_type: str, schema: str, full_name: str
) -> str:
    final_name = full_name.replace("[", "").replace("]", "")
    if connection_type == "vertica":
        platform = "vertica"
        final_name = "da." + full_name.replace("[", "").replace("]", "")
    elif connection_type == "textscan":
        platform = "local_file"
    elif connection_type == "excel-direct":
        platform = "local_file"
    else:
        # TODO log warning
        platform = connection_type

    urn = builder.make_dataset_urn(
        platform, f"{upstream_db}.{schema}.{final_name}", env
    )
    return urn


def make_dataset_urn(platform, env, *name_parts):
    full_name = ""
    for name in name_parts:
        if full_name:
            full_name += "."
        full_name += name
    urn = builder.make_dataset_urn(platform, full_name, env)
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
        field_value = field.get("remoteField", {}).get(field_name, "")
    else:
        field_value = field.get(field_name, "")
    return field_value


def clean_query(query):
    """
    Clean special chars in query
    """
    query = query.replace("<<", "<").replace(">>", ">").replace("\n\n", "\n")
    query = html.unescape(query)
    return query


# TODO report failure
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
    if "errors" in query_result:
        raise Exception(query_result["errors"])
    return query_result


# TODO get this info from graphQL
def find_sheet_path(sheet_name, dashboards):
    for dashboard in dashboards:
        sheets = dashboard.get("sheets", [])
        for sheet in sheets:
            if sheet.get("name", "") == sheet_name:
                return f"{dashboard.get('path', '')}/{sheet.get('name', '')}"
    return None
