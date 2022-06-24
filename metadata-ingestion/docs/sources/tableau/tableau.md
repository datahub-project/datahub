### Prerequisites

In order to ingest metadata from tableau, you will need:

- Python 3.6+
- Tableau Server Version 2021.1.10 and above. It may also work for older versions.
- [Enable the Tableau Metadata API](https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_start.html#enable-the-tableau-metadata-api-for-tableau-server) for Tableau Server, if its not already enabled.
- Tableau Credentials (Username/Password or [Personal Access Token](https://help.tableau.com/current/pro/desktop/en-us/useracct.htm#create-and-revoke-personal-access-tokens)) 

## Integration Details

This plugin extracts Sheets, Dashboards, Embedded and Published Data sources metadata within Workbooks in a given project
on a Tableau site. This plugin is in beta and has only been tested on PostgreSQL database and sample workbooks 
on Tableau online. Tableau's GraphQL interface is used to extract metadata information. Queries used to extract metadata are located
in `metadata-ingestion/src/datahub/ingestion/source/tableau_common.py`

### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept | DataHub Concept | Notes |
| -- | -- | -- |
| `"Tableau"` | [Data Platform](../../metamodel/entities/dataPlatform.md) | |
| Embedded DataSource | [Dataset](../../metamodel/entities/dataset.md) | SubType `"Embedded Data Source"` |
| Published DataSource | [Dataset](../../metamodel/entities/dataset.md) | SubType `"Published Data Source"` |
| Custom SQL Table | [Dataset](../../metamodel/entities/dataset.md) | SubTypes `"View"`, `"Custom SQL"` |
| Embedded or External Tables | [Dataset](../../metamodel/entities/dataset.md) | |
| Sheet | [Chart](../../metamodel/entities/chart.md) | |
| Dashboard | [Dashboard](../../metamodel/entities/dashboard.md) | |
| User | [User (a.k.a CorpUser)](../../metamodel/entities/corpuser.md) | |
| Workbook | [Container](../../metamodel/entities/container.md) | SubType `"Workbook"` | 
| Tag | [Tag](../../metamodel/entities/tag.md) | | 


- [Workbook](#Workbook)
- [Dashboard](#Dashboard)
- [Sheet](#Sheet)
- [Embedded Data source](#Embedded-Data-Source)
- [Published Data source](#Published-Data-Source)
- [Custom SQL Data source](#Custom-SQL-Data-Source)

#### Workbook
Workbooks from Tableau are ingested as Container in datahub. <br/>
- GraphQL query <br/>
```graphql
{
  workbooksConnection(first: 10, offset: 0, filter: {projectNameWithin: ["default", "Project 2"]}) {
    nodes {
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
    }
    pageInfo {
      hasNextPage
      endCursor
    }
    totalCount
  }
}
```

#### Dashboard
Dashboards from Tableau are ingested as Dashboard in datahub. <br/>
- GraphQL query <br/>
```graphql
{
  workbooksConnection(first: 10, offset: 0, filter: {projectNameWithin: ["default", "Project 2"]}) {
    nodes {
      .....
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
    }
    pageInfo {
      hasNextPage
      endCursor
    }
    totalCount
  }
}

```

#### Sheet
Sheets from Tableau are ingested as charts in datahub. <br/>
- GraphQL query <br/>
```graphql
{
  workbooksConnection(first: 10, offset: 0, filter: {projectNameWithin: ["default"]}) {
    .....
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
        upstreamDatasources {
          id
          name
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
    }
     .....
  }
}
```

#### Embedded Data Source
Embedded Data source from Tableau is ingested as a Dataset in datahub.

- GraphQL query <br/>
```graphql
{
  workbooksConnection(first: 10, offset: 0, filter: {projectNameWithin: ["default"]}) {
    nodes {
      ....
      embeddedDatasources {
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
          name
          schema
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
    }
    ....
  }
}
```

#### Published Data Source
Published Data source from Tableau is ingested as a Dataset in datahub.

- GraphQL query <br/>
```graphql
{
  publishedDatasourcesConnection(first: 10, offset: 0, filter: {idWithin: ["00cce29f-b561-bb41-3557-8e19660bb5dd", "618c87db-5959-338b-bcc7-6f5f4cc0b6c6"]}) {
    nodes {
      __typename
      id
      name
      hasExtracts
      extractLastRefreshTime
      extractLastIncrementalUpdateTime
      extractLastUpdateTime
      downstreamSheets {
        id
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
      owner {
        username
      }
      description
      uri
      projectName
    }
    pageInfo {
      hasNextPage
      endCursor
    }
    totalCount
  }
}
```

#### Custom SQL Data Source
For custom sql data sources, the query is viewable in UI under View Definition tab. <br/>
- GraphQL query <br/>
```graphql
{
  customSQLTablesConnection(first: 10, offset: 0, filter: {idWithin: ["22b0b4c3-6b85-713d-a161-5a87fdd78f40"]}) {
    nodes {
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
            id
            name
            upstreamDatabases {
              id
              name
            }
            upstreamTables {
              id
              name
              schema
              connectionType
              columns {
                id
              }
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
        id
        name
        schema
        connectionType
      }
    }
  }
}
```

#### Lineage
Lineage is emitted as received from Tableau's metadata API for
- Sheets contained in Dashboard
- Embedded or Published datasources upstream to Sheet
- Published datasources upstream to Embedded datasource
- Tables upstream to Embedded or Published datasource
- Custom SQL datasources upstream to Embedded or Published datasource
- Tables upstream to Custom SQL datasource


#### Caveats
- Tableau metadata API might return incorrect schema name for tables for some databases, leading to incorrect metadata in DataHub. This source attempts to extract correct schema from databaseTable's fully qualified name, wherever possible. Read [Using the databaseTable object in query](https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_model.html#schema_attribute) for caveats in using schema attribute.

## Troubleshooting

### Why are only some workbooks/custom SQLs/published datasources ingested from the specified project?

This may happen when the Tableau API returns NODE_LIMIT_EXCEEDED error in response to metadata query and returns partial results with message "Showing partial results. , The request exceeded the ‘n’ node limit. Use pagination, additional filtering, or both in the query to adjust results." To resolve this, consider 
- reducing the page size using the `page_size` config param in datahub recipe (Defaults to 10).
- increasing tableau configuration [metadata query node limit](https://help.tableau.com/current/server/en-us/cli_configuration-set_tsm.htm#metadata_nodelimit) to higher value. 