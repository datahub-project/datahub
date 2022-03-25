# Tableau
For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

Note that this connector is currently considered in `BETA`, and has not been validated for production use. 

## Setup

To install this plugin, run `pip install 'acryl-datahub[tableau]'`.

See documentation for Tableau's metadata API at https://help.tableau.com/current/api/metadata_api/en-us/index.html

## Capabilities

This plugin extracts Sheets, Dashboards, Embedded and Published Data sources metadata within Workbooks in a given project
on a Tableau Online site. This plugin is in beta and has only been tested on PostgreSQL database and sample workbooks 
on Tableau online.

Tableau's GraphQL interface is used to extract metadata information. Queries used to extract metadata are located
in `metadata-ingestion/src/datahub/ingestion/source/tableau_common.py`

- [Workbook](#Workbook)
- [Dashboard](#Dashboard)
- [Sheet](#Sheet)
- [Embedded Data source](#Embedded-Data-Source)
- [Published Data source](#Published-Data-Source)
- [Custom SQL Data source](#Custom-SQL-Data-Source)


### Workbook
Workbooks from Tableau are ingested as Container in datahub. <br/>
- GraphQL query <br/>
```graphql
{
  workbooksConnection(first: 15, offset: 0, filter: {projectNameWithin: ["default", "Project 2"]}) {
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

### Dashboard
Dashboards from Tableau are ingested as Dashboard in datahub. <br/>
- GraphQL query <br/>
```graphql
{
  workbooksConnection(first: 15, offset: 0, filter: {projectNameWithin: ["default", "Project 2"]}) {
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

### Sheet
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

### Embedded Data Source
Embedded Data source from Tableau is ingested as a Dataset in datahub.

- GraphQL query <br/>
```graphql
{
  workbooksConnection(first: 15, offset: 0, filter: {projectNameWithin: ["default"]}) {
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

### Published Data Source
Published Data source from Tableau is ingested as a Dataset in datahub.

- GraphQL query <br/>
```graphql
{
  publishedDatasourcesConnection(filter: {idWithin: ["00cce29f-b561-bb41-3557-8e19660bb5dd", "618c87db-5959-338b-bcc7-6f5f4cc0b6c6"]}) {
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

### Custom SQL Data Source
For custom sql data sources, the query is viewable in UI under View Definition tab. <br/>
- GraphQL query <br/>
```graphql
{
  customSQLTablesConnection(filter: {idWithin: ["22b0b4c3-6b85-713d-a161-5a87fdd78f40"]}) {
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

## Quickstart recipe

Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.

For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes).

```yml
source:
  type: tableau
  config:
    # Coordinates
    connect_uri: https://prod-ca-a.online.tableau.com
    site: acryl
    projects: ["default", "Project 2"]
    
    # Credentials
    username: username@acrylio.com
    password: pass

    # Options
    ingest_tags: True
    ingest_owner: True
    default_schema_map:
      mydatabase: public
      anotherdatabase: anotherschema
    
sink:
  # sink configs
```

## Config details

| Field                 | Required | Default   | Description                                                              |
|-----------------------|----------|-----------|--------------------------------------------------------------------------|
| `connect_uri`         | ✅        |           | Tableau host URL.                                                        |
| `site`                |          |  `""`    | Tableau Site. Always required for Tableau Online. Use emptystring "" to connect with Default site on Tableau Server.   |
| `env`                 |          | `"PROD"`  | Environment to use in namespace when constructing URNs.                  |
| `username`            |          |           | Tableau username, must be set if authenticating using username/password.    |
| `password`            |          |           | Tableau password, must be set if authenticating using username/password.    |
| `token_name`          |          |           | Tableau token name, must be set if authenticating using a personal access token.  |
| `token_value`         |          |           | Tableau token value, must be set if authenticating using a personal access token. |
| `projects`            |          | `default` | List of projects                                                         |
| `workbooks_page_size`            |          | 10 | Number of workbooks to query at a time using Tableau api.                                              |
| `default_schema_map`* |          |           | Default schema to use when schema is not found.                          |
| `ingest_tags`         |          | `False`   | Ingest Tags from source. This will override Tags entered from UI         |
| `ingest_owners`       |          | `False`   | Ingest Owner from source. This will override Owner info entered from UI  |

*Tableau may not provide schema name when ingesting Custom SQL data source. Use `default_schema_map` to provide a default
schema name to use when constructing a table URN.


### Authentication

Currently, authentication is supported on Tableau using username and password
and personal token. For more information on Tableau authentication, refer to [How to Authenticate](https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_auth.html) guide.


## Compatibility

Tableau Server Version: 2021.4.0 (20214.22.0114.0959) 64-bit Linux 


## Questions

If you've got any questions on configuring this source, feel free to ping us on
[our Slack](https://slack.datahubproject.io/)!
