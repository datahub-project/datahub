# Tableau

![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)

## Integration Details

This plugin extracts Sheets, Dashboards, Embedded and Published Data sources metadata within Workbooks in a given project
on a Tableau site. This plugin is in beta and has only been tested on PostgreSQL database and sample workbooks 
on Tableau online. Tableau's GraphQL interface is used to extract metadata information. Queries used to extract metadata are located
in `metadata-ingestion/src/datahub/ingestion/source/tableau_common.py`

### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept | DataHub Concept | Notes |
| -- | -- | -- |
| `Tableau` | [Data Platform](../../docs/generated/metamodel/entities/dataPlatform.md) | |
| Embedded DataSource | [Dataset](../../docs/generated/metamodel/entities/dataset.md) | |
| Published DataSource | [Dataset](../../docs/generated/metamodel/entities/dataset.md) | |
| Custom SQL Table | [Dataset](../../docs/generated/metamodel/entities/dataset.md) | |
| Embedded or External Tables | [Dataset](../../docs/generated/metamodel/entities/dataset.md) | |
| Sheet | [Chart](../../docs/generated/metamodel/entities/chart.md) | |
| Dashboard | [Dashboard](../../docs/generated/metamodel/entities/dashboard.md) | |
| User | [User (a.k.a CorpUser)](../../docs/generated/metamodel/entities/corpuser.md) | |
| Workbook | [Container](../../docs/generated/metamodel/entities/container.md) | | 
| Tag | [Tag](../../docs/generated/metamodel/entities/tag.md) | | 


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

#### Dashboard
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

#### Published Data Source
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

#### Custom SQL Data Source
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


### Supported Capabilities

<!-- This should be an auto-generated table of supported DataHub features/functionality -->
<!-- Each capability should link out to a feature guide -->

| Capability | Status | Notes |
| --- | :-: | --- |
| Data Container | ✅ | Enabled by default |
| Detect Deleted Entities | ❌ | |
| Data Domain | ❌ | Requires transformer |
| Dataset Profiling | ❌ | |
| Dataset Usage | ❌ | |
| Extract Descriptions | ✅ | Enabled by default |
| Extract Lineage | ✅ | Enabled by default |
| Extract Ownership | ✅ | Requires recipe configuration |
| Extract Tags | ✅ | Requires recipe configuration |
| Partition Support | ❌ | Not applicable to source |
| Platform Instance | ❌ | Not applicable to source |

## Metadata Ingestion Quickstart

### Prerequisites

In order to ingest metadata from tableau, you will need:

- Python 3.6+
- Tableau Server Version 2021.1.10 and above. It may also work for older versions.
- [Enable the Tableau Metadata API](https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_start.html#enable-the-tableau-metadata-api-for-tableau-server) for Tableau Server, if its not already enabled.
- Tableau Credentials (Username/Password or [Personal Access Token](https://help.tableau.com/current/pro/desktop/en-us/useracct.htm#create-and-revoke-personal-access-tokens)) 


### Install the Plugin(s)

Run the following commands to install the relevant plugin(s):

`pip install 'acryl-datahub[tableau]'`

### Configure the Ingestion Recipe(s)

Use the following recipe(s) to get started with ingestion. 

_For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes)._

#### `'acryl-datahub[tableau]'`

```yml
source:
  type: tableau
  config:
    # Coordinates
    connect_uri: https://prod-ca-a.online.tableau.com
    site: acryl
    projects: ["default", "Project 2"]
    
    # Credentials
    username: "${TABLEAU_USER}"
    password: "${TABLEAU_PASSWORD}"

    # Options
    ingest_tags: True
    ingest_owner: True
    default_schema_map:
      mydatabase: public
      anotherdatabase: anotherschema

sink:
  # sink configs
```

<details>
  <summary>View All Recipe Configuartion Options</summary>
  
  
| Field                 | Required | Default   | Description                                                              |
|-----------------------|----------|-----------|--------------------------------------------------------------------------|
| `connect_uri`         | ✅        |           | Tableau host URL.                                                        |
| `site`                |          |  `""`    | Tableau Site. Always required for Tableau Online. Use emptystring "" to connect with Default site on Tableau Server.   |
| `env`                 |          | `"PROD"`  | Environment to use in namespace when constructing URNs.                  |
| `username`            |          |           | Tableau username, must be set if authenticating using username/password.    |
| `password`            |          |           | Tableau password, must be set if authenticating using username/password.    |
| `token_name`          |          |           | Tableau token name, must be set if authenticating using a personal access token.  |
| `token_value`         |          |           | Tableau token value, must be set if authenticating using a personal access token. |
| `projects`            |          | `["default"]` | List of projects                                                         |
| `workbooks_page_size`            |          | 10 | Number of workbooks to query at a time using Tableau api.                                              |
| `default_schema_map`* |          |           | Default schema to use when schema is not found.                          |
| `ingest_tags`         |          | `False`   | Ingest Tags from source. This will override Tags entered from UI         |
| `ingest_owners`       |          | `False`   | Ingest Owner from source. This will override Owner info entered from UI  |

*Tableau may not provide schema name when ingesting Custom SQL data source. Use `default_schema_map` to provide a default
schema name to use when constructing a table URN.

</details>


## Troubleshooting

### Why are only some workbooks ingested from the specified project?

This happens when the Tableau API returns NODE_LIMIT_EXCEEDED error and returns partial results with message "Showing partial results. , The request exceeded the ‘n’ node limit. Use pagination, additional filtering, or both in the query to adjust results." To resolve this, reduce the page size using the `workbooks_page_size` config param (Defaults to 10).
