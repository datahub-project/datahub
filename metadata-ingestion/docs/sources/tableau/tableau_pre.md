### Prerequisites

In order to ingest metadata from tableau, you will need:

- Tableau Server Version 2021.1.10 and above. It may also work for older versions.
- [Enable the Tableau Metadata API](https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_start.html#enable-the-tableau-metadata-api-for-tableau-server) for Tableau Server, if its not already enabled.
- Tableau Credentials (Username/Password or [Personal Access Token](https://help.tableau.com/current/pro/desktop/en-us/useracct.htm#create-and-revoke-personal-access-tokens))
- The user or token must have **Site Administrator Explorer** permissions.

### Ingestion through UI

The following video shows you how to get started with ingesting Tableau metadata through the UI.

<div
  style={{
    position: "relative",
    paddingBottom: "57.692307692307686%",
    height: 0
  }}
>
  <iframe
    src="https://www.loom.com/embed/ef521c4e66564614a6ddde35dc3840f8"
    frameBorder={0}
    webkitallowfullscreen=""
    mozallowfullscreen=""
    allowFullScreen=""
    style={{
      position: "absolute",
      top: 0,
      left: 0,
      width: "100%",
      height: "100%"
    }}
  />
</div>

### Integration Details

This plugin extracts Sheets, Dashboards, Embedded and Published Data sources metadata within Workbooks in a given project
on a Tableau site. This plugin is in beta and has only been tested on PostgreSQL database and sample workbooks
on Tableau online. Tableau's GraphQL interface is used to extract metadata information. Queries used to extract metadata are located
in `metadata-ingestion/src/datahub/ingestion/source/tableau_common.py`

#### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept              | DataHub Concept                                               | Notes                                                                       |
| --------------------------- | ------------------------------------------------------------- | --------------------------------------------------------------------------- |
| `"Tableau"`                 | [Data Platform](../../metamodel/entities/dataPlatform.md)     |                                                                             |
| Embedded DataSource         | [Dataset](../../metamodel/entities/dataset.md)                | SubType `"Embedded Data Source"`                                            |
| Published DataSource        | [Dataset](../../metamodel/entities/dataset.md)                | SubType `"Published Data Source"`                                           |
| Custom SQL Table            | [Dataset](../../metamodel/entities/dataset.md)                | SubTypes `"View"`, `"Custom SQL"`, Raw SQL shown in the View Definition tab |
| Embedded or External Tables | [Dataset](../../metamodel/entities/dataset.md)                |                                                                             |
| Sheet                       | [Chart](../../metamodel/entities/chart.md)                    |                                                                             |
| Dashboard                   | [Dashboard](../../metamodel/entities/dashboard.md)            |                                                                             |
| User                        | [User (a.k.a CorpUser)](../../metamodel/entities/corpuser.md) |                                                                             |
| Workbook                    | [Container](../../metamodel/entities/container.md)            | SubType `"Workbook"`                                                        |
| Tag                         | [Tag](../../metamodel/entities/tag.md)                        |                                                                             |

- [Workbook](#Workbook)
- [Dashboard](#Dashboard)
- [Sheet](#Sheet)
- [Embedded Data source](#Embedded-Data-Source)
- [Published Data source](#Published-Data-Source)
- [Custom SQL Data source](#Custom-SQL-Data-Source)

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

### Troubleshooting

### Why are only some workbooks/custom SQLs/published datasources ingested from the specified project?

This may happen when the Tableau API returns NODE_LIMIT_EXCEEDED error in response to metadata query and returns partial results with message "Showing partial results. , The request exceeded the ‘n’ node limit. Use pagination, additional filtering, or both in the query to adjust results." To resolve this, consider

- reducing the page size using the `page_size` config param in datahub recipe (Defaults to 10).
- increasing tableau configuration [metadata query node limit](https://help.tableau.com/current/server/en-us/cli_configuration-set_tsm.htm#metadata_nodelimit) to higher value.
