### Prerequisites

In order to ingest metadata from Tableau, you will need:

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
on a Tableau site. Tableau's GraphQL interface is used to extract metadata information. Queries used to extract metadata are located
in `metadata-ingestion/src/datahub/ingestion/source/tableau_common.py`

#### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept              | DataHub Concept                                               | Notes                             |
| --------------------------- | ------------------------------------------------------------- | --------------------------------- |
| `"Tableau"`                 | [Data Platform](../../metamodel/entities/dataPlatform.md)     |  
| Project                 | [Container](../../metamodel/entities/container.md)      | SubType `"Project"`              |
| Embedded DataSource         | [Dataset](../../metamodel/entities/dataset.md)                | SubType `"Embedded Data Source"`  |
| Published DataSource        | [Dataset](../../metamodel/entities/dataset.md)                | SubType `"Published Data Source"` |
| Custom SQL Table            | [Dataset](../../metamodel/entities/dataset.md)                | SubTypes `"View"`, `"Custom SQL"` |
| Embedded or External Tables | [Dataset](../../metamodel/entities/dataset.md)                |                                   |
| Sheet                       | [Chart](../../metamodel/entities/chart.md)                    |                                   |
| Dashboard                   | [Dashboard](../../metamodel/entities/dashboard.md)            |                                   |
| User                        | [User (a.k.a CorpUser)](../../metamodel/entities/corpuser.md) | Optionally Extracted              |
| Workbook                    | [Container](../../metamodel/entities/container.md)            | SubType `"Workbook"`              |
| Tag                         | [Tag](../../metamodel/entities/tag.md)                        | Optionally Extracted              |
| Permissions (Groups)        | [Role](../../metamodel/entities/role)                         | Optionally Extracted              |

#### Lineage

Lineage is emitted as received from Tableau's metadata API for

- Sheets contained within a Dashboard
- Embedded or Published Data Sources depended on by a Sheet
- Published Data Sources upstream to Embedded datasource
- Tables upstream to Embedded or Published Data Source
- Custom SQL datasources upstream to Embedded or Published Data Source
- Tables upstream to Custom SQL Data Source

#### Access Roles

The ingestion can be configured to ingest Tableau permissions as access roles. This enables users to request access to Tableau assets from Datahub.
The Tableau group permissions are fetched from the Tableau API, optionally transformed to your needs, and then added as roles to the Tableau asset in Datahub. Currently, this is only available for Workbooks.

Assuming you have groups in Tableau with names such as `AB_XY00-Tableau-Access_A_123_PROJECT_XY_Consumer` or `AB_XY00-Tableau-Access_A_123_PROJECT_XY_Analyst` and corresponding IAM roles like `AR-Tableau-PROJECT_XY_Consumer` or `AR-Tableau-PROJECT_XY_Analyst`.
Using the recipe below, would filter the groups that end with "_Consumer" and transform the group names into the corresponding IAM roles.
With `group_substring_start` and `group_substring_end` you can define a substring of the group name to be used as role name and `role_prefix` can be used to add a prefix to the generated role name.
The role names then act as a substitute of `$ROLE_NAME` in the `request_url` and result in access request URLs like `https://iam.example.com/accessRequest?role=AR-Tableau-PROJECT_XY_Consumer`, for example.
```
source:
  type: tableau
  config:
    connect_uri: https://tableau.example.com

    access_role_ingestion:
      enable_workbooks: True
      role_prefix: "AR-Tableau-"
      group_substring_start: 29
      role_description: "IAM role required to access this Tableau asset."
      displayed_capabilities: ["Read", "Write", "Delete"]
      request_url: "https://iam.example.com/accessRequest?role=$ROLE_NAME"
      group_name_pattern:
        allow: [ "^.*_Consumer$" ]

    username: "${TABLEAU_USER}"
    password: "${TABLEAU_PASSWORD}"

sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```
You can find more information about the specific fields of the `access_role_ingestion` section in the config details below.

#### Caveats

- Tableau metadata API might return incorrect schema name for tables for some databases, leading to incorrect metadata in DataHub. This source attempts to extract correct schema from databaseTable's fully qualified name, wherever possible. Read [Using the databaseTable object in query](https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_model.html#schema_attribute) for caveats in using schema attribute.

### Troubleshooting

### Why are only some workbooks/custom SQLs/published datasources ingested from the specified project?

This may happen when the Tableau API returns NODE_LIMIT_EXCEEDED error in response to metadata query and returns partial results with message "Showing partial results. , The request exceeded the ‘n’ node limit. Use pagination, additional filtering, or both in the query to adjust results." To resolve this, consider

- reducing the page size using the `page_size` config param in datahub recipe (Defaults to 10).
- increasing tableau configuration [metadata query node limit](https://help.tableau.com/current/server/en-us/cli_configuration-set_tsm.htm#metadata_nodelimit) to higher value.

### `PERMISSIONS_MODE_SWITCHED` error in ingestion report 
This error occurs if the Tableau site is using external assets. For more detail, refer to the Tableau documentation [Manage Permissions for External Assets](https://help.tableau.com/current/online/en-us/dm_perms_assets.htm).

Follow the below steps to enable the derived permissions:

1.  Sign in to Tableau Cloud or Tableau Server as an admin.
2.  From the left navigation pane, click Settings.
3.  On the General tab, under Automatic Access to Metadata about Databases and Tables, select the `Automatically grant authorized users access to metadata about databases and tables` check box.
