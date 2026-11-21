


# Qlik Sense

## Overview

Qlik Sense is a business intelligence and analytics platform. Learn more in the [official Qlik Sense documentation](https://www.qlik.com/us/products/qlik-sense).

The DataHub integration for Qlik Sense covers BI entities such as dashboards, charts, datasets, and related ownership context. It also captures table- and column-level lineage, ownership, and stateful deletion detection.

## Concept Mapping

| Qlik Sense | Datahub                                                                                                | Notes                                                                                                                                                                                                                                                                                               |
| ---------- | ------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Space`    | [Container](../../metamodel/entities/container.md)                                                     | SubType `"Qlik Space"`. On-prem streams too.                                                                                                                                                                                                                                                        |
| `App`      | [Container](../../metamodel/entities/container.md), [Dashboard](../../metamodel/entities/dashboard.md) | The container has SubTypes `"Qlik App"` and `"Workbook"`, so the UI treats it as its charts' workbook, as for Tableau. It holds the app's tables, sheets and charts, and a dashboard with SubType `"App"`, which links to the sheets and to the tables no chart reads, so the app shows in lineage. |
| `Sheet`    | [Dashboard](../../metamodel/entities/dashboard.md)                                                     | SubType `"Sheet"`.                                                                                                                                                                                                                                                                                  |
| `Chart`    | [Chart](../../metamodel/entities/chart.md)                                                             |                                                                                                                                                                                                                                                                                                     |
| `Dataset`  | [Dataset](../../metamodel/entities/dataset.md)                                                         | SubType `"Qlik Dataset"`. Qlik Cloud catalog datasets (data files in a space).                                                                                                                                                                                                                      |
| App table  | [Dataset](../../metamodel/entities/dataset.md)                                                         | SubType `"Qlik Table"`. Tables in an app's data model and from its load script.                                                                                                                                                                                                                     |
| QVD        | [Dataset](../../metamodel/entities/dataset.md)                                                         | SubType `"QVD"`. Qlik Cloud QVD datasets, and on-prem QVD files that load scripts read or write.                                                                                                                                                                                                    |
| `User`     | [User (aka CorpUser)](../../metamodel/entities/corpuser.md)                                            | Optionally Extracted                                                                                                                                                                                                                                                                                |


## Module `qlik-sense`
![Alpha](https://img.shields.io/badge/support%20status-Alpha-lightgrey)


### Important Capabilities
| Capability | Status | Notes |
| ---------- | ------ | ----- |
| Asset Containers | ✅ | Enabled by default. |
| Column-level Lineage | ✅ | Enabled by default, can be disabled via configuration `include_column_lineage`. |
| Descriptions | ✅ | Enabled by default. |
| [Detect Deleted Entities](../../../../metadata-ingestion/docs/dev_guides/stateful.md#stale-entity-removal) | ✅ | Enabled by default via stateful ingestion. |
| Extract Ownership | ✅ | Enabled by default, configured using `ingest_owner`. |
| [Platform Instance](../../../platform-instances.md) | ✅ | Enabled by default. |
| Schema Metadata | ✅ | Enabled by default. |
| Table-Level Lineage | ✅ | Enabled by default. |
| Test Connection | ✅ | Enabled by default. |

### Overview

The `qlik-sense` module ingests metadata from Qlik Sense into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This source extracts the following:

- Accessible spaces and apps within that spaces as Container.
- Qlik Datasets as Datahub Datasets with schema metadata.
- Tables in each app's data model, and tables read from its load script, as DataHub Datasets with subtype `Qlik Table`, with schema metadata and upstream lineage when available.
- Sheets as Datahub dashboard and charts present inside sheets.
- On-prem streams, apps, sheets, and charts when the `on_prem` block is set.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

#### Qlik Cloud

1. Refer [doc](https://qlik.dev/authenticate/api-key/generate-your-first-api-key/) to generate an API key from the hub.
2. Get tenant hostname from About tab after login to qlik sense account.

#### Qlik Sense Enterprise on Windows

Set the `on_prem` block instead of `tenant_hostname` and `api_key`. Export the client certificate, client key, and root certificate from the QMC as PEM files. `client_cert`, `client_key`, and `root_cert` are file paths, not file contents, so the files must exist on the machine that runs ingestion. When the source is set up in the DataHub UI, run it on an executor where the files are mounted (on DataHub Cloud, a remote executor). The ingestion host must reach the Repository Service (port 4242) and the Engine (port 4747) on the central node. Certificate calls run with administrator rights. The default accounts are `INTERNAL\sa_repository` for inventory and `INTERNAL\sa_engine` for app content. Set `client_key_password` when the client key was exported with a password. Test connection checks both ports. Each Repository Service request and each Engine connection or reply waits up to `timeout_seconds` (default 300), and an app whose read times out is ingested as partly read.

This mode ingests streams, apps, sheets, and charts, plus lineage from the `SELECT` statements in each app's load script. It does not ingest Qlik Cloud datasets or QVD lineage. In a published app, users' private sheets are skipped, as on Qlik Cloud; base and community sheets are ingested. Owners are ingested by their Qlik user id, which for Windows directories is the account name, so they join with users ingested from Active Directory or LDAP by username.

The server certificate is verified against `root_cert`. Qlik issues it to the central node's hostname, so if you connect by IP address or an alias, set `server_cert_hostname` to the name on the certificate. The certificate chain is still verified when `verify_server_hostname` is `false`, but any certificate signed by that root is accepted for any host.

:::caution
`verify_server_hostname: false` leaves the connection open to impersonation by another server holding a certificate from the same root. Use it only as a last resort, after trying `server_cert_hostname`, a DNS entry that matches the certificate, or a reissued certificate.
:::

```yaml
source:
  type: qlik-sense
  config:
    on_prem:
      server: 10.0.0.10
      server_cert_hostname: qlik-central.example.com
      client_cert: /secrets/client.pem
      client_key: /secrets/client_key.pem
      root_cert: /secrets/root.pem
```


### Install the Plugin
```shell
pip install 'acryl-datahub[qlik-sense]'
```

### Starter Recipe
Check out the following recipe to get started with ingestion! See [below](#config-details) for full configuration options.


For general pointers on writing and running a recipe, see our [main recipe guide](../../../../metadata-ingestion/README.md#recipes).
```yaml
source:
  type: qlik-sense
  config:
    # Coordinates
    tenant_hostname: "xyz12xz.us.qlikcloud.com"
    # Credentials
    api_key: "QLIK_API_KEY"

    # Qlik Sense Enterprise on Windows: set on_prem instead of tenant_hostname and api_key.
    # on_prem:
    #   server: qlik-central.example.com
    #   client_cert: /secrets/client.pem
    #   client_key: /secrets/client_key.pem
    #   root_cert: /secrets/root.pem
    
    # Optional - filter for certain space names instead of ingesting everything.
    # Mention 'personal_space' if entities of personal space need to ingest
    # space_pattern:
    #   allow:
    #     - space_name

    ingest_owner: true
    
    # Optional -- This mapping is optional and only required to configure platform-instance for Qlik app dataset upstream source tables
    # A mapping of the Qlik app dataset upstream tables from data connection to platform instance. Use 'data_connection_name' as key.
    # data_connection_to_platform_instance:
    #   data_connection_name:
    #     platform_instance: cloud_instance
    #     env: DEV
    #     platform: mssql
    #     database: sales_db
    #     default_schema: dbo

sink:
  # sink configs

```

### Config Details

                
#### Options


Note that a `.` is used to denote nested fields in the YAML recipe.


<div className='config-table'>

| Field | Description |
|:--- |:--- |
| <div className="path-line"><span className="path-main">api_key</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Qlik Cloud API key. Required unless `on_prem` is set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">include_column_lineage</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Emit column-level lineage, from load scripts and app data models and, on Qlik Cloud, from the field level of Qlik's lineage graph, which is read for each app table and each dataset an app stores into and also gives the fields of tables a load script drops. The field-level reads cost one request per table. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">include_sheets_and_charts</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Ingest each app's sheets as dashboards and their charts. When false, apps are read without their sheets, which is faster, and each app dashboard links only to its tables. With stale metadata removal, sheets and charts ingested earlier are soft-deleted; raise `stateful_ingestion.fail_safe_threshold` for that first run, as they are usually most of the entities and their removal otherwise trips it. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">ingest_owner</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Ingest Owner from source. This will override Owner info entered from UI <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">max_partial_reads_kept</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | With stateful ingestion and stale metadata removal, how many consecutive runs an app that is only partly read keeps the entities of its last complete read. After that, its partial read is ingested and reported as a failure, so its current state appears and stale metadata removal is skipped for that run. <div className="default-line default-line-with-docs">Default: <span className="default-value">3</span></div> |
| <div className="path-line"><span className="path-main">max_workers</span></div> <div className="type-name-line"><span className="type-name">One of integer, null</span></div> | Number of apps and datasets read at the same time, across all spaces and streams. Each app read holds one Engine session open. Defaults to 1, as every app opened with its data is loaded into Engine memory, and to 4 on-prem when `on_prem.read_data_model` is false and apps are opened without their data. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">script_variables</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-main">tenant_hostname</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Qlik Cloud tenant hostname. Required unless `on_prem` is set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">timeout_seconds</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Seconds to wait for the Qlik Engine, and on-prem for the Repository Service, to connect or reply before the request fails. An app whose read times out is ingested as partly read. <div className="default-line default-line-with-docs">Default: <span className="default-value">300</span></div> |
| <div className="path-line"><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-main">data_connection_to_platform_instance</span></div> <div className="type-name-line"><span className="type-name">map(str,PlatformDetail)</span></div> |   |
| <div className="path-line"><span className="path-prefix">data_connection_to_platform_instance.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-prefix">data_connection_to_platform_instance.`key`.</span><span className="path-main">database</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Database (catalog for Databricks) used for tables the load script does not qualify. Inferred from the connection string when not set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">data_connection_to_platform_instance.`key`.</span><span className="path-main">default_schema</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Schema used for tables the load script does not qualify. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">data_connection_to_platform_instance.`key`.</span><span className="path-main">platform</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | DataHub platform of the tables read through this connection, e.g. `mssql` or `databricks`. Inferred from the data connection when not set. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">data_connection_to_platform_instance.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">file_connection_to_storage</span></div> <div className="type-name-line"><span className="type-name">map(str,FileStorageDetail)</span></div> |   |
| <div className="path-line"><span className="path-prefix">file_connection_to_storage.`key`.</span><span className="path-main">env</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | The environment that all assets produced by this connector belong to <div className="default-line default-line-with-docs">Default: <span className="default-value">PROD</span></div> |
| <div className="path-line"><span className="path-prefix">file_connection_to_storage.`key`.</span><span className="path-main">convert_urns_to_lowercase</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Lowercase the file dataset names, to match a storage source run with `convert_urns_to_lowercase`. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">file_connection_to_storage.`key`.</span><span className="path-main">platform_instance</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">file_connection_to_storage.`key`.</span><span className="path-main">storage_uri</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Where the connection's files are, as an `abfss://<container>@<account>.dfs.core.windows.net/<folder>`, `s3://<bucket>/<folder>` or `gs://<bucket>/<folder>` URI, or Azure HTTPS. An Azure URI must name the container; `wasb://` and `wasbs://` are not accepted. Read from the data connection when not set, which needs the API key's user to see the connection. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-main">on_prem</span></div> <div className="type-name-line"><span className="type-name">One of QlikOnPremConfig, null</span></div> | Connection to Qlik Sense Enterprise on Windows. Set this instead of `tenant_hostname` and `api_key` to read an on-prem site. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">on_prem.</span><span className="path-main">client_cert</span>&nbsp;<abbr title="Required if on_prem is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Path to the PEM client certificate exported from the QMC.  |
| <div className="path-line"><span className="path-prefix">on_prem.</span><span className="path-main">client_key</span>&nbsp;<abbr title="Required if on_prem is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Path to the PEM client key exported from the QMC.  |
| <div className="path-line"><span className="path-prefix">on_prem.</span><span className="path-main">root_cert</span>&nbsp;<abbr title="Required if on_prem is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Path to the PEM root certificate exported from the QMC.  |
| <div className="path-line"><span className="path-prefix">on_prem.</span><span className="path-main">server</span>&nbsp;<abbr title="Required if on_prem is set">❓</abbr></div> <div className="type-name-line"><span className="type-name">string</span></div> | Qlik Sense Enterprise hostname. Must match the certificate name. Repository Service and Engine ports are reached on this host.  |
| <div className="path-line"><span className="path-prefix">on_prem.</span><span className="path-main">client_key_password</span></div> <div className="type-name-line"><span className="type-name">One of string(password), null</span></div> | Password of the client key, when it was exported from the QMC with one. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">on_prem.</span><span className="path-main">engine_port</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Qlik Engine port used for app, sheet, and chart reads. <div className="default-line default-line-with-docs">Default: <span className="default-value">4747</span></div> |
| <div className="path-line"><span className="path-prefix">on_prem.</span><span className="path-main">engine_user_id</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | User id for Engine websocket calls. Certificate auth defaults to the engine service account. <div className="default-line default-line-with-docs">Default: <span className="default-value">sa&#95;engine</span></div> |
| <div className="path-line"><span className="path-prefix">on_prem.</span><span className="path-main">ingest_unpublished_apps</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Also ingest apps that are not published to a stream. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">on_prem.</span><span className="path-main">qrs_port</span></div> <div className="type-name-line"><span className="type-name">integer</span></div> | Qlik Repository Service port used for inventory. <div className="default-line default-line-with-docs">Default: <span className="default-value">4242</span></div> |
| <div className="path-line"><span className="path-prefix">on_prem.</span><span className="path-main">qrs_user_id</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | User id for Repository Service calls. Certificate auth defaults to the repository service account. <div className="default-line default-line-with-docs">Default: <span className="default-value">sa&#95;repository</span></div> |
| <div className="path-line"><span className="path-prefix">on_prem.</span><span className="path-main">qvd_folder_aliases</span></div> <div className="type-name-line"><span className="type-name">map(str,string)</span></div> |   |
| <div className="path-line"><span className="path-prefix">on_prem.</span><span className="path-main">read_data_model</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Open each app with its data to read its data model, which gives every table its fields and types, including tables loaded with `LOAD *` and from QVDs, and lets charts link to exactly the tables and fields they use. Off by default: apps are then opened without their data, which is much faster, and table fields come only from the load script. Opening an app with its data loads it into Engine memory until the app cache time set in the QMC has passed since its last session, reads one app at a time unless `max_workers` is set, and fails for apps whose section access denies the service user their data. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">on_prem.</span><span className="path-main">server_cert_hostname</span></div> <div className="type-name-line"><span className="type-name">One of string, null</span></div> | Hostname the Qlik server certificate was issued to, when it differs from `server` (for example when connecting by IP address). The certificate is still verified against `root_cert`, but its name is checked against this value. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">on_prem.</span><span className="path-main">skip_unchanged_apps</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Do not re-read an app from the Engine when the app, its objects (sheets, stories, master items), its reload time, the data connections, this recipe and the connector version are all unchanged since the last run, and DataHub still holds that run's metadata for it. Its entities are kept, not soft-deleted. Requires stateful ingestion. Set `stateful_ingestion.ignore_old_state` for one run to force a full read, for example after ingesting upstream schemas that column-level lineage should resolve against. <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">on_prem.</span><span className="path-main">user_directory</span></div> <div className="type-name-line"><span className="type-name">string</span></div> | User directory for the X-Qlik-User header. <div className="default-line default-line-with-docs">Default: <span className="default-value">INTERNAL</span></div> |
| <div className="path-line"><span className="path-prefix">on_prem.</span><span className="path-main">verify_server_hostname</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Set to false to skip the certificate name check when no hostname matches it. The certificate chain is still verified against `root_cert`. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-prefix">on_prem.</span><span className="path-main">qvd_ignored_connections</span></div> <div className="type-name-line"><span className="type-name">array</span></div> | Folder data connections whose QVDs are not ingested. By default the connections the monitoring apps read their logs through. <div className="default-line default-line-with-docs">Default: <span className="default-value">&#91;&#x27;ServerLogFolder&#x27;, &#x27;ArchivedLogsFolder&#x27;&#93;</span></div> |
| <div className="path-line"><span className="path-prefix">on_prem.qvd_ignored_connections.</span><span className="path-main">string</span></div> <div className="type-name-line"><span className="type-name">string</span></div> |   |
| <div className="path-line"><span className="path-main">space_pattern</span></div> <div className="type-name-line"><span className="type-name">AllowDenyPattern</span></div> | A class to store allow deny regexes. <br />  <br /> Patterns are matched against the start of the string only, not the entire <br /> string - a pattern does not need to match to the end to be considered a match. <br /> For example, the pattern "prod" matches "prod", "prod_east", and "production". <br /> To require an exact match, anchor your pattern explicitly, e.g. "^prod$".  |
| <div className="path-line"><span className="path-prefix">space_pattern.</span><span className="path-main">ignoreCase</span></div> <div className="type-name-line"><span className="type-name">One of boolean, null</span></div> | Whether to ignore case sensitivity during pattern matching. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |
| <div className="path-line"><span className="path-main">stateful_ingestion</span></div> <div className="type-name-line"><span className="type-name">One of StatefulStaleMetadataRemovalConfig, null</span></div> | Configuration for stateful ingestion and stale metadata removal. <div className="default-line default-line-with-docs">Default: <span className="default-value">None</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">enabled</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False <div className="default-line default-line-with-docs">Default: <span className="default-value">False</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">fail_safe_threshold</span></div> <div className="type-name-line"><span className="type-name">number</span></div> | Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'. <div className="default-line default-line-with-docs">Default: <span className="default-value">75.0</span></div> |
| <div className="path-line"><span className="path-prefix">stateful_ingestion.</span><span className="path-main">remove_stale_metadata</span></div> <div className="type-name-line"><span className="type-name">boolean</span></div> | Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled. <div className="default-line default-line-with-docs">Default: <span className="default-value">True</span></div> |

</div>




#### Schema


The [JSONSchema](https://json-schema.org/) for this configuration is inlined below.


```javascript
{
  "$defs": {
    "AllowDenyPattern": {
      "additionalProperties": false,
      "description": "A class to store allow deny regexes.\n\nPatterns are matched against the start of the string only, not the entire\nstring - a pattern does not need to match to the end to be considered a match.\nFor example, the pattern \"prod\" matches \"prod\", \"prod_east\", and \"production\".\nTo require an exact match, anchor your pattern explicitly, e.g. \"^prod$\".",
      "properties": {
        "allow": {
          "default": [
            ".*"
          ],
          "description": "List of regex patterns to include in ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'.",
          "items": {
            "type": "string"
          },
          "title": "Allow",
          "type": "array"
        },
        "deny": {
          "default": [],
          "description": "List of regex patterns to exclude from ingestion. Patterns match from the start of the string only, not the entire string - anchor with '^...$' for an exact match, e.g. '^prod$'.",
          "items": {
            "type": "string"
          },
          "title": "Deny",
          "type": "array"
        },
        "ignoreCase": {
          "anyOf": [
            {
              "type": "boolean"
            },
            {
              "type": "null"
            }
          ],
          "default": true,
          "description": "Whether to ignore case sensitivity during pattern matching.",
          "title": "Ignorecase"
        }
      },
      "title": "AllowDenyPattern",
      "type": "object"
    },
    "FileStorageDetail": {
      "additionalProperties": false,
      "properties": {
        "env": {
          "default": "PROD",
          "description": "The environment that all assets produced by this connector belong to",
          "title": "Env",
          "type": "string"
        },
        "platform_instance": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
          "title": "Platform Instance"
        },
        "storage_uri": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Where the connection's files are, as an `abfss://<container>@<account>.dfs.core.windows.net/<folder>`, `s3://<bucket>/<folder>` or `gs://<bucket>/<folder>` URI, or Azure HTTPS. An Azure URI must name the container; `wasb://` and `wasbs://` are not accepted. Read from the data connection when not set, which needs the API key's user to see the connection.",
          "title": "Storage Uri"
        },
        "convert_urns_to_lowercase": {
          "default": false,
          "description": "Lowercase the file dataset names, to match a storage source run with `convert_urns_to_lowercase`.",
          "title": "Convert Urns To Lowercase",
          "type": "boolean"
        }
      },
      "title": "FileStorageDetail",
      "type": "object"
    },
    "PlatformDetail": {
      "additionalProperties": false,
      "properties": {
        "env": {
          "default": "PROD",
          "description": "The environment that all assets produced by this connector belong to",
          "title": "Env",
          "type": "string"
        },
        "platform_instance": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
          "title": "Platform Instance"
        },
        "platform": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "DataHub platform of the tables read through this connection, e.g. `mssql` or `databricks`. Inferred from the data connection when not set.",
          "title": "Platform"
        },
        "database": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Database (catalog for Databricks) used for tables the load script does not qualify. Inferred from the connection string when not set.",
          "title": "Database"
        },
        "default_schema": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Schema used for tables the load script does not qualify.",
          "title": "Default Schema"
        }
      },
      "title": "PlatformDetail",
      "type": "object"
    },
    "QlikOnPremConfig": {
      "additionalProperties": false,
      "properties": {
        "server": {
          "description": "Qlik Sense Enterprise hostname. Must match the certificate name. Repository Service and Engine ports are reached on this host.",
          "minLength": 1,
          "title": "Server",
          "type": "string"
        },
        "client_cert": {
          "description": "Path to the PEM client certificate exported from the QMC.",
          "minLength": 1,
          "title": "Client Cert",
          "type": "string"
        },
        "client_key": {
          "description": "Path to the PEM client key exported from the QMC.",
          "minLength": 1,
          "title": "Client Key",
          "type": "string"
        },
        "root_cert": {
          "description": "Path to the PEM root certificate exported from the QMC.",
          "minLength": 1,
          "title": "Root Cert",
          "type": "string"
        },
        "client_key_password": {
          "anyOf": [
            {
              "format": "password",
              "type": "string",
              "writeOnly": true
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Password of the client key, when it was exported from the QMC with one.",
          "title": "Client Key Password"
        },
        "qrs_port": {
          "default": 4242,
          "description": "Qlik Repository Service port used for inventory.",
          "maximum": 65535,
          "minimum": 1,
          "title": "Qrs Port",
          "type": "integer"
        },
        "engine_port": {
          "default": 4747,
          "description": "Qlik Engine port used for app, sheet, and chart reads.",
          "maximum": 65535,
          "minimum": 1,
          "title": "Engine Port",
          "type": "integer"
        },
        "server_cert_hostname": {
          "anyOf": [
            {
              "type": "string"
            },
            {
              "type": "null"
            }
          ],
          "default": null,
          "description": "Hostname the Qlik server certificate was issued to, when it differs from `server` (for example when connecting by IP address). The certificate is still verified against `root_cert`, but its name is checked against this value.",
          "title": "Server Cert Hostname"
        },
        "verify_server_hostname": {
          "default": true,
          "description": "Set to false to skip the certificate name check when no hostname matches it. The certificate chain is still verified against `root_cert`.",
          "title": "Verify Server Hostname",
          "type": "boolean"
        },
        "user_directory": {
          "default": "INTERNAL",
          "description": "User directory for the X-Qlik-User header.",
          "title": "User Directory",
          "type": "string"
        },
        "qrs_user_id": {
          "default": "sa_repository",
          "description": "User id for Repository Service calls. Certificate auth defaults to the repository service account.",
          "title": "Qrs User Id",
          "type": "string"
        },
        "engine_user_id": {
          "default": "sa_engine",
          "description": "User id for Engine websocket calls. Certificate auth defaults to the engine service account.",
          "title": "Engine User Id",
          "type": "string"
        },
        "ingest_unpublished_apps": {
          "default": false,
          "description": "Also ingest apps that are not published to a stream.",
          "title": "Ingest Unpublished Apps",
          "type": "boolean"
        },
        "skip_unchanged_apps": {
          "default": false,
          "description": "Do not re-read an app from the Engine when the app, its objects (sheets, stories, master items), its reload time, the data connections, this recipe and the connector version are all unchanged since the last run, and DataHub still holds that run's metadata for it. Its entities are kept, not soft-deleted. Requires stateful ingestion. Set `stateful_ingestion.ignore_old_state` for one run to force a full read, for example after ingesting upstream schemas that column-level lineage should resolve against.",
          "title": "Skip Unchanged Apps",
          "type": "boolean"
        },
        "read_data_model": {
          "default": false,
          "description": "Open each app with its data to read its data model, which gives every table its fields and types, including tables loaded with `LOAD *` and from QVDs, and lets charts link to exactly the tables and fields they use. Off by default: apps are then opened without their data, which is much faster, and table fields come only from the load script. Opening an app with its data loads it into Engine memory until the app cache time set in the QMC has passed since its last session, reads one app at a time unless `max_workers` is set, and fails for apps whose section access denies the service user their data.",
          "title": "Read Data Model",
          "type": "boolean"
        },
        "qvd_folder_aliases": {
          "additionalProperties": {
            "type": "string"
          },
          "default": {},
          "description": "Folder paths to treat as the same folder when matching QVD files across apps, as the path one app names -> the path to use. For example `{'//qlik-sense/qvd': '//qlik-sense.corp.local/qvd'}`, when apps reach one share by different host names or drive letters. A QVD is identified by its file path, so files under these paths join into one dataset.",
          "title": "Qvd Folder Aliases",
          "type": "object"
        },
        "qvd_ignored_connections": {
          "default": [
            "ServerLogFolder",
            "ArchivedLogsFolder"
          ],
          "description": "Folder data connections whose QVDs are not ingested. By default the connections the monitoring apps read their logs through.",
          "items": {
            "type": "string"
          },
          "title": "Qvd Ignored Connections",
          "type": "array"
        }
      },
      "required": [
        "server",
        "client_cert",
        "client_key",
        "root_cert"
      ],
      "title": "QlikOnPremConfig",
      "type": "object"
    },
    "StatefulStaleMetadataRemovalConfig": {
      "additionalProperties": false,
      "description": "Base specialized config for Stateful Ingestion with stale metadata removal capability.",
      "properties": {
        "enabled": {
          "default": false,
          "description": "Whether or not to enable stateful ingest. Default: True if a pipeline_name is set and either a datahub-rest sink or `datahub_api` is specified, otherwise False",
          "title": "Enabled",
          "type": "boolean"
        },
        "remove_stale_metadata": {
          "default": true,
          "description": "Soft-deletes the entities present in the last successful run but missing in the current run with stateful_ingestion enabled.",
          "title": "Remove Stale Metadata",
          "type": "boolean"
        },
        "fail_safe_threshold": {
          "default": 75.0,
          "description": "Prevents large amount of soft deletes & the state from committing from accidental changes to the source configuration if the relative change percent in entities compared to the previous state is above the 'fail_safe_threshold'.",
          "maximum": 100.0,
          "minimum": 0.0,
          "title": "Fail Safe Threshold",
          "type": "number"
        }
      },
      "title": "StatefulStaleMetadataRemovalConfig",
      "type": "object"
    }
  },
  "additionalProperties": false,
  "properties": {
    "env": {
      "default": "PROD",
      "description": "The environment that all assets produced by this connector belong to",
      "title": "Env",
      "type": "string"
    },
    "platform_instance": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "The instance of the platform that all assets produced by this recipe belong to. This should be unique within the platform. See https://docs.datahub.com/docs/platform-instances/ for more details.",
      "title": "Platform Instance"
    },
    "stateful_ingestion": {
      "anyOf": [
        {
          "$ref": "#/$defs/StatefulStaleMetadataRemovalConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Configuration for stateful ingestion and stale metadata removal."
    },
    "tenant_hostname": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Qlik Cloud tenant hostname. Required unless `on_prem` is set.",
      "title": "Tenant Hostname"
    },
    "api_key": {
      "anyOf": [
        {
          "format": "password",
          "type": "string",
          "writeOnly": true
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Qlik Cloud API key. Required unless `on_prem` is set.",
      "title": "Api Key"
    },
    "on_prem": {
      "anyOf": [
        {
          "$ref": "#/$defs/QlikOnPremConfig"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Connection to Qlik Sense Enterprise on Windows. Set this instead of `tenant_hostname` and `api_key` to read an on-prem site."
    },
    "max_partial_reads_kept": {
      "default": 3,
      "description": "With stateful ingestion and stale metadata removal, how many consecutive runs an app that is only partly read keeps the entities of its last complete read. After that, its partial read is ingested and reported as a failure, so its current state appears and stale metadata removal is skipped for that run.",
      "minimum": 0,
      "title": "Max Partial Reads Kept",
      "type": "integer"
    },
    "max_workers": {
      "anyOf": [
        {
          "minimum": 1,
          "type": "integer"
        },
        {
          "type": "null"
        }
      ],
      "default": null,
      "description": "Number of apps and datasets read at the same time, across all spaces and streams. Each app read holds one Engine session open. Defaults to 1, as every app opened with its data is loaded into Engine memory, and to 4 on-prem when `on_prem.read_data_model` is false and apps are opened without their data.",
      "title": "Max Workers"
    },
    "space_pattern": {
      "$ref": "#/$defs/AllowDenyPattern",
      "default": {
        "allow": [
          ".*"
        ],
        "deny": [],
        "ignoreCase": true
      },
      "description": "Regex patterns to filter Qlik spaces in ingestion.Mention 'personal_space' if entities of personal space need to ingest"
    },
    "ingest_owner": {
      "anyOf": [
        {
          "type": "boolean"
        },
        {
          "type": "null"
        }
      ],
      "default": true,
      "description": "Ingest Owner from source. This will override Owner info entered from UI",
      "title": "Ingest Owner"
    },
    "timeout_seconds": {
      "default": 300,
      "description": "Seconds to wait for the Qlik Engine, and on-prem for the Repository Service, to connect or reply before the request fails. An app whose read times out is ingested as partly read.",
      "minimum": 1,
      "title": "Timeout Seconds",
      "type": "integer"
    },
    "include_sheets_and_charts": {
      "default": true,
      "description": "Ingest each app's sheets as dashboards and their charts. When false, apps are read without their sheets, which is faster, and each app dashboard links only to its tables. With stale metadata removal, sheets and charts ingested earlier are soft-deleted; raise `stateful_ingestion.fail_safe_threshold` for that first run, as they are usually most of the entities and their removal otherwise trips it.",
      "title": "Include Sheets And Charts",
      "type": "boolean"
    },
    "include_column_lineage": {
      "default": true,
      "description": "Emit column-level lineage, from load scripts and app data models and, on Qlik Cloud, from the field level of Qlik's lineage graph, which is read for each app table and each dataset an app stores into and also gives the fields of tables a load script drops. The field-level reads cost one request per table.",
      "title": "Include Column Lineage",
      "type": "boolean"
    },
    "data_connection_to_platform_instance": {
      "additionalProperties": {
        "$ref": "#/$defs/PlatformDetail"
      },
      "default": {},
      "description": "A mapping of the Qlik app dataset upstream tables from data connection to platform instance.Use 'data_connection_name' as key. Set `platform`, `database` and `default_schema` when they cannot be inferred from the data connection.",
      "title": "Data Connection To Platform Instance",
      "type": "object"
    },
    "script_variables": {
      "additionalProperties": {
        "type": "string"
      },
      "default": {},
      "description": "Values for load script variables the connector can't read, such as ones computed with LET, set in an included script, or deleted after the reload, keyed by variable name without `$()`. Used in QVD and file paths, connection names, SQL and chart expressions. An app's own variables, as its last reload left them, take precedence, and a variable the visible script sets overrides it from where it is set.",
      "title": "Script Variables",
      "type": "object"
    },
    "file_connection_to_storage": {
      "additionalProperties": {
        "$ref": "#/$defs/FileStorageDetail"
      },
      "default": {},
      "description": "Qlik Cloud only. Where the files of Azure Storage, Amazon S3 and Google Cloud Storage data connections are in DataHub, for lineage from the files apps load. Use the data connection name as key, as load scripts write it (`<space>:<connection>` or `<connection>`). Set `storage_uri` when the API key's user cannot see the connection, and `platform_instance` and `env` to match the source that ingests the storage.",
      "title": "File Connection To Storage",
      "type": "object"
    }
  },
  "title": "QlikSourceConfig",
  "type": "object"
}
```





### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Lineage from load scripts

On-prem, the connector always reads each app's load script. On Qlik Cloud it reads the script when the app's data model has no tables, or when some of its tables have no upstream lineage, for example apps loaded through the data load editor. Tables already traced by the data model are not added again from the script. A `SELECT` that runs after `LIB CONNECT TO`, `ODBC CONNECT TO`, or `OLEDB CONNECT TO` becomes a Qlik table with lineage to the tables it reads, as long as its data connection resolves to a platform and the SQL can be parsed. The upstream platform and database come from the data connection. Use `data_connection_to_platform_instance` when they cannot be inferred, or when the upstream source uses a platform instance or env:

```yaml
data_connection_to_platform_instance:
  "My SQL Server connection":
    platform: mssql
    database: sales_db
    default_schema: dbo
  "My Databricks connection":
    platform: databricks
    database: main
```

Variables set with `SET`, or with `LET` and a quoted string, are expanded in connection names and SQL from where the script sets them. A variable the visible script doesn't set, such as one computed with `LET` from an expression or set in a script pulled in with `$(Include=...)` or `$(Must_Include=...)`, takes the value the app's last reload left it, and otherwise the value given in `script_variables`. An app value that starts with `=` is an expression and is not used, so a `script_variables` value for the same name is used instead. A loop variable, a variable set with `LET` from an expression inside a loop, a `SUB`, or an `IF` or `SWITCH` branch, a variable set with `LET` more than once, a `SET` or `LET` that copies or reads one of these, a `SUB` parameter the script gives no literal value, and a variable whose `IF` branches disagree stay unknown, even after an include, since the app only keeps their last value. The included script itself is not read, so after an include the connection is treated as unknown. A `SUB` body is read where the script calls it, with the connection and variables in effect at that `CALL`, and a `SUB` that is never called adds no tables. Quoted or numeric arguments of a `CALL` are bound to the `SUB` parameters for that call, and any other argument leaves its parameter unknown. The variable of a `FOR` or `FOR EACH` loop is unknown inside the loop. Nothing after an unconditional `EXIT SCRIPT` is read. An unlabelled `SELECT` table is named after the table that follows `FROM`, cut to 32 characters, as Qlik names it. `--` comments in SQL are ignored. A `CALL` of a `SUB` the script does not define makes the connection and all variables unknown, as an include does. After either, the table loaded last is unknown too, so a `Concatenate` or `Join` without a table name is skipped until another table is loaded. A `SELECT` that runs while the connection is unknown is skipped. Rows added with `Concatenate (Table)` or fields added with `Join (Table)` belong to the named table, and without a table name to the table loaded just before. Conditions are not evaluated: a connection or variable set inside an `IF` or `SWITCH` block is kept after the block only when every branch sets the same value. An `IF` without `ELSE` or a `SWITCH` without `DEFAULT` can run no branch, so the value from before the block must match as well.

Use `script_variables` for variables neither the app nor its visible script has, for example one set to nothing after the reload, which deletes it, or a date-based folder. They're used in QVD and file paths, connection names, SQL and chart expressions:

```yaml
script_variables:
  vSchema: sales
  vs_AnnoMeseText: "2026-10"
```

A `LOAD` stacked on a `SELECT` (a preceding load) gives the table its field names: `amount AS [Net Sales]` becomes field `Net Sales` with column lineage from `amount`, and an expression such as `Year(order_date) AS Year` gets column lineage from every column it reads. `LOAD *` keeps the `SELECT` columns, and so does a preceding load with a field list item that is a `$(variable)` on its own, aliased or not (such as `LOAD $(vFields)` or `$(vField) AS Name`), or whose field name is a `$(variable)` (such as `amount AS [$(vName)]`), since the variable may hold any of them. A field aliased from such a variable, `Name` here, gets no column lineage, and a field named by a variable is left out. A variable inside a field's expression, as in `amount * $(vRate) AS Net`, only changes that field's value. The fields it does name are still added. Unquoted field names may start with `%`, as Qlik key fields often do. A field from an expression without `AS` is left out, since Qlik names it after the expression text. A table loaded with `RESIDENT` from another script table gets no lineage. Source columns are matched by exact name first and then in any case, since the SQL parser may change their case; when two columns differ only in case, a name in a third case gets no column lineage. A `CrossTable` load keeps column lineage for its qualifier fields (the first fields, one unless its third argument says otherwise); its data field gets lineage from the columns it turns into rows, and its attribute field, which holds column names, gets none. An unaliased expression in its field list still takes its place among the fields, so the qualifiers are counted correctly, but it is not ingested as a field. A `Generic` load, or a `CrossTable` whose field list is not known (such as `LOAD *`), gets table lineage only.

A connection named in `LIB CONNECT TO` resolves through `data_connection_to_platform_instance`, or through a data connection the connector can list, whose platform comes from its connector type or connection string. The platform is never guessed from the connection name, so an unlisted name without a mapping does not resolve. On Qlik Cloud, a space-prefixed name resolves to the connection in that space, and a name without a space prefix resolves only to the connection with that name in the app's own space (the personal space for personal apps), even when another space holds a connection with that name. Map the name in `data_connection_to_platform_instance` when the app's own connection is not visible to the API key. An inline `ODBC CONNECT TO` or `OLEDB CONNECT TO` connection string resolves from its driver or provider. Only the properties that name where the data lives (driver, provider, server, host, port, data source, database, catalog, schema, DSN, warehouse, and account) are kept when an inline connection string is reported or ingested, and URLs in them lose their user name, password, and query string.

A `SELECT` whose connection resolves to no platform is skipped, and the connection is listed under `unresolved_data_connections` in the ingestion report. A `SELECT` that cannot be parsed, for example one that uses an unexpanded variable in a table name, is skipped and reported as a warning. On SQL Server, Snowflake, PostgreSQL, Redshift, Databricks, BigQuery, MySQL, MariaDB, and Oracle, a table whose database or schema is neither in the SQL nor resolved for its connection is left out of lineage and reported as a warning. Set `database` and `default_schema` for the connection to include it.

On Qlik Cloud, only the Owner and users with **Can manage** or **Can operate** in a managed space can read an app's load script. When the Engine denies access to the script, the app is still ingested and the run reports a warning. Charts of an app whose script was denied get their inputs from the data model. Charts of an app whose data model or load script could not be read for another reason keep the inputs from their last read. Any other failure to read the script marks the app as partly read and reports a failure for the run, which keeps stale metadata removal from deleting its lineage.

#### Lineage through QVDs and other Qlik datasets (Qlik Cloud)

When an app's data model has no tables (the app is listed under `apps_without_load_model` in the report), the connector reads the app's tables from Qlik Cloud's lineage graph. Each table is ingested as `{app id}.{table name}`, the same name a load script table gets, and links upstream to the Qlik datasets it loads, such as QVD files, and to the app's own tables it loads from. A table that the load script parser also finds with a `SELECT` is ingested once, with the upstreams from both. The tables' fields and their types come from the app's data model in the Engine, as its last reload left them. A load script table whose name the parser can't read from its statement, such as an unlabelled `SELECT` from a subquery, takes the name of the one data model table with exactly its columns; otherwise it is ingested as `table_<n>`. Label such loads in the script to name them for certain.

Each Qlik dataset links upstream to the app tables that `STORE` into it, so lineage runs from an ETL app, through the QVDs it writes, to the apps that load them. QVD datasets have the `QVD` subtype and are named without the `.qvd` extension; the dataset URN still holds the file name.

Only datasets in spaces allowed by `space_pattern`, and visible to the API key, are linked. A dataset in any other space is left out of lineage, so give the API key's user access to the spaces that hold the QVDs to see the whole chain. With `include_column_lineage` (on by default), the connector also reads the field level of the lineage graph for each table and for each dataset an app stores into, which adds column-level lineage between app tables and datasets, and gives a table the script drops after storing it its field names. This costs one request per table. A field loaded under a new name is linked only when its source field's name is known this run. Script apps (data preparation scripts) are read like analytics apps, and the QVDs they store into are linked to their tables. Qlik answers "Forbidden" for datasets and spaces the API key's user cannot access, so those sources are left out; files on Azure Storage, Amazon S3 and Google Cloud Storage connections can still be linked as described below. Database tables in the lineage graph are not linked, as Qlik identifies them by an opaque hash; their lineage comes from the load script instead.

A table's lineage is replaced each run, as its app owns it. When something it loads could not be read this run, such as the list of datasets or the app's load script, the lineage read this run is added to the table's existing lineage instead, and the run reports a warning. A dataset is not built by one app, so only its edges from Qlik app tables are replaced: the connector reads the dataset's current lineage from DataHub, keeps every other edge, such as lineage added in DataHub, and drops edges from app tables that no longer store into it. When a storing app could not be read, or DataHub's current lineage can't be read, the storing tables are only added. If Qlik Cloud has no lineage graph for an app whose last read had tables, the app is treated as partly read rather than as having lost its tables.

#### Lineage from files on Azure Storage, Amazon S3 and Google Cloud Storage (Qlik Cloud)

Qlik Cloud's lineage graph names each file an app loads from an Azure Storage, Amazon S3 or Google Cloud Storage data connection by a hash of the connection's container or bucket and of the file's path, so the file itself can't be read from the graph. The connector links such a file to its dataset in DataHub (`abs`, `s3` or `gcs`) when the app's load script reads it with `LOAD ... FROM [lib://<connection>/<path>]`, and the hash of that path is the one in the graph. Paths built from variables are tried both as the script's own `SET` and `LET` statements expand them and with the app's variables as its last reload left them, including those set in an included script, since Qlik's lineage graph records the paths of the last reload. Mapping loads and loads that add to another table count too.

The connector also needs to know where the connection's files are. It reads the container or bucket from the data connection when the API key's user can see it (**Can consume data** in the connection's space), and then also checks it against the graph. Otherwise, set it in `file_connection_to_storage`, keyed by the connection name as load scripts write it, with the `platform_instance` and `env` of the source that ingests the storage so the URNs join:

```yaml
file_connection_to_storage:
  "Sales Space:Lake":
    storage_uri: abfss://landing@salesaccount.dfs.core.windows.net/qlik
    platform_instance: sales_lake
```

`storage_uri` takes an `abfss://<container>@<account>.dfs.core.windows.net/<folder>` (or `abfs://`, or Azure HTTPS) URI that names the container, an `s3://<bucket>/<folder>` URI or a `gs://<bucket>/<folder>` URI. The scheme may be in any case. `wasb://` and `wasbs://` are not accepted.

A file is named `<container or bucket>/<folder>/<path>`, as the `abs`, `s3` and `gcs` sources name a file when their `path_specs` ingest files individually. Names keep the case of the path the script reads; set `convert_urns_to_lowercase: true` on a mapping when the storage source is run with that option. A file is linked only when one connection could hold it: when the script reads the same path through another connection whose storage is not known, the file is left out. When two connections known only from `storage_uri` could hold it, such as the prod and dev branches of an `IF`, the one whose Azure container path or Google Cloud Storage bucket matches the file's QRI is linked, and otherwise neither is. When the data connections or an app's variables could not be read, files that are not resolved keep their lineage from the last run. File connections whose files an app reads but whose storage is not known are listed under `unresolved_file_connections` in the report. Files loaded in scripts brought in with `$(Include)` or `$(Must_Include)`, or in a `SUB` the script calls but does not define, are not read, so for such an app, a file in its lineage graph that its own script does not load is only added to the table's lineage, never removed. A file loaded through a variable that varies as the script runs, as in a `FOR EACH` loop over `FileList(...)`, is not named from the app's last value, so the table's file lineage is only added to. Files are also resolved when the list of Qlik datasets could not be read. This applies to apps whose tables come from the lineage graph, listed under `apps_without_load_model`.

#### App data models (on-prem)

On-prem, the connector opens each app without its data by default, so table fields come only from the `SELECT` and `LOAD` field lists in the load scripts, and fields have no types. Set `read_data_model: true` in the `on_prem` block to open each app with its data and read its data model instead: every table then gets the fields and types the last reload left in it, including tables loaded with `LOAD *` and from QVDs, and charts link to exactly the tables and fields they use. This is much slower, about a minute per large app, and reads one app at a time unless `max_workers` is set. The Engine keeps an app it opened with its data in memory until the engine's app cache time, set in the QMC, has passed since its last session, as it does for an app users open ([Qlik support article](https://community.qlik.com/t5/Official-Support-Articles/Why-does-the-Qlik-Sense-Memory-usage-remain-high/ta-p/1805698)), and apps whose section access denies the service user their data cannot be read. Schedule such runs outside peak hours. When opening an app with its data is denied, as section access can deny it to the service user, or times out while the data loads, the app is read without its data instead and listed in the run's "Qlik apps read without their data" warning.

#### Lineage through QVDs (on-prem)

On Qlik Sense Enterprise on Windows, QVD lineage comes from the load scripts. A table loaded with `LOAD ... FROM <file> (qvd)` links upstream to that QVD, and a QVD written with `STORE <table> INTO <file>` links upstream to the stored table, so lineage runs from an ETL app, through the QVDs it writes, to the apps that load them. Each QVD is a dataset with the `QVD` subtype, named after its file without the extension.

Each QVD is placed in `Folder` containers that follow its path, one per server, share or drive and folder (`nas` / `qvd` / `raw` for `\\nas\qvd\raw\orders.qvd`), as the S3, ABS and GCS sources place files, so QVDs are browsed by where they are stored.

A QVD is identified by its file path: a `lib://` path is resolved through the folder of its data connection, and file system paths, which only legacy mode allows, are used as they are. Paths are matched without regard to case or slash direction, so two connections to the same folder find the same QVD. When apps reach one share by different names, such as a short and a fully qualified host name, map them with `qvd_folder_aliases`:

```yaml
on_prem:
  qvd_folder_aliases:
    "//qlik-sense/qvd": "//qlik-sense.corp.local/qvd"
```

A QVD's schema comes from the table stored into it, or, when no app stores it, from the fields apps load from it. Column-level lineage follows the fields of each `LOAD` and `STORE` field list; a `LOAD *` leaves the table's fields unknown.

Variables in a QVD path, including those set with `LET` or in an included script such as a connection prefix, are expanded with the app's variables as its last reload left them, and a `lib://` path may separate the connection from the path with a backslash. As in Qlik, a variable the app doesn't have expands to nothing in a connection name, such as an empty connection prefix in `lib://$(vPrefix)QVD Data/`, but only when what is left names a folder connection. Quotes a `SET` variable carries into a path are dropped. The app keeps only the last value of a variable, which is also its value before the script first sets it, as variables persist between reloads. So inside a loop, a loop variable or a variable set with `LET` from an expression there is not expanded from it, nor is one set that way in a `SUB` or an `IF` or `SWITCH` branch, a `SUB` parameter given no literal value, a variable whose `IF` branches disagree, a variable between assignments when the script sets it more than once, or a `SET` or `LET` that copies one of these, even after an include. After the loop, a variable it set takes the app's value again. Assignments in comments, in a `SUB` that is never called and after `EXIT SCRIPT` are not counted. A path the connector can't resolve to one file, such as a wildcard, a variable the app doesn't have, a path relative to the `Directory` statement, or a `lib://` path of a connection that isn't a folder connection, is listed under `unresolved_qvd_paths` in the report. An unlabelled `LOAD` from a QVD is skipped, as Qlik names its table. QVDs of the connections in `qvd_ignored_connections` (by default the monitoring apps' `ServerLogFolder` and `ArchivedLogsFolder`) and the Engine's `BUFFER` QVDs are not ingested. When an app's load script can't be read, the QVDs it writes are unknown, so QVD schemas and lineage are kept from the last run.

#### Apps without sheets and charts

Each app is ingested both as a container, which holds its tables, sheets and charts, and as a dashboard with the `Qlik App` subtype. The app dashboard links to the app's sheets, and to the app's tables that no chart reads, so lineage runs from every table to the app, through a chart and its sheet or directly. To keep only apps and their tables, set `include_sheets_and_charts: false`. Apps are then read without their sheets, which is faster, and each app dashboard links to all of its tables. With stale metadata removal enabled, the sheets and charts an earlier run ingested are soft-deleted on the next run. They are usually most of the entities a run ingests, so dropping them can exceed `stateful_ingestion.fail_safe_threshold` (75% by default), and the run then skips stale removal and reports a failure. Raise the threshold to 100 for the first run with the option off, and set it back afterwards.

#### Charts in containers

Charts inside a container object's tabs are ingested as charts of the sheet. An untitled chart takes its tab label as its title. Objects with no data, such as buttons, filter panes and variable inputs, are skipped.

#### Chart input fields

Each chart lists the Qlik table fields it reads as input fields, and its inputs are narrowed to the tables those fields belong to. Fields are taken from the chart's dimensions and measure expressions, including master measures. A name in an expression counts only when it matches a field of one of the app's ingested tables. Names match case-sensitively, as Qlik field names do, except for the columns of tables traced from a load script, whose case the SQL parser may change. Tables loaded with `MAPPING` are only used by mapping operations such as `ApplyMap`, `Map ... Using`, `Rename Field` and `MapSubstring`, and are never chart inputs. A derived field such as `Date.autoCalendar.Year` or `[Date].autoCalendar.Year` counts as its base field `Date`, unless a field named exactly `Date.autoCalendar.Year` exists, which then takes precedence. When neither is known, the chart keeps all of the app's tables as inputs. Values in set modifiers, such as `"Net*"` and `"East"` in `{<Region={"Net*"}+{"East"}>}`, are search strings and are not matched. For tables traced from a load script, those are the columns the SQL parser found, under the names a preceding load gives them. A field shared by several tables, such as a key field, is listed for each of them. A `$(variable)` is expanded with the app variable's definition as the app was last saved or reloaded, including variables set in included scripts, so a chart whose dimension is `$(vGrain)` reads the field the variable names. A variable a user changes through a variable input or button is read with its saved value. When a measure cannot be read, or an expression uses a variable whose definition starts with `=` or that the app does not have, the chart keeps all of the app's tables as inputs. A chart that reads no field of the app, such as a text object or a chart whose fields were dropped by the load script, has no input tables. A chart whose measures could not be read this time keeps the input fields from its last read, while a chart that uses a deleted master measure lists the fields its other dimensions and measures read. Comments in expressions are ignored.

#### Large sites

The Engine loads each app's data when the connector opens the app, on Qlik Cloud and on-prem unless `read_data_model` is false, so every worker holds an app in memory. On Qlik Cloud a rate-limited Engine connection is not retried and leaves its app unread. Raise `max_workers` with care. `max_workers` sets how many apps and datasets are read at the same time, across all spaces and streams. It defaults to 1, and to 4 on-prem when `read_data_model` is false. Each worker holds its own Engine session under the configured user. `max_workers` applies to Qlik Cloud too, where a rate-limited (HTTP 429) REST request is retried up to 5 times. Each retry waits for the `Retry-After` time when Qlik sends one, and otherwise backs off for 0, 2, 4, 8 and 16 seconds. A request made while listing or reading a space, item or app that is still rate-limited after that fails, and that space, item or app is reported as unread or partly read, which keeps stale metadata removal from deleting its metadata. Lower `max_workers` if this happens.

With stateful ingestion and stale metadata removal enabled, on Qlik Cloud and on-prem, each run records which entities every app produced. When an app is only partly read and its last read was recorded, nothing from the partial read is emitted and the run reports a warning: the app's metadata stays as of its last complete read until a read of it completes. This lasts for at most as many consecutive runs as the top-level `max_partial_reads_kept` option sets (3 by default). After that, the partial read is ingested so the app's current state appears, and the run reports a failure, which skips stale metadata removal for that run. A complete read resets the count. A failed run keeps the recorded last read of the apps it did not reach. When its last read was not recorded, for example on the first run or when the last checkpoint could not be saved, the partial read is emitted and the run reports a failure, which keeps stale metadata removal from deleting anything in that run. A complete read records only what it produced, so sheets and charts removed in Qlik are soft-deleted. The checkpoint holds every entity URN of every app, alongside the stale metadata removal state. If it grows past `stateful_ingestion.max_checkpoint_state_size` (16 MB by default), it is not saved and the run reports a warning; raise the limit.

With `skip_unchanged_apps: true` in the `on_prem` block and stateful ingestion enabled, later runs only open apps that changed in the Engine. This is on-prem only, because the Repository Service reports when each app, sheet, story and master item was last modified, and Qlik Cloud has no equivalent listing. An app is read again when the app or any of its sheets, stories or master items is saved, when it is reloaded, published or moves stream, when its owner or a sheet's owner changes, when the recipe or a data connection changes, or after a connector upgrade. It is also read again if the last read of it failed, if its app container in DataHub is missing or does not carry the fingerprint of that read (stored as the container's `ingestionFingerprint` custom property, which is visible in the UI), or if any entity that read produced is missing from DataHub or soft-deleted. When the change check itself fails, the app is read in full, the run reports a warning, and the app is listed under `apps_change_check_failed`. Aspects that failed to write for an entity that exists are not detected. To read every app again, turn `skip_unchanged_apps` off for one run; `stateful_ingestion.ignore_old_state: true` also does that, but skips stale metadata removal for that run. Entities of a skipped app stay in place and are not soft-deleted.

Column-level lineage is resolved against the upstream schemas in DataHub at read time. After ingesting an upstream source for the first time, turn `skip_unchanged_apps` off for one run so every app is read again.

```yaml
max_workers: 4
on_prem:
  # server and certificates as above
  skip_unchanged_apps: true
stateful_ingestion:
  enabled: true
  remove_stale_metadata: true
```

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.


### Code Coordinates
- Class Name: `datahub.ingestion.source.qlik_sense.qlik_sense.QlikSenseSource`
- Browse on [GitHub](https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/qlik_sense/qlik_sense.py)


:::tip Questions?

If you've got any questions on configuring ingestion for Qlik Sense, feel free to ping us on [our Slack](https://datahub.com/slack).
:::



:::note 💡 **Contributing to this documentation**
This page is auto-generated from the underlying source code. To make changes, please edit the relevant source files in the [metadata-ingestion](https://github.com/datahub-project/datahub/tree/master/metadata-ingestion) directory. 

**Tip:** For quick typo fixes or documentation updates, you can click the ✏️ **Edit** icon directly in the GitHub UI to open a Pull Request. For larger changes and PR naming conventions, please refer to our [Contributing Guide](/docs/contributing).
:::
