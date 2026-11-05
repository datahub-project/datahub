### Overview

The `qlik-sense` module ingests metadata from Qlik Sense into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This source extracts the following:

- Accessible spaces and apps within that spaces as Container.
- Qlik Datasets as Datahub Datasets with schema metadata.
- Sheets as Datahub dashboard and charts present inside sheets.
- On-prem streams, apps, sheets, and charts when `deployment` is `on_prem`.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.

1. Refer [doc](https://qlik.dev/authenticate/api-key/generate-your-first-api-key/) to generate an API key from the hub.
2. Get tenant hostname from About tab after login to qlik sense account.

#### Qlik Sense Enterprise on Windows

Set `deployment: on_prem`. Qlik Cloud API keys are not used. Export the client certificate, client key, and root certificate from the QMC as PEM files. The ingestion host must reach the Repository Service (port 4242) and the Engine (port 4747) on the central node. Certificate calls run with administrator rights. The default accounts are `INTERNAL\sa_repository` for inventory and `INTERNAL\sa_engine` for app content.

This mode ingests streams, apps, sheets, and charts, plus lineage from the `SELECT` statements in each app's load script. It does not ingest Qlik Cloud datasets or QVD lineage.

The server certificate is verified against `root_cert`. Qlik issues it to the central node's hostname, so if you connect by IP address or an alias, set `server_cert_hostname` to the name on the certificate. Set `verify_server_hostname: false` only if no name matches. The certificate chain is still verified.

```yaml
source:
  type: qlik-sense
  config:
    deployment: on_prem
    server: 10.0.0.10
    server_cert_hostname: qlik-central.example.com
    client_cert: /secrets/client.pem
    client_key: /secrets/client_key.pem
    root_cert: /secrets/root.pem
```

#### Lineage from load scripts

When an app has no data model table metadata (always on-prem, and on Qlik Cloud for apps loaded through the data load editor), the connector reads the app's load script. Each `SELECT` that runs after `LIB CONNECT TO`, `ODBC CONNECT TO`, or `OLEDB CONNECT TO` becomes a Qlik table with lineage to the tables it reads. The upstream platform and database come from the data connection. Use `data_connection_to_platform_instance` when they cannot be inferred, or when the upstream source uses a platform instance or env:

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

Connections that resolve to no platform are listed under `unresolved_data_connections` in the ingestion report. Statements that use script variables in table names cannot be parsed and are reported as warnings.
