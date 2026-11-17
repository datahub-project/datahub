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
