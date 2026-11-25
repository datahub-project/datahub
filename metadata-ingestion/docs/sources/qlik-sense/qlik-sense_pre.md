### Overview

The `qlik-sense` module ingests metadata from Qlik Cloud and from Qlik Sense Enterprise on Windows (on-prem). Set `tenant_hostname` and `api_key` for Qlik Cloud, or the `on_prem` block for Qlik Sense Enterprise on Windows. One recipe reads one deployment; to ingest both, run two recipes with different `platform_instance` values. With stateful ingestion, also give them different `pipeline_name` values: the checkpoint is keyed by `pipeline_name`, not `platform_instance`, so two recipes that share one would remove each other's metadata as stale.

| Metadata                                        | Qlik Cloud                                                                                                                                                                              | On-prem                                                    |
| ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| Spaces and streams                              | Spaces, as containers                                                                                                                                                                   | Streams, as containers                                     |
| Apps                                            | Container and dashboard                                                                                                                                                                 | Container and dashboard                                    |
| Sheets and charts                               | Yes                                                                                                                                                                                     | Yes                                                        |
| App tables                                      | From the data model, the lineage graph and the load script                                                                                                                              | From the load script, or the data model if read            |
| Qlik datasets                                   | Yes, with schema; files on connections of unknown storage are named by connection and path, and datasets the API key can't list are named from the lineage graph, both without a schema | No                                                         |
| QVDs                                            | QVD datasets in spaces, and QVDs on connections of unknown storage, named by connection and path without a schema                                                                       | QVD files that load scripts read or write                  |
| Lineage from databases                          | From load script `SELECT` statements                                                                                                                                                    | From load script `SELECT` statements                       |
| Lineage from files on cloud storage             | Azure Storage, Amazon S3 and Google Cloud Storage connections                                                                                                                           | QVDs on connections mapped in `file_connection_to_storage` |
| Chart input fields and column-level lineage     | Yes                                                                                                                                                                                     | Yes                                                        |
| Owners                                          | Yes                                                                                                                                                                                     | Yes, by Qlik user id                                       |
| Skipping unchanged apps (`skip_unchanged_apps`) | No                                                                                                                                                                                      | Yes                                                        |

### Prerequisites

Before running ingestion, make sure the machine that runs it can reach Qlik and that the credential can read the spaces or streams you want to ingest.

#### Qlik Cloud

1. [Generate an API key](https://qlik.dev/authenticate/api-key/generate-your-first-api-key/) from the hub.
2. Get the tenant hostname from the **About** tab after you log in to Qlik Cloud.
3. Give the API key's user access to every space you want to ingest. Some metadata needs more than view access:

| Metadata                                                 | Access needed                                                                                                                         |
| -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| Load scripts, and lineage from them                      | Owner of the app, or **Can manage** or **Can operate** in a managed space                                                             |
| Data connections, for their platform and files' storage  | **Can consume data** in the space that holds the connection                                                                           |
| QVDs and other Qlik datasets an app loads or stores into | Access to the space that holds the dataset. Without it, a dataset in an app's lineage graph is named from the graph, without a schema |
| Scripts apps include with `$(Include=...)`               | **Can view** in the space that holds the file                                                                                         |

#### Qlik Sense Enterprise on Windows

1. Export the client certificate, client key and root certificate from the QMC as PEM files.
2. Put the files on the machine that runs ingestion. `client_cert`, `client_key` and `root_cert` are file paths, not file contents. When the source is set up in the DataHub UI, run it on an executor where the files are mounted (on DataHub Cloud, a remote executor).
3. Make sure the ingestion host can reach the Repository Service (`qrs_port`, 4242 by default) and the Engine (`engine_port`, 4747 by default) on the central node. **Test Connection** checks both ports.
4. Set the `on_prem` block instead of `tenant_hostname` and `api_key`. Set `client_key_password` when the client key was exported with a password.

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

Certificate calls run with administrator rights. The default accounts are `INTERNAL\sa_repository` for inventory and `INTERNAL\sa_engine` for app content. Each Repository Service request and each Engine connection or reply waits up to `timeout_seconds` (default 300), and an app whose read times out is ingested as partly read.

##### Server certificate

The server certificate is verified against `root_cert`. Qlik issues it to the central node's hostname, so if you connect by IP address or an alias, set `server_cert_hostname` to the name on the certificate. The certificate chain is still verified when `verify_server_hostname` is `false`, but any certificate signed by that root is accepted for any host.

:::caution
`verify_server_hostname: false` leaves the connection open to impersonation by another server holding a certificate from the same root. Use it only as a last resort, after trying `server_cert_hostname`, a DNS entry that matches the certificate, or a reissued certificate.
:::
