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

This mode ingests streams, apps, sheets, and charts. It does not ingest Qlik Cloud datasets, and it does not build QVD or SQL lineage.

```yaml
source:
  type: qlik-sense
  config:
    deployment: on_prem
    server: qlik.example.com
    client_cert: /secrets/client.pem
    client_key: /secrets/client_key.pem
    root_cert: /secrets/root.pem
```
