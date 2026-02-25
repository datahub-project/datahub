# Azure Data Factory

For context on getting started with ingestion, check out our [metadata ingestion guide](../../../../metadata-ingestion/README.md).

## Setup

To install this plugin, run `pip install 'acryl-datahub[azure-data-factory]'`.

## Quickstart Recipe

```yaml
source:
  type: azure-data-factory
  config:
    # Required
    subscription_id: ${AZURE_SUBSCRIPTION_ID}

    # Authentication (service principal)
    credential:
      authentication_method: service_principal
      client_id: ${AZURE_CLIENT_ID}
      client_secret: ${AZURE_CLIENT_SECRET}
      tenant_id: ${AZURE_TENANT_ID}

    # Optional filters
    factory_pattern:
      allow: ["prod-.*"]

    # Features
    include_lineage: true
    include_execution_history: false

    env: PROD

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

## Authentication Methods

| Method            | Config Value        | Use Case          |
| ----------------- | ------------------- | ----------------- |
| Service Principal | `service_principal` | Production        |
| Managed Identity  | `managed_identity`  | Azure-hosted      |
| Azure CLI         | `cli`               | Local development |
| Auto-detect       | `default`           | Flexible          |

## Config Details

| Field                              | Required | Description                               |
| ---------------------------------- | -------- | ----------------------------------------- |
| `subscription_id`                  | ✅       | Azure subscription ID                     |
| `credential.authentication_method` |          | Auth method (default: `default`)          |
| `credential.client_id`             |          | App (client) ID for service principal     |
| `credential.client_secret`         |          | Client secret for service principal       |
| `credential.tenant_id`             |          | Tenant (directory) ID                     |
| `resource_group`                   |          | Filter to specific resource group         |
| `factory_pattern`                  |          | Regex allow/deny for factories            |
| `pipeline_pattern`                 |          | Regex allow/deny for pipelines            |
| `include_lineage`                  |          | Extract lineage (default: `true`)         |
| `include_execution_history`        |          | Extract pipeline runs (default: `false`)  |
| `execution_history_days`           |          | Days of history, 1-90 (default: `7`)      |
| `platform_instance_map`            |          | Map linked services to platform instances |
| `env`                              |          | Environment (default: `PROD`)             |

## Entity Mapping

| ADF Concept  | DataHub Entity      |
| ------------ | ------------------- |
| Data Factory | Container           |
| Pipeline     | DataFlow            |
| Activity     | DataJob             |
| Dataset      | Dataset             |
| Pipeline Run | DataProcessInstance |

## Questions

If you've got any questions on configuring this source, feel free to ping us on [our Slack](https://slack.datahubproject.io/).
