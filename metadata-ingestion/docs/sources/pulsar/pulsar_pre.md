### Overview

The `pulsar` module ingests metadata from Pulsar into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

The Datahub Pulsar source plugin extracts `topic` and `schema` metadata from an Apache Pulsar instance and ingest the information into Datahub. The plugin uses the [Pulsar admin Rest API interface](https://pulsar.apache.org/admin-rest-api/#) to interact with the Pulsar instance. The following APIs are used in order to:

- [Get the list of existing tenants](https://pulsar.apache.org/admin-rest-api/#tag/tenants)
- [Get the list of namespaces associated with each tenant](https://pulsar.apache.org/admin-rest-api/#tag/namespaces)
- [Get the list of topics associated with each namespace](https://pulsar.apache.org/admin-rest-api/#tag/persistent-topic)
  - persistent topics
  - persistent partitioned topics
  - non-persistent topics
  - non-persistent partitioned topics
- [Get the latest schema associated with each topic](https://pulsar.apache.org/admin-rest-api/#tag/schemas)

The data is extracted on `tenant` and `namespace` basis, topics with corresponding schema (if available) are ingested as [Dataset](docs/generated/metamodel/entities/dataset.md) into Datahub. Some additional values like `schema description`, `schema_version`, `schema_type` and `partitioned` are included as `DatasetProperties`.

### Prerequisites

- **Pulsar Instance**: Access with valid access token (if authentication enabled)
- **Version**: Pulsar 2.7.0 or later
- **Role**: `superUser` required to list all tenants

:::info
A `superUser` role is required for listing all existing tenants within a Pulsar instance.
:::
