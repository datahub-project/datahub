# Pulsar

<!-- Set Support Status -->
<!-- ![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen)
![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey)-->
![Incubating](https://img.shields.io/badge/support%20status-incubating-blue)

## Integration Details

<!-- Plain-language description of what this integration is meant to do.  -->
<!-- Include details about where metadata is extracted from (ie. logs, source API, manifest, etc.)   -->

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


### Concept Mapping

<!-- This should be a manual mapping of concepts from the source to the DataHub Metadata Model -->
<!-- Authors should provide as much context as possible about how this mapping was generated, including assumptions made, known shortcuts, & any other caveats -->

This ingestion source maps the following Source System Concepts to DataHub Concepts:

<!-- Remove all unnecessary/irrelevant DataHub Concepts -->


| Source Concept | DataHub Concept                                                    | Notes                                                                     |
|----------------|--------------------------------------------------------------------|---------------------------------------------------------------------------|
| `pulsar`       | [Data Platform](docs/generated/metamodel/entities/dataPlatform.md) |                                                                           |
| Pulsar Topic   | [Dataset](docs/generated/metamodel/entities/dataset.md)            | _subType_: `topic`                                                        |
| Pulsar Schema  | [SchemaField](docs/generated/metamodel/entities/schemaField.md)    | Maps to the fields defined within the `Avro` or `JSON` schema definition. | 


### Supported Capabilities

<!-- This should be an auto-generated table of supported DataHub features/functionality -->
<!-- Each capability should link out to a feature guide -->

| Capability                                            | Status | Notes                                                                                                                                                                                                                                        |
|-------------------------------------------------------|:------:|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Data Container                                        |   ❌    |                                                                                                                                                                                                                                              |
| [Stateful Ingestion](./stateful_ingestion.md)         |   ✅    | Requires recipe configuration, stateful Ingestion is available only when a Platform Instance is assigned to this source.                                                                                                                     |
| Partition Support                                     |   ✅    | Requires recipe configuration, each individual partition topic can be ingest. Behind the scenes, a partitioned topic is actually implemented as N internal topics, where N is the number of partitions. This feature is disabled by default. |
| [Platform Instance](../../docs/platform-instances.md) |   ✅    | Requires recipe configuration and is mandatory for Stateful Ingestion. A Pulsar instance consists of one or more Pulsar clusters.                                                                                                            |
| [Data Domain](../../docs/domains.md)                  |   ✅    | Requires recipe configuration                                                                                                                                                                                                                |
| Dataset Profiling                                     |   ❌    |                                                                                                                                                                                                                                              |
| Dataset Usage                                         |   ❌    |                                                                                                                                                                                                                                              |
| Extract Descriptions                                  |   ❌    |                                                                                                                                                                                                                                              |
| Extract Lineage                                       |   ❌    |                                                                                                                                                                                                                                              |
| Extract Ownership                                     |   ❌    |                                                                                                                                                                                                                                              |
| Extract Tags                                          |   ❌    |                                                                                                                                                                                                                                              |
| ...                                                   |        |

## Metadata Ingestion Quickstart

For context on getting started with ingestion, check out our [metadata ingestion guide](../README.md).

### Prerequisites

In order to ingest metadata from Apache Pulsar, you will need:

* Access to a Pulsar Instance, if authentication is enabled a valid access token.
* Pulsar version >= 2.7.0
* ...

> **_NOTE:_**  A _superUser_ role is required for listing all existing tenants within a Pulsar instance.
>

### Install the Plugin(s)

Run the following commands to install the relevant plugin(s):

`pip install 'acryl-datahub[pulsar]'`

### Configure the Ingestion Recipe(s)

Use the following recipe(s) to get started with ingestion. See [below](#config-details) for full configuration options.

_For general pointers on writing and running a recipe, see our [main recipe guide](../README.md#recipes)._

#### Quickstart recipe
Getting started receipt
```yml
source:
  type: pulsar
  config:
    # Required fields
    web_service_url: "http://localhost:8080"

sink:
  # sink configs
```


#### Example recipe with authentication
An example recipe for ingesting from a Pulsar instance with oauth authentication and ssl enabled.


```yml
source: 
  type: "pulsar" 
  config:
    env: "TEST" 
    platform_instance: "local"
    ## Pulsar client connection config ## 
    web_service_url: "https://localhost:8443"
    verify_ssl: "/opt/certs/ca.cert.pem"
    # Issuer url for auth document, for example "http://localhost:8083/realms/pulsar"
    issuer_url: <issuer_url>
    client_id: ${CLIENT_ID}
    client_secret: ${CLIENT_SECRET}
    # Tenant list to scrape 
    tenants:
      - tenant_1
      - tenant_2
    # Topic filter pattern 
    topic_patterns:
      allow:
        - ".*sales.*"

sink:
  # sink configs
```

> **_NOTE:_**  Always use TLS encryption in a production environment and use variable substitution for sensitive information (e.g. ${CLIENT_ID} and ${CLIENT_SECRET}).
>

## Config details
<details>
  <summary>View All Recipe Configuration Options</summary>

Note that a `.` is used to denote nested fields in the YAML recipe.

| Field                           | Required | Default                 | Description                                                                                                                                                     |
|---------------------------------|:--------:|-------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `env`                           |    ❌     | `PROD`                  | The data fabric, defaults to PROD                                                                                                                               |
| `platform_instance`             |    ❌     |                         | The Platform instance to use while constructing URNs. Mandatory for Stateful Ingestion                                                                          |
| `web_service_url`               |    ✅     | `http://localhost:8080` | The web URL for the cluster.                                                                                                                                    |
| `timeout`                       |    ❌     | `5`                     | Timout setting, how long to wait for the Pulsar rest api to send data before giving up                                                                          |
| `verify_ssl`                    |    ❌     | `True`                  | Either a boolean, in which case it controls whether we verify the server's TLS certificate, or a string, in which case it must be a path to a CA bundle to use. |
| `issuer_url`                    |    ❌     |                         | The complete URL for a Custom Authorization Server. Mandatory for OAuth based authentication.                                                                   |
| `client_id`                     |    ❌     |                         | The application's client ID                                                                                                                                     |
| `client_secret`                 |    ❌     |                         | The application's client secret                                                                                                                                 |
| `token`                         |    ❌     |                         | The access token for the application. Mandatory for token based authentication.                                                                                 |
| `tenant_patterns.allow`         |    ❌     | `.*`                    | List of regex patterns for tenants to include in ingestion. By default all tenants are allowed.                                                                 |
| `tenant_patterns.deny`          |    ❌     | `pulsar`                | List of regex patterns for tenants to exclude from ingestion. By default the Pulsar system tenant is denied.                                                    |
| `tenant_patterns.ignoreCase`    |    ❌     | `True`                  | Whether to ignore case sensitivity during tenant pattern matching.                                                                                              |
| `namespace_patterns.allow`      |    ❌     | `.*`                    | List of regex patterns for namespaces to include in ingestion. By default all namespaces are allowed.                                                           |
| `namespace_patterns.deny`       |    ❌     | `public/functions`      | List of regex patterns for namespaces to exclude from ingestion. By default the functions namespace is denied.                                                  |
| `namespace_patterns.ignoreCase` |    ❌     | `True`                  | Whether to ignore case sensitivity during namespace pattern matching.                                                                                           |
| `topic_patterns.allow`          |    ❌     | `.*`                    | List of regex patterns for topics to include in ingestion. By default all topics are allowed.                                                                   |
| `topic_patterns.deny`           |    ❌     | `/__.*$`                | List of regex patterns for topics to exclude from ingestion. By default the Pulsar system topics are denied.                                                    |
| `topic_patterns.ignoreCase`     |    ❌     | `True`                  | Whether to ignore case sensitivity during topic pattern matching.                                                                                               |
| `tenants`                       |    ❌     |                         | Listing all tenants requires superUser role, alternative you can set a list of tenants you want to scrape using the tenant admin role                           | 
| `exclude_individual_partitions` |    ❌     | `True`                  | Extract each individual partitioned topic. e.g. when turned off a topic with 100 partitions will result in 100 `Datesets`.                                      |
| `domain.domain_urn.allow`       |    ❌     |                         | List of regex patterns for topics to set domain_urn domain key. There can be multiple domain key specified.                                                     |
| `domain.domain_urn.deny`        |    ❌     |                         | List of regex patterns for topics to not assign domain_urn. There can be multiple domain key specified.                                                         |
| `domain.domain_urn.ignoreCase`  |    ❌     | `True`                  | Whether to ignore case sensitivity during pattern matching.There can be multiple domain key specified.                                                          |
| `stateful_ingestion`            |    ❌     |                         | see [Stateful Ingestion](./stateful_ingestion.md)                                                                                                               |
</details>


## Troubleshooting

### [Common Issue]

[Provide description of common issues with this integration and steps to resolve]