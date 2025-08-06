## Setting up connection to an Iceberg catalog

There are multiple servers compatible with the Iceberg Catalog specification. DataHub's `iceberg` connector uses `pyiceberg`
library to extract metadata from them. The recipe for the source consists of 2 parts:

1. `catalog` part which is passed as-is to the `pyiceberg` library and configures the connection and its details (i.e. authentication).
   The name of catalog specified in the recipe has no consequence, it is just a formal requirement from the library.
   Only one catalog will be considered for the ingestion.
2. The remaining configuration consists of parameters, such as `env` or `stateful_ingestion` which are standard
   DataHub's ingestor configuration parameters and are described in the [Config Details](#config-details) chapter.

This chapter showcases several examples of setting up connections to an Iceberg catalog, varying based on the underlying
implementation. Iceberg is designed to have catalog and warehouse separated, which is reflected in how we configure it.
It is especially visible when using Iceberg REST Catalog - which can use many blob storages
(AWS S3, Azure Blob Storage, MinIO) as a warehouse.

Note that, for advanced users, it is possible to specify a custom catalog client implementation via `py-catalog-impl`
configuration option - refer to `pyiceberg` documentation on details.

### Glue catalog + S3 warehouse

The minimal configuration for connecting to Glue catalog with S3 warehouse:

```yaml
source:
  type: "iceberg"
  config:
    env: dev
    catalog:
      my_catalog:
        type: "glue"
        s3.region: "us-west-2"
        region_name: "us-west-2"
```

Where `us-west-2` is the region from which you want to ingest. The above configuration will work assuming your pod or environment in which
you run your datahub CLI is already authenticated to AWS and has proper permissions granted (see below). If you need
to specify secrets directly, use the following configuration as the template:

```yaml
source:
  type: "iceberg"
  config:
    env: dev
    catalog:
      demo:
        type: "glue"
        s3.region: "us-west-2"
        s3.access-key-id: "${AWS_ACCESS_KEY_ID}"
        s3.secret-access-key: "${AWS_SECRET_ACCESS_KEY}"
        s3.session-token: "${AWS_SESSION_TOKEN}"

        aws_access_key_id: "${AWS_ACCESS_KEY_ID}"
        aws_secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
        aws_session_token: "${AWS_SESSION_TOKEN}"
        region_name: "us-west-2"
```

This example uses references to fill credentials (either from Secrets defined in Managed Ingestion or environmental variables).
It is possible (but not recommended due to security concerns) to provide those values in plaintext, directly in the recipe.

#### Glue and S3 permissions required

The role used by the ingestor for ingesting metadata from Glue Iceberg Catalog and S3 warehouse is:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["glue:GetDatabases", "glue:GetTables", "glue:GetTable"],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket", "s3:GetObjectVersion"],
      "Resource": [
        "arn:aws:s3:::<bucket used by the warehouse>",
        "arn:aws:s3:::<bucket used by the warehouse>/*"
      ]
    }
  ]
}
```

### Iceberg REST Catalog + MinIO

The following configuration assumes MinIO defines authentication using the `s3.*` prefix. Note the specification of `s3.endpoint`, assuming
MinIO listens on port `9000` at `minio-host`. The `uri` parameter points at Iceberg REST Catalog (IRC) endpoint (in this case `iceberg-catalog:8181`).

```yaml
source:
  type: "iceberg"
  config:
    env: dev
    catalog:
      demo:
        type: "rest"
        uri: "http://iceberg-catalog:8181"
        s3.access-key-id: "${AWS_ACCESS_KEY_ID}"
        s3.secret-access-key: "${AWS_SECRET_ACCESS_KEY}"
        s3.region: "eu-east-1"
        s3.endpoint: "http://minio-host:9000"
```

### Iceberg REST Catalog (with authentication) + S3

This example assumes IRC requires token authentication (via `Authorization` header). There are more options available,
see https://py.iceberg.apache.org/configuration/#rest-catalog for details. Moreover, the assumption here is that the
environment (i.e. pod) is already authenticated to perform actions against AWS S3.

```yaml
source:
  type: "iceberg"
  config:
    env: dev
    catalog:
      demo:
        type: "rest"
        uri: "http://iceberg-catalog-uri"
        token: "token-value"
        s3.region: "us-west-2"
```

#### Special REST connection parameters for resiliency

Unlike other parameters provided in the dictionary under the `catalog` key, `connection` parameter is a custom feature in
DataHub, allowing to inject connection resiliency parameters to the REST connection made by the ingestor. `connection`
allows for 2 parameters:

- `timeout` is provided as amount of seconds, it needs to be whole number (or `null` to turn it off)
- `retry` is a complex object representing parameters used to create [urllib3 Retry object](https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html#module-urllib3.util.retry).
  There are many possible parameters, most important would be `total` (total retries) and `backoff_factor`. See the linked docs
  for the details.

```yaml
source:
  type: "iceberg"
  config:
    env: dev
    catalog:
      demo:
        type: "rest"
        uri: "http://iceberg-catalog-uri"
        connection:
          retry:
            backoff_factor: 0.5
            total: 3
          timeout: 120
```

### SQL catalog + Azure DLS as the warehouse

This example targets `Postgres` as the sql-type `Iceberg` catalog and uses Azure DLS as the warehouse.

```yaml
source:
  type: "iceberg"
  config:
    env: dev
    catalog:
      demo:
        type: sql
        uri: postgresql+psycopg2://user:password@sqldatabase.postgres.database.azure.com:5432/icebergcatalog
        adlfs.tenant-id: <Azure tenant ID>
        adlfs.account-name: <Azure storage account name>
        adlfs.client-id: <Azure Client/Application ID>
        adlfs.client-secret: <Azure Client Secret>
```

## Concept Mapping

<!-- This should be a manual mapping of concepts from the source to the DataHub Metadata Model -->
<!-- Authors should provide as much context as possible about how this mapping was generated, including assumptions made, known shortcuts, & any other caveats -->

This ingestion source maps the following Source System Concepts to DataHub Concepts:

<!-- Remove all unnecessary/irrelevant DataHub Concepts -->

| Source Concept                                                                                                                          | DataHub Concept                                                        | Notes                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| --------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `iceberg`                                                                                                                               | [Data Platform](docs/generated/metamodel/entities/dataPlatform.md)     |                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| Table                                                                                                                                   | [Dataset](docs/generated/metamodel/entities/dataset.md)                | An Iceberg table is registered inside a catalog using a name, where the catalog is responsible for creating, dropping and renaming tables. Catalogs manage a collection of tables that are usually grouped into namespaces. The name of a table is mapped to a Dataset name. If a [Platform Instance](https://docs.datahub.com/docs/platform-instances/) is configured, it will be used as a prefix: `<platform_instance>.my.namespace.table`. |
| [Table property](https://iceberg.apache.org/docs/latest/configuration/#table-properties)                                                | [User (a.k.a CorpUser)](docs/generated/metamodel/entities/corpuser.md) | The value of a table property can be used as the name of a CorpUser owner. This table property name can be configured with the source option `user_ownership_property`.                                                                                                                                                                                                                                                                        |
| [Table property](https://iceberg.apache.org/docs/latest/configuration/#table-properties)                                                | CorpGroup                                                              | The value of a table property can be used as the name of a CorpGroup owner. This table property name can be configured with the source option `group_ownership_property`.                                                                                                                                                                                                                                                                      |
| Table parent folders (excluding [warehouse catalog location](https://iceberg.apache.org/docs/latest/configuration/#catalog-properties)) | Container                                                              | Available in a future release                                                                                                                                                                                                                                                                                                                                                                                                                  |
| [Table schema](https://iceberg.apache.org/spec/#schemas-and-data-types)                                                                 | SchemaField                                                            | Maps to the fields defined within the Iceberg table schema definition.                                                                                                                                                                                                                                                                                                                                                                         |

## Troubleshooting

### Exceptions while increasing `processing_threads`

Each processing thread will open several files/sockets to download manifest files from blob storage. If you experience
exceptions appearing when increasing `processing_threads` configuration parameter, try to increase limit of open
files (e.g. using `ulimit` in Linux).

## DataHub Iceberg REST Catalog

DataHub also implements the Iceberg REST Catalog. See the [Iceberg Catalog documentation](docs/iceberg-catalog.md) for more details.
