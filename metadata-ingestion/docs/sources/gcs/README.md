This connector ingests Google Cloud Storage datasets into DataHub. It allows mapping an individual file or a folder of files to a dataset in DataHub.
To specify the group of files that form a dataset, use `path_specs` configuration in ingestion recipe. This source leverages [Interoperability of GCS with S3](https://cloud.google.com/storage/docs/interoperability)
and uses DataHub S3 Data Lake integration source under the hood. Refer section [Path Specs](https://docs.datahub.com/docs/generated/ingestion/sources/s3/#path-specs) from S3 connector for more details.

### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept                             | DataHub Concept                                                                           | Notes                |
| ------------------------------------------ | ----------------------------------------------------------------------------------------- | -------------------- |
| `"Google Cloud Storage"`                   | [Data Platform](https://docs.datahub.com/docs/generated/metamodel/entities/dataplatform/) |                      |
| GCS object / Folder containing GCS objects | [Dataset](https://docs.datahub.com/docs/generated/metamodel/entities/dataset/)            |                      |
| GCS bucket                                 | [Container](https://docs.datahub.com/docs/generated/metamodel/entities/container/)        | Subtype `GCS bucket` |
| GCS folder                                 | [Container](https://docs.datahub.com/docs/generated/metamodel/entities/container/)        | Subtype `Folder`     |

### Supported file types

Supported file types are as follows:

- CSV
- TSV
- JSONL
- JSON
- Parquet
- Apache Avro

Schemas for Parquet and Avro files are extracted as provided.

Schemas for schemaless formats (CSV, TSV, JSONL, JSON) are inferred. For CSV, TSV and JSONL files, we consider the first 100 rows by default, which can be controlled via the `max_rows` recipe parameter (see [below](#config-details))
JSON file schemas are inferred on the basis of the entire file (given the difficulty in extracting only the first few objects of the file), which may impact performance.
We are working on using iterator-based JSON parsers to avoid reading in the entire JSON object.

### Prerequisites

Complete the setup for your chosen authentication method:

**For HMAC authentication (default):**

1. Create a service account with "Storage Object Viewer" role — [Create a service account](https://cloud.google.com/iam/docs/service-accounts-create).
2. Ensure you meet the [requirements to generate an HMAC key](https://cloud.google.com/storage/docs/authentication/managing-hmackeys#before-you-begin).
3. Create an HMAC key for the service account — [Create HMAC keys](https://cloud.google.com/storage/docs/authentication/managing-hmackeys#create).

**For Workload Identity Federation:**

1. Set up a Workload Identity Pool and Provider in Google Cloud — [Workload Identity Federation](https://cloud.google.com/iam/docs/workload-identity-federation).
2. Configure the credential (file path, inline JSON, or JSON string) that the connector will use to obtain short-lived access tokens. For Kubernetes (GKE), see [Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity).
