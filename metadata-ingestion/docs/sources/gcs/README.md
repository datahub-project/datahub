This connector ingests Google Cloud Storage datasets into DataHub. It allows mapping an individual file or a folder of files to a dataset in DataHub. 
To specify the group of files that form a dataset, use `path_specs` configuration in ingestion recipe. This source leverages [Interoperability of GCS with S3](https://cloud.google.com/storage/docs/interoperability)
and uses DataHub S3 Data Lake integration source under the hood. Refer section [Path Specs](https://datahubproject.io/docs/generated/ingestion/sources/s3/#path-specs) from S3 connector for more details.



### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept                             | DataHub Concept                                                                            | Notes                |
| ------------------------------------------ |--------------------------------------------------------------------------------------------| -------------------- |
| `"Google Cloud Storage"`                   | [Data Platform](https://datahubproject.io/docs/generated/metamodel/entities/dataplatform/) |                      |
| GCS object / Folder containing GCS objects | [Dataset](https://datahubproject.io/docs/generated/metamodel/entities/dataset/)            |                      |
| GCS bucket                                 | [Container](https://datahubproject.io/docs/generated/metamodel/entities/container/)        | Subtype `GCS bucket` |
| GCS folder                                 | [Container](https://datahubproject.io/docs/generated/metamodel/entities/container/)        | Subtype `Folder`     |


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
1. Create a service account with "Storage Object Viewer" Role - https://cloud.google.com/iam/docs/service-accounts-create
2. Make sure you meet following requirements to generate HMAC key - https://cloud.google.com/storage/docs/authentication/managing-hmackeys#before-you-begin
3. Create an HMAC key for service account created above - https://cloud.google.com/storage/docs/authentication/managing-hmackeys#create .
