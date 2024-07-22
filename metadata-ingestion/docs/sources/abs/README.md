This connector ingests Azure Blob Storage (abbreviated to abs) datasets into DataHub. It allows mapping an individual
file or a folder of files to a dataset in DataHub.
To specify the group of files that form a dataset, use `path_specs` configuration in ingestion recipe. Refer
section [Path Specs](https://datahubproject.io/docs/generated/ingestion/sources/s3/#path-specs) for more details.

### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept                         | DataHub Concept                                                                            | Notes            |
|----------------------------------------|--------------------------------------------------------------------------------------------|------------------|
| `"abs"`                                | [Data Platform](https://datahubproject.io/docs/generated/metamodel/entities/dataplatform/) |                  |
| abs blob / Folder containing abs blobs | [Dataset](https://datahubproject.io/docs/generated/metamodel/entities/dataset/)            |                  |
| abs container                          | [Container](https://datahubproject.io/docs/generated/metamodel/entities/container/)        | Subtype `Folder` |

This connector supports both local files and those stored on Azure Blob Storage (which must be identified using the
prefix `http(s)://<account>.blob.core.windows.net/` or `azure://`).

### Supported file types

Supported file types are as follows:

- CSV (*.csv)
- TSV (*.tsv)
- JSONL (*.jsonl)
- JSON (*.json)
- Parquet (*.parquet)
- Apache Avro (*.avro)

Schemas for Parquet and Avro files are extracted as provided.

Schemas for schemaless formats (CSV, TSV, JSONL, JSON) are inferred. For CSV, TSV and JSONL files, we consider the first
100 rows by default, which can be controlled via the `max_rows` recipe parameter (see [below](#config-details))
JSON file schemas are inferred on the basis of the entire file (given the difficulty in extracting only the first few
objects of the file), which may impact performance.
We are working on using iterator-based JSON parsers to avoid reading in the entire JSON object.

### Profiling

Profiling is not available in the current release.
