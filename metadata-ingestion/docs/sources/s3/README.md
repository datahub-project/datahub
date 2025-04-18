This connector ingests AWS S3 datasets into DataHub. It allows mapping an individual file or a folder of files to a dataset in DataHub. 
Refer to the section [Path Specs](https://datahubproject.io/docs/generated/ingestion/sources/s3/#path-specs) for more details.

:::tip
This connector can also be used to ingest local files.
Just replace `s3://` in your path_specs with an absolute path to files on the machine running ingestion.
:::

### Supported file types
Supported file types are as follows:

- CSV (*.csv)
- TSV (*.tsv)
- JSONL (*.jsonl)
- JSON (*.json)
- Parquet (*.parquet)
- Apache Avro (*.avro)

Schemas for Parquet and Avro files are extracted as provided.

Schemas for schemaless formats (CSV, TSV, JSONL, JSON) are inferred. For CSV, TSV and JSONL files, we consider the first 100 rows by default, which can be controlled via the `max_rows` recipe parameter (see [below](#config-details))
JSON file schemas are inferred on the basis of the entire file (given the difficulty in extracting only the first few objects of the file), which may impact performance.
We are working on using iterator-based JSON parsers to avoid reading in the entire JSON object.

### Concept Mapping

This ingestion source maps the following Source System Concepts to DataHub Concepts:

| Source Concept                           | DataHub Concept                                                                            | Notes               |
| ---------------------------------------- |--------------------------------------------------------------------------------------------| ------------------- |
| `"s3"`                                   | [Data Platform](https://datahubproject.io/docs/generated/metamodel/entities/dataplatform/) |                     |
| s3 object / Folder containing s3 objects | [Dataset](https://datahubproject.io/docs/generated/metamodel/entities/dataset/)            |                     |
| s3 bucket                                | [Container](https://datahubproject.io/docs/generated/metamodel/entities/container/)        | Subtype `S3 bucket` |
| s3 folder                                | [Container](https://datahubproject.io/docs/generated/metamodel/entities/container/)        | Subtype `Folder`    |

### Profiling

This plugin extracts:
- Row and column counts for each dataset
- For each column, if profiling is enabled:
    - null counts and proportions
    - distinct counts and proportions
    - minimum, maximum, mean, median, standard deviation, some quantile values
    - histograms or frequencies of unique values

Note that because the profiling is run with PySpark, we require Spark 3.0.3 with Hadoop 3.2 to be installed (see [compatibility](#compatibility) for more details). If profiling, make sure that permissions for **s3a://** access are set because Spark and Hadoop use the s3a:// protocol to interface with AWS (schema inference outside of profiling requires s3:// access).
Enabling profiling will slow down ingestion runs.
