### Overview

The `abs` module ingests metadata from Abs into DataHub. It is intended for production ingestion workflows and module-specific capabilities are documented below.

This connector supports both local files and those stored on Azure Blob Storage (which must be identified using the
prefix `http(s)://<account>.blob.core.windows.net/` or `azure://`).

#### Supported file types

Supported file types are as follows:

- CSV (`*.csv`)
- TSV (`*.tsv`)
- JSONL (`*.jsonl`)
- JSON (`*.json`)
- Parquet (`*.parquet`)
- Apache Avro (`*.avro`)

Schemas for Parquet and Avro files are extracted as provided.

Schemas for schemaless formats (CSV, TSV, JSONL, JSON) are inferred. For CSV, TSV and JSONL files, we consider the first
100 rows by default, which can be controlled via the `max_rows` recipe parameter (see [below](#config-details))
JSON file schemas are inferred on the basis of the entire file (given the difficulty in extracting only the first few
objects of the file), which may impact performance.
We are working on using iterator-based JSON parsers to avoid reading in the entire JSON object.

Profiling is not available in the current release.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.
