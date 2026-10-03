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

#### File type detection

The format of a file is detected from its name. The connector first checks the apparent extension (everything after the last dot) and accepts it only if it matches one of the supported file types above. For files compressed with `.gz`, `.gzip`, or `.bz2`, the compression suffix is stripped and the inner extension is checked the same way (so `data.json.gz` is treated as JSON). Extension matching is case-insensitive.

File names whose stem contains dots — for example `events.account.update-2026-05-27-<hash>.gz` — are **not** misinterpreted as having an extension of `.update-2026-05-27-<hash>`. Such files fall back to `path_spec.default_extension` if it is set, and are skipped otherwise. When `default_extension` is set, any file whose format cannot be inferred from its name is parsed as that format, including stray files such as `.crc` checksums or `.txt` manifests; use the path_spec `exclude` patterns to filter those out.

Profiling is not available in the current release.

### Prerequisites

Before running ingestion, ensure network connectivity to the source, valid authentication credentials, and read permissions for metadata APIs required by this module.
