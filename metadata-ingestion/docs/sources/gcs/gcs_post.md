### Capabilities

Use the **Important Capabilities** table above as the source of truth for supported features and whether additional configuration is required.

#### Path Specs

**Example - Dataset per file**

Bucket structure:

```
test-gs-bucket
├── employees.csv
└── food_items.csv
```

Path specs config

```
path_specs:
    - include: gs://test-gs-bucket/*.csv

```

**Example - Datasets with partitions**

Bucket structure:

```
test-gs-bucket
├── orders
│   └── year=2022
│       └── month=2
│           ├── 1.parquet
│           └── 2.parquet
└── returns
    └── year=2021
        └── month=2
            └── 1.parquet

```

Path specs config:

```
path_specs:
    - include: gs://test-gs-bucket/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.parquet
```

**Example - Datasets with partition and exclude**

Bucket structure:

```
test-gs-bucket
├── orders
│   └── year=2022
│       └── month=2
│           ├── 1.parquet
│           └── 2.parquet
└── tmp_orders
    └── year=2021
        └── month=2
            └── 1.parquet


```

Path specs config:

```
path_specs:
    - include: gs://test-gs-bucket/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.parquet
      exclude:
        - **/tmp_orders/**
```

**Example - Datasets of mixed nature**

Bucket structure:

```
test-gs-bucket
├── customers
│   ├── part1.json
│   ├── part2.json
│   ├── part3.json
│   └── part4.json
├── employees.csv
├── food_items.csv
├── tmp_10101000.csv
└──  orders
    └── year=2022
        └── month=2
            ├── 1.parquet
            ├── 2.parquet
            └── 3.parquet

```

Path specs config:

```
path_specs:
    - include: gs://test-gs-bucket/*.csv
      exclude:
        - **/tmp_10101000.csv
    - include: gs://test-gs-bucket/{table}/*.json
    - include: gs://test-gs-bucket/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.parquet
```

**Valid path_specs.include**

```python
gs://my-bucket/foo/tests/bar.avro # single file table
gs://my-bucket/foo/tests/*.* # mulitple file level tables
gs://my-bucket/foo/tests/{table}/*.avro #table without partition
gs://my-bucket/foo/tests/{table}/*/*.avro #table where partitions are not specified
gs://my-bucket/foo/tests/{table}/*.* # table where no partitions as well as data type specified
gs://my-bucket/{dept}/tests/{table}/*.avro # specifying keywords to be used in display name
gs://my-bucket/{dept}/tests/{table}/{partition_key[0]}={partition[0]}/{partition_key[1]}={partition[1]}/*.avro # specify partition key and value format
gs://my-bucket/{dept}/tests/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.avro # specify partition value only format
gs://my-bucket/{dept}/tests/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.* # for all extensions
gs://my-bucket/*/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.* # table is present at 2 levels down in bucket
gs://my-bucket/*/*/{table}/{partition[0]}/{partition[1]}/{partition[2]}/*.* # table is present at 3 levels down in bucket
```

**Valid path_specs.exclude**

- \*\*/tests/\*\*
- gs://my-bucket/hr/\*\*
- \*_/tests/_.csv
- gs://my-bucket/foo/\*/my_table/\*\*

**Notes**

- `{table}` represents folder for which dataset will be created.
- include path must end with (`*.*` or `*.[ext]`) to represent leaf level.
- if `*.[ext]` is provided then only files with specified type will be scanned.
- `/*/ `represents single folder.
- `{partition[i]}` represents value of partition.
- `{partition_key[i]}` represents name of the partition.
- While extracting, "i" will be used to match partition_key to partition.
- all folder levels need to be specified in include. Only exclude path can have `**` like matching.
- exclude path cannot have named variables ( {} ).
- Folder names should not contain {, }, \*, / in their names.
- {folder} is reserved for internal working. please do not use in named variables.

If you would like to write a more complicated function for resolving file names, then a {transformer} would be a good fit.

:::caution

Specify as long fixed prefix ( with out /\*/ ) as possible in `path_specs.include`. This will reduce the scanning time and cost, specifically on Google Cloud Storage.

:::

:::caution

If you are ingesting datasets from Google Cloud Storage, we recommend running the ingestion on a server in the same region to avoid high egress costs.

:::

#### Checking path_specs before you ingest

`datahub recipe probe` lists what your recipe can see and judges it with the same `path_specs`
rules ingestion uses, without ingesting anything:

```shell
datahub recipe probe methods --recipe gcs.yml
datahub recipe probe run buckets --recipe gcs.yml
datahub recipe probe run folders --recipe gcs.yml --bucket my-bucket --prefix data/
datahub recipe probe run objects --recipe gcs.yml --bucket my-bucket --prefix data/
datahub recipe probe run datasets --recipe gcs.yml --path-spec 0 --report-to datasets.json
datahub recipe probe filter --recipe gcs.yml --from-run datasets.json
datahub recipe probe filter --recipe gcs.yml --kind Table --name gs://my-bucket/data/events
datahub recipe probe run path_spec_folders --recipe gcs.yml --path-spec 1 --report-to folders.json
datahub recipe probe filter --recipe gcs.yml --kind Folder --from-run folders.json
```

- The probe uses the same S3-compatible client and credentials as ingestion, and only lists
  (`ListBuckets`, `ListObjectsV2`). It never reads object contents. That limit is in the
  probe's code, not in the credential: `roles/storage.objectViewer` can read objects too.
- For a probe-only credential, grant `storage.buckets.list` and `storage.objects.list` only.
  Without `storage.buckets.list` on the project, `buckets` reports a failure, not an empty
  list.
- Names are `gs://` URIs, as you write them in `path_specs`. Bucket names are bare
  (`my-bucket`).
- `datasets` lists candidates, including the ones `path_specs` drop; `probe filter` says which
  rule drops each one (for example `path_specs[0].tables_filter_pattern`).
- For a `{table}` path_spec the verdict is decided at the table folder. `exclude`, `file_types`
  and the files in the selected partitions are applied per file during ingestion, so a table
  folder whose files all fail them is reported as included, with a warning. Use `objects` to
  check its files.
- `folders` lists the folders one level under a bucket and prefix, to write an include against.
  For an `emit_folders_only` path_spec, `datasets` lists nothing: list its folders with
  `path_spec_folders --path-spec N` and judge them with `probe filter --kind Folder`.
- A listing denied while `datasets` or `path_spec_folders` walks a path_spec (resolving its
  wildcards, or listing its table folders) is reported as a failure, at any depth: ingestion
  stops the whole run on it, so later path_specs are not ingested either. The rows listed
  before the denial are still returned. A missing bucket under a `{table}` path_spec is only a
  warning, because ingestion warns "Missing bucket" and skips that path_spec.
- A credential that is rejected (a wrong HMAC secret, or WIF / ADC credentials that cannot be
  loaded or refreshed) is reported as a connection error. The error names the failure type
  only, never the credential material, token file paths or the token endpoint's response.

### Limitations

Module behavior is constrained by source APIs, permissions, and metadata exposed by the platform. Refer to capability notes for unsupported or conditional features.

#### Supported file types

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

#### File type detection

The format of a file is detected from its name. The connector first checks the apparent extension (everything after the last dot) and accepts it only if it matches one of the supported file types above. For files compressed with `.gz`, `.gzip`, or `.bz2` (when `enable_compression` is enabled on the path_spec, which is the default), the compression suffix is stripped and the inner extension is checked the same way (so `data.json.gz` is treated as JSON). Extension matching is case-insensitive.

File names whose stem contains dots — for example `events.account.update-2026-05-27-<hash>.gz` — are **not** misinterpreted as having an extension of `.update-2026-05-27-<hash>`. When compression is enabled, such files fall back to `path_spec.default_extension` if it is set, and are skipped otherwise; when compression is disabled, compressed files are skipped. When `default_extension` is set, any other file whose format cannot be inferred from its name is parsed as that format, including stray files such as `.crc` checksums or `.txt` manifests; use the path_spec `exclude` patterns to filter those out.

#### Profiling

Profiling is supported for GCS and, when enabled, extracts:

- Row and column counts for each dataset
- For each column, if profiling is enabled:
  - null counts and proportions
  - distinct counts and proportions
  - minimum, maximum, mean, median, standard deviation, some quantile values
  - histograms or frequencies of unique values

Profiling is a pure-Python implementation (built on `pyarrow` and Apache DataSketches) and requires no Spark, Hadoop, or JVM. Distinct counts and quantiles/histograms are approximate (DataSketches). GCS files are read over the S3-interoperability endpoint using the same credentials configured for ingestion, so no extra setup is needed beyond enabling profiling.

Enabling profiling will slow down ingestion runs.

### Troubleshooting

If ingestion fails, validate credentials, permissions, connectivity, and scope filters first. Then review ingestion logs for source-specific errors and adjust configuration accordingly.
