### Overview

The `dlt` module ingests pipeline metadata from [dlt (data load tool)](https://dlthub.com) into DataHub. It reads dlt's local state directory directly — no live connection to dlt or the destination is required for basic metadata extraction. If the dlt Python package is installed, the connector uses the SDK for richer metadata; otherwise it falls back to parsing the YAML state files directly.

#### What is ingested

- **DataFlow** — one per dlt pipeline (`pipeline_name`)
- **DataJob** — one per destination table (including auto-unnested child tables like `orders__items`)
- **Outlet lineage** — DataJob → destination Dataset URNs (Postgres, BigQuery, Snowflake, etc.)
- **Inlet lineage** — user-configured upstream Dataset URNs (dlt does not record source connection info)
- **Column-level lineage** — for direct-copy pipelines with exactly one inlet and one outlet
- **DataProcessInstance** — per-run history from `_dlt_loads` (opt-in)

#### How dlt stores metadata

dlt writes pipeline state to a local directory after each `pipeline.run()` call:

```
~/.dlt/pipelines/
  <pipeline_name>/
    schemas/
      <schema_name>.schema.yaml   # Table definitions with columns and types
    state.json                    # Destination type, dataset name, pipeline state
```

### Prerequisites

- dlt pipeline(s) must have been run at least once (state files are created automatically)
- The `pipelines_dir` must be accessible from where DataHub ingestion runs
- For run history: the dlt package must be installed and destination credentials must be configured (see Capabilities → Run History below)

#### Where to find your `pipelines_dir`

##### Local / Quickstart

dlt's default location. Works out of the box:

```yaml
pipelines_dir: "~/.dlt/pipelines"
```

##### CI/CD (GitHub Actions, Airflow, Jenkins)

dlt runs in one job and DataHub ingestion runs in another. Both must use the same path or shared storage:

```yaml
pipelines_dir: "/data/dlt-pipelines"
```

Many dlt users already set a `PIPELINES_DIR` environment variable:

```yaml
pipelines_dir: "${PIPELINES_DIR:-~/.dlt/pipelines}"
```

##### Kubernetes / Docker

dlt runs in one pod and DataHub in another. Mount the same PersistentVolumeClaim in both pods:

```yaml
pipelines_dir: "/mnt/dlt-pipelines"
```

#### Required permissions

The connector reads local files only — no network permissions are needed for basic metadata extraction.

| Feature                                        | Requirement                                                              |
| ---------------------------------------------- | ------------------------------------------------------------------------ |
| Pipeline metadata (DataFlow, DataJob, lineage) | Filesystem read access to `pipelines_dir`                                |
| Run history (`_dlt_loads`)                     | dlt package installed + destination credentials in `~/.dlt/secrets.toml` |
