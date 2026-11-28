# datahub ingest

The `ingest` command allows you to ingest metadata from your sources using ingestion configuration files, which we call recipes.
Source specific crawlers are provided by plugins and might sometimes need additional extras to be installed. See [installing plugins](./plugins.md) for more information.
[Removing Metadata from DataHub](../how/delete-metadata.md) contains detailed instructions about how you can use the ingest command to perform operations like rolling-back previously ingested metadata through the `rollback` sub-command and listing all runs that happened through `list-runs` sub-command.

```console
Usage: datahub [datahub-options] ingest [command-options]

Command Options:
  -c / --config             Config file in .toml or .yaml format
  -n / --dry-run            Perform a dry run of the ingestion, essentially skipping writing to sink
  --preview                 Perform limited ingestion from the source to the sink to get a quick preview
  --preview-workunits       The number of workunits to produce for preview
  --strict-warnings         If enabled, ingestion runs with warnings will yield a non-zero error code
  --test-source-connection  When set, ingestion will only test the source connection details from the recipe
  --no-progress             If enabled, mute intermediate progress ingestion reports
```

### ingest --dry-run

The `--dry-run` option of the `ingest` command performs all of the ingestion steps, except writing to the sink. This is useful to validate that the
ingestion recipe is producing the desired metadata events before ingesting them into datahub.

```shell
# Dry run
datahub ingest -c ./examples/recipes/example_to_datahub_rest.dhub.yaml --dry-run
# Short-form
datahub ingest -c ./examples/recipes/example_to_datahub_rest.dhub.yaml -n
```

### ingest list-source-runs

The `list-source-runs` option of the `ingest` command lists the previous runs, displaying their run ID, source name,
start time, status, and source URN. This command allows you to filter results using the --urn option for URN-based
filtering or the --source option to filter by source name (partial or complete matches are supported).

```shell
# List all ingestion runs
datahub ingest list-source-runs
# Filter runs by a source name containing "demo"
datahub ingest list-source-runs --source "demo"
```

### ingest --preview

The `--preview` option of the `ingest` command performs all of the ingestion steps, but limits the processing to only the first 10 workunits produced by the source.
This option helps with quick end-to-end smoke testing of the ingestion recipe.

```shell
# Preview
datahub ingest -c ./examples/recipes/example_to_datahub_rest.dhub.yaml --preview
# Preview with dry-run
datahub ingest -c ./examples/recipes/example_to_datahub_rest.dhub.yaml -n --preview
```

By default `--preview` creates 10 workunits. But if you wish to try producing more workunits you can use another option `--preview-workunits`

```shell
# Preview 20 workunits without sending anything to sink
datahub ingest -c ./examples/recipes/example_to_datahub_rest.dhub.yaml -n --preview --preview-workunits=20
```

### ingest --no-default-report

By default, the cli sends an ingestion report to DataHub, which allows you to see the result of all cli-based ingestion in the UI. This can be turned off with the `--no-default-report` flag.

```shell
# Running ingestion with reporting to DataHub turned off
datahub ingest -c ./examples/recipes/example_to_datahub_rest.dhub.yaml --no-default-report
```

The reports include the recipe that was used for ingestion. This can be turned off by adding an additional section to the ingestion recipe.

```yaml
source:
  # source configs

sink:
  # sink configs

# Add configuration for the datahub reporter
reporting:
  - type: datahub
    config:
      report_recipe: false

# Optional log to put failed JSONs into a file
# Helpful in case you are trying to debug some issue with specific ingestion failing
failure_log:
  enabled: false
  log_config:
    filename: ./path/to/failure.json
```

### ingest --record (Beta)

:::note Beta Feature
Recording and replay is currently in beta. The feature is stable for debugging purposes but the archive format may change in future releases.
:::

The `--record` option enables recording of all HTTP requests and database queries during ingestion. This creates an encrypted archive that can be replayed offline for debugging.

```shell
# Record an ingestion run with password protection
datahub ingest -c ./recipe.yaml --record --record-password mysecret

# Record to a specific local directory
export INGESTION_ARTIFACT_DIR=/path/to/recordings
datahub ingest -c ./recipe.yaml --record --record-password mysecret --no-s3-upload

# Record and upload directly to S3
datahub ingest -c ./recipe.yaml --record --record-password mysecret \
    --record-output-path s3://my-bucket/recordings/my-run.zip
```

Recording options:

| Option                  | Description                                                                                                |
| ----------------------- | ---------------------------------------------------------------------------------------------------------- |
| `--record`              | Enable recording of the ingestion run                                                                      |
| `--record-password`     | Password for encrypting the archive. Can also be set via `DATAHUB_RECORDING_PASSWORD` environment variable |
| `--record-output-path`  | Path to save the recording archive. Use local path or S3 URL (`s3://bucket/path/file.zip`)                 |
| `--no-s3-upload`        | Disable S3 upload (save locally only)                                                                      |
| `--no-secret-redaction` | Keep actual credentials in recording (use with caution, for local debugging only)                          |

The recording creates an encrypted ZIP archive containing HTTP cassettes, database query recordings, and a redacted recipe. This archive can be replayed using `datahub ingest replay`.

**Installation:** Recording requires the `debug-recording` plugin:

```shell
pip install 'acryl-datahub[debug-recording]'
```

➡️ [Learn more about recording and debugging ingestion](../how/debug-ingestion-recording.md)

## ingest replay (Beta)

The `ingest replay` command replays a recorded ingestion run for debugging. This allows you to reproduce issues in an air-gapped environment without network access.

```shell
# Replay from local file
datahub ingest replay ./recording.zip --password mysecret

# Replay from S3
datahub ingest replay s3://bucket/recordings/run-id.zip --password mysecret

# Replay with live sink (emit to real DataHub instance)
datahub ingest replay ./recording.zip --password mysecret --live-sink --server http://localhost:8080
```

Replay options:

| Option        | Description                                                                                                |
| ------------- | ---------------------------------------------------------------------------------------------------------- |
| `--password`  | Password for decrypting the archive. Can also be set via `DATAHUB_RECORDING_PASSWORD` environment variable |
| `--live-sink` | Emit to real GMS server instead of using recorded responses                                                |
| `--server`    | GMS server URL when using `--live-sink`                                                                    |
| `--report-to` | Path to write the report file                                                                              |

## recording (Beta)

The `recording` command group provides utilities for working with recording archives.

```shell
# View archive metadata
datahub recording info recording.zip --password mysecret

# Extract archive contents
datahub recording extract recording.zip --password mysecret --output-dir ./extracted

# List archive contents
datahub recording list recording.zip --password mysecret
```

### recording info

Display metadata about a recording archive including run ID, source type, creation time, and whether an exception was captured.

```shell
datahub recording info recording.zip --password mysecret
```

Use `--json` for machine-readable output.

### recording extract

Extract a recording archive to inspect its contents:

```shell
datahub recording extract recording.zip --password mysecret --output-dir ./extracted
```

### recording list

List the files contained in a recording archive:

```shell
datahub recording list recording.zip --password mysecret
```

## ingest deploy

The `ingest deploy` command instructs the cli to upload an ingestion recipe to DataHub to be run by DataHub's [UI Ingestion](../ui-ingestion.md).
This command can also be used to schedule the ingestion while uploading or even to update existing sources. It will upload to the remote instance the
CLI is connected to, not the sink of the recipe. Use `datahub init` to set the remote if not already set.

This command will automatically create a new recipe if it doesn't exist, or update it if it does.
Note that this is a complete update, and will remove any options that were previously set.
I.e: Not specifying a schedule in the cli update command will remove the schedule from the recipe to be updated.

### Command Options

```console
Usage: datahub ingest deploy [OPTIONS]

Options:
  -n, --name TEXT     Recipe Name
  -c, --config FILE   Config file in .toml or .yaml format.  [required]
  --urn TEXT          Urn of recipe to update. If not specified here or in the recipe's pipeline_name,
                      this will create a new ingestion source.
  --executor-id TEXT  Executor id to route execution requests to. Do not use this unless you have
                      configured a custom executor.
  --cli-version TEXT  Provide a custom CLI version to use for ingestion. By default will use server
                      default.
  --schedule TEXT     Cron definition for schedule. If none is provided, ingestion recipe will not be
                      scheduled
  --time-zone TEXT    Timezone for the schedule in 'America/New_York' format. Uses UTC by default.
  --debug BOOLEAN     Should we debug.
  --extra-pip TEXT    Extra pip packages. e.g. ["memray"]
  --extra-env TEXT    Environment variables as comma-separated KEY=VALUE pairs.
                      e.g. "VAR1=value1,VAR2=value2"
```

### Examples

**Schedule a recipe with default executor:**

```shell
datahub ingest deploy --name "Snowflake Integration" --schedule "0 5 * * *" --time-zone "Europe/London" -c recipe.yaml
```

**Deploy to a specific remote executor:**

```shell
datahub ingest deploy --name "Remote Snowflake Integration" --executor-id "remote-executor-pool-1" --schedule "0 5 * * *" -c recipe.yaml
```

**Update an existing recipe:**

```shell
datahub ingest deploy --urn "urn:li:dataHubIngestionSource:deploy-12345678" --schedule "0 6 * * *" -c updated_recipe.yaml
```

**Deploy with environment variables:**

```shell
datahub ingest deploy --name "Snowflake Integration" --extra-env "SNOWFLAKE_WAREHOUSE=COMPUTE_WH,SNOWFLAKE_ROLE=ANALYST" -c recipe.yaml
```

By default, the ingestion recipe's identifier is generated by hashing the name.
You can override the urn generation by passing the `--urn` flag to the CLI.

### Remote Executors

The `--executor-id` option allows you to route ingestion execution to specific executors:

- **Default executor** (`"default"`): Uses the managed executor provided by DataHub Cloud or your configured default executor
- **Remote executors**: Route to custom remote executors you've deployed in your environment

:::note
Use executor IDs other than "default" only if you have configured custom remote executors.
:::

**Examples with remote executors:**

```shell
# Deploy to default executor (can be configured as remote)
datahub ingest deploy --name "My Integration" -c recipe.yaml

# Deploy to specific remote executor pool
datahub ingest deploy --name "Private Network Integration" --executor-id "private-network-pool" -c recipe.yaml

# Deploy to region-specific executor
datahub ingest deploy --name "EU Region Integration" --executor-id "eu-west-executor" -c recipe.yaml
```

For more information on setting up remote executors, see the [Remote Executor Setup Guide](../managed-datahub/operator-guide/setting-up-remote-ingestion-executor.md).

### Using deployment section

As an alternative to configuring settings from the CLI, they can also be set in the `deployment` field of the recipe.

```yml
# deployment_recipe.yml
deployment:
  name: "Snowflake Integration"
  schedule: "0 5 * * *"
  time_zone: "Europe/London"
  executor_id: "remote-executor-pool-1" # Optional: specify remote executor
  cli_version: "0.15.0.1" # Optional: specify CLI version
  extra_pip: '["polars==1.35.2"]'
  extra_env: "VAR1=value1,VAR2=value2"

source: ...
```

```shell
datahub ingest deploy -c deployment_recipe.yml
```

CLI options will override corresponding values in the deployment section.

### Deployment Configuration Options

These deployment options that can be specified via CLI flags can also be configured in the `deployment` section:

| Field         | CLI Option      | Description                     | Default            |
| ------------- | --------------- | ------------------------------- | ------------------ |
| `name`        | `--name`        | Recipe name displayed in the UI | Required           |
| `schedule`    | `--schedule`    | Cron expression for scheduling  | None (manual only) |
| `time_zone`   | `--time-zone`   | Timezone for scheduled runs     | `"UTC"`            |
| `executor_id` | `--executor-id` | Target executor for ingestion   | `"default"`        |
| `cli_version` | `--cli-version` | CLI version for ingestion       | Server default     |
| `extra_pip`   | `--extra-pip`   | Extra pip packages              | None               |
| `extra_env`   | `--extra-env`   | Extra environment variables     | None               |

### Batch Deployment

Deploy multiple recipes from version control:

```shell
# Deploy every yml recipe in a directory
ls recipe_directory/*.yml | xargs -n 1 -I {} datahub ingest deploy -c {}

# Deploy with consistent executor across all recipes
ls recipe_directory/*.yml | xargs -n 1 -I {} datahub ingest deploy --executor-id "production-executor" -c {}
```
