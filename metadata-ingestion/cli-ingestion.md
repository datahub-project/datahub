# CLI Ingestion

## Ingesting Metadata

```shell
pip install 'acryl-datahub[datahub-rest]'  # install the required plugin
datahub ingest -c <path_to_recipe_file>
```

For more information on configuring recipes, please refer to [Recipes](recipe_overview.md)

### --dry-run

The `--dry-run` option of the `ingest` command performs all of the ingestion steps, except writing to the sink. This is useful to validate that the
ingestion recipe is producing the desired metadata events before ingesting them into datahub.

```shell
# Dry run
datahub ingest -c ./examples/recipes/example_to_datahub_rest.dhub.yml --dry-run
# Short-form
datahub ingest -c ./examples/recipes/example_to_datahub_rest.dhub.yml -n
```

### --preview

The `--preview` option of the `ingest` command performs all of the ingestion steps, but limits the processing to only the first 10 workunits produced by the source.
This option helps with quick end-to-end smoke testing of the ingestion recipe.

```shell
# Preview
datahub ingest -c ./examples/recipes/example_to_datahub_rest.dhub.yml --preview
# Preview with dry-run
datahub ingest -c ./examples/recipes/example_to_datahub_rest.dhub.yml -n --preview
```

By default `--preview` creates 10 workunits. But if you wish to try producing more workunits you can use another option `--preview-workunits`

```shell
# Preview 20 workunits without sending anything to sink
datahub ingest -c ./examples/recipes/example_to_datahub_rest.dhub.yml -n --preview --preview-workunits=20
```

## Reporting

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

## Deploying and scheduling ingestion to the UI

The `deploy` subcommand of the `ingest` command tree allows users to upload their recipes and schedule them in the server.

```shell
datahub ingest deploy -n <user friendly name for ingestion> -c recipe.yaml
```

By default, no schedule is done unless explicitly configured with the `--schedule` parameter. Schedule timezones are UTC by default and can be overriden with `--time-zone` flag.

```shell
datahub ingest deploy -n test --schedule "0 * * * *" --time-zone "Europe/London" -c recipe.yaml
```

## Compatibility

DataHub server uses a 3 digit versioning scheme, while the CLI uses a 4 digit scheme. For example, if you're using DataHub server version 0.10.0, you should use CLI version 0.10.0.x, where x is a patch version.
We do this because we do CLI releases at a much higher frequency than server releases, usually every few days vs twice a month.

For ingestion sources, any breaking changes will be highlighted in the [release notes](../docs/how/updating-datahub.md). When fields are deprecated or otherwise changed, we will try to maintain backwards compatibility for two server releases, which is about 4-6 weeks. The CLI will also print warnings whenever deprecated options are used.
