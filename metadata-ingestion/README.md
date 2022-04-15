# Introduction to Metadata Ingestion

![Python version 3.6+](https://img.shields.io/badge/python-3.6%2B-blue)

### Metadata Ingestion Source Status

We apply a Support Status to each Metadata Source to help you understand the integration reliability at a glance.

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen): Certified Sources are well-tested & widely-adopted by the DataHub Community. We expect the integration to be stable with few user-facing issues.

![Incubating](https://img.shields.io/badge/support%20status-incubating-blue): Incubating Sources are ready for DataHub Community adoption but have not been tested for a wide variety of edge-cases. We eagerly solicit feedback from the Community to streghten the connector; minor version changes may arise in future releases.

![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey): Testing Sources are available for experiementation by DataHub Community members, but may change without notice. 

## Getting Started

### Prerequisites

Before running any metadata ingestion job, you should make sure that DataHub backend services are all running. If you are trying this out locally check out the [CLI](../docs/cli.md) to install the CLI and understand the options available in the CLI. You can reference the CLI usage guide given there as you go through this page.

### Core Concepts

## Recipes

A recipe is a configuration file that tells our ingestion scripts where to pull data from (source) and where to put it (sink).
Here's a simple example that pulls metadata from MSSQL (source) and puts it into datahub rest (sink).

> Note that one recipe file can only have 1 source and 1 sink. If you want multiple sources then you will need multiple recipe files.

```yaml
# A sample recipe that pulls metadata from MSSQL and puts it into DataHub
# using the Rest API.
source:
  type: mssql
  config:
    username: sa
    password: ${MSSQL_PASSWORD}
    database: DemoData

transformers:
  - type: "fully-qualified-class-name-of-transformer"
    config:
      some_property: "some.value"


sink:
  type: "datahub-rest"
  config:
    server: "http://localhost:8080"
```

A number of recipes are included in the [examples/recipes](./examples/recipes) directory. For full info and context on each source and sink, see the pages described in the [table of plugins](../docs/cli.md#installing-plugins).

### Handling sensitive information in recipes

We automatically expand environment variables in the config (e.g. `${MSSQL_PASSWORD}`),
similar to variable substitution in GNU bash or in docker-compose files. For details, see
https://docs.docker.com/compose/compose-file/compose-file-v2/#variable-substitution. This environment variable substitution should be used to mask sensitive information in recipe files. As long as you can get env variables securely to the ingestion process there would not be any need to store sensitive information in recipes.

### Basic Usage of CLI for ingestion

```shell
pip install 'acryl-datahub[datahub-rest]'  # install the required plugin
datahub ingest -c ./examples/recipes/mssql_to_datahub.yml
```

The `--dry-run` option of the `ingest` command performs all of the ingestion steps, except writing to the sink. This is useful to ensure that the
ingestion recipe is producing the desired workunits before ingesting them into datahub.

```shell
# Dry run
datahub ingest -c ./examples/recipes/example_to_datahub_rest.yml --dry-run
# Short-form
datahub ingest -c ./examples/recipes/example_to_datahub_rest.yml -n
```

The `--preview` option of the `ingest` command performs all of the ingestion steps, but limits the processing to only the first 10 workunits produced by the source.
This option helps with quick end-to-end smoke testing of the ingestion recipe.

```shell
# Preview
datahub ingest -c ./examples/recipes/example_to_datahub_rest.yml --preview
# Preview with dry-run
datahub ingest -c ./examples/recipes/example_to_datahub_rest.yml -n --preview
```

By default `--preview` creates 10 workunits. But if you wish to try producing more workunits you can use another option `--preview-workunits`

```shell
# Preview 20 workunits without sending anything to sink
datahub ingest -c ./examples/recipes/example_to_datahub_rest.yml -n --preview --preview-workunits=20
```

Sometimes, while running the ingestion pipeline, unexpected exceptions may occur. This can cause `stackprinter` to print all variables the logs. This may lead to credentials being written to logfiles. To prevent this behavior, in case of unexpected errors, a `--suppress-error-logs` option can be added to ingest cli command. By default, this option is set to false. However, if enabled, prevents printing all variables to logs, mitigating the risk of writing credentials to logs. The `--suppress-error-logs` option is applied when the ingestion pipeline is actually running.

```shell
# Running ingestion with --suppress-error-logs option
datahub ingest -c ./examples/recipes/example_to_datahub_rest.yml --suppress-error-logs
```

## Transformations

If you'd like to modify data before it reaches the ingestion sinks – for instance, adding additional owners or tags – you can use a transformer to write your own module and integrate it with DataHub.

Check out the [transformers guide](./transformers.md) for more info!

## Using as a library

In some cases, you might want to construct Metadata events directly and use programmatic ways to emit that metadata to DataHub. In this case, take a look at the [Python emitter](./as-a-library.md) and the [Java emitter](../metadata-integration/java/as-a-library.md) libraries which can be called from your own code. 

### Programmatic Pipeline
In some cases, you might want to configure and run a pipeline entirely from within your custom python script. Here is an example of how to do it.
 - [programmatic_pipeline.py](./examples/library/programatic_pipeline.py) - a basic mysql to REST programmatic pipeline.


## Developing

See the guides on [developing](./developing.md), [adding a source](./adding-source.md) and [using transformers](./transformers.md).

