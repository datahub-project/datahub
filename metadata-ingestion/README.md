# Introduction to Metadata Ingestion

## Integration Options

DataHub supports both **push-based** and **pull-based** metadata integration. 

Push-based integrations allow you to emit metadata directly from your data systems when metadata changes, while pull-based integrations allow you to "crawl" or "ingest" metadata from the data systems by connecting to them and extracting metadata in a batch or incremental-batch manner. Supporting both mechanisms means that you can integrate with all your systems in the most flexible way possible. 

Examples of push-based integrations include [Airflow](../docs/lineage/airflow.md), [Spark](../metadata-integration/java/spark-lineage/README.md), [Great Expectations](./integration_docs/great-expectations.md) and [Protobuf Schemas](../metadata-integration/java/datahub-protobuf/README.md). This allows you to get low-latency metadata integration from the "active" agents in your data ecosystem. Examples of pull-based integrations include BigQuery, Snowflake, Looker, Tableau and many others.

This document describes the pull-based metadata ingestion system that is built into DataHub for easy integration with a wide variety of sources in your data stack.

## Getting Started

### Prerequisites

Before running any metadata ingestion job, you should make sure that DataHub backend services are all running. You can either run ingestion via the [UI](../docs/ui-ingestion.md) or via the [CLI](../docs/cli.md). You can reference the CLI usage guide given there as you go through this page.

## Core Concepts

### Sources

Data systems that we are extracting metadata from are referred to as **Sources**. The `Sources` tab on the left in the sidebar shows you all the sources that are available for you to ingest metadata from. For example, we have sources for [BigQuery](./source_docs/bigquery.md), [Looker](./source_docs/looker.md), [Tableau](./source_docs/tableau.md) and many others.

#### Metadata Ingestion Source Status

We apply a Support Status to each Metadata Source to help you understand the integration reliability at a glance.

![Certified](https://img.shields.io/badge/support%20status-certified-brightgreen): Certified Sources are well-tested & widely-adopted by the DataHub Community. We expect the integration to be stable with few user-facing issues.

![Incubating](https://img.shields.io/badge/support%20status-incubating-blue): Incubating Sources are ready for DataHub Community adoption but have not been tested for a wide variety of edge-cases. We eagerly solicit feedback from the Community to streghten the connector; minor version changes may arise in future releases.

![Testing](https://img.shields.io/badge/support%20status-testing-lightgrey): Testing Sources are available for experiementation by DataHub Community members, but may change without notice. 

### Sinks

Sinks are destinations for metadata. When configuring ingestion for DataHub, you're likely to be sending the metadata to DataHub over either the [REST (datahub-sink)](./sink_docs/datahub.md#datahub-rest) or the [Kafka (datahub-kafka)](./sink_docs/datahub.md#datahub-kafka) sink. In some cases, the [File](./sink_docs/file.md) sink is also helpful to store a persistent offline copy of the metadata during debugging.

The default sink that most of the ingestion systems and guides assume is the `datahub-rest` sink, but you should be able to adapt all of them for the other sinks as well!

### Recipes

A recipe is the main configuration file that puts it all together. It tells our ingestion scripts where to pull data from (source) and where to put it (sink).

Since `acryl-datahub` version `>=0.8.33.2`, the default sink is assumed to be a DataHub REST endpoint:
- Hosted at "http://localhost:8080" or the environment variable `${DATAHUB_HOST}` if present
- With an empty auth token or the environment variable `${DATAHUB_TOKEN}` if present. 

Here's a simple recipe that pulls metadata from MSSQL (source) and puts it into the default sink (datahub rest).

```yaml
# The simplest recipe that pulls metadata from MSSQL and puts it into DataHub
# using the Rest API.
source:
  type: mssql
  config:
    username: sa
    password: ${MSSQL_PASSWORD}
    database: DemoData

# sink section omitted as we want to use the default datahub-rest sink
```

Running this recipe is as simple as:
```shell
datahub ingest -c recipe.yaml
```

or if you want to override the default endpoints, you can provide the environment variables as part of the command like below:
```shell
DATAHUB_SERVER="https://my-datahub-server:8080" DATAHUB_TOKEN="my-datahub-token" datahub ingest -c recipe.yaml
```

A number of recipes are included in the [examples/recipes](./examples/recipes) directory. For full info and context on each source and sink, see the pages described in the [table of plugins](../docs/cli.md#installing-plugins).

> Note that one recipe file can only have 1 source and 1 sink. If you want multiple sources then you will need multiple recipe files.

### Handling sensitive information in recipes

We automatically expand environment variables in the config (e.g. `${MSSQL_PASSWORD}`),
similar to variable substitution in GNU bash or in docker-compose files. For details, see
https://docs.docker.com/compose/compose-file/compose-file-v2/#variable-substitution. This environment variable substitution should be used to mask sensitive information in recipe files. As long as you can get env variables securely to the ingestion process there would not be any need to store sensitive information in recipes.

### Basic Usage of CLI for ingestion

```shell
pip install 'acryl-datahub[datahub-rest]'  # install the required plugin
datahub ingest -c ./examples/recipes/mssql_to_datahub.yml
```

The `--dry-run` option of the `ingest` command performs all of the ingestion steps, except writing to the sink. This is useful to validate that the
ingestion recipe is producing the desired metadata events before ingesting them into datahub.

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

If you'd like to modify data before it reaches the ingestion sinks – for instance, adding additional owners or tags – you can use a transformer to write your own module and integrate it with DataHub. Transformers require extending the recipe with a new section to describe the transformers that you want to run.

For example, a pipeline that ingests metadata from MSSQL and applies a default "important" tag to all datasets is described below:
```yaml
# A recipe to ingest metadata from MSSQL and apply default tags to all tables
source:
  type: mssql
  config:
    username: sa
    password: ${MSSQL_PASSWORD}
    database: DemoData

transformers: # an array of transformers applied sequentially
  - type: simple_add_dataset_tags
    config:
      tag_urns:
        - "urn:li:tag:Important"

# default sink, no config needed
```

Check out the [transformers guide](./transformers.md) to learn more about how you can create really flexible pipelines for processing metadata using Transformers!

## Using as a library (SDK)

In some cases, you might want to construct Metadata events directly and use programmatic ways to emit that metadata to DataHub. In this case, take a look at the [Python emitter](./as-a-library.md) and the [Java emitter](../metadata-integration/java/as-a-library.md) libraries which can be called from your own code. 

### Programmatic Pipeline
In some cases, you might want to configure and run a pipeline entirely from within your custom Python script. Here is an example of how to do it.
 - [programmatic_pipeline.py](./examples/library/programatic_pipeline.py) - a basic mysql to REST programmatic pipeline.

## Developing

See the guides on [developing](./developing.md), [adding a source](./adding-source.md) and [using transformers](./transformers.md).

