# Introduction to Metadata Ingestion

:::tip Find Integration Source
Please see our **[Integrations page](https://datahubproject.io/integrations)** to browse our ingestion sources and filter on their features.
:::

## Integration Methods

DataHub offers three methods for data ingestion:

- UI ingestion
- CLI ingestion
- SDK-based ingestion

### UI Ingestion

DataHub supports configuring and monitoring ingestion via the UI.
For a detailed guide on UI ingestion, please refer to the [UI Ingestion](../docs/ui-ingestion.md) page.

### CLI Ingestion

DataHub supports configuring ingestion via [CLI](../docs/cli.md).
For more information, refer to the [CLI Ingestion guide](cli-ingestion.md).

### SDK-based ingestion

In some cases, you might want to construct Metadata events directly and use programmatic ways to emit that metadata to DataHub.
In this case, take a look at the [Python emitter](./as-a-library.md) and the [Java emitter](../metadata-integration/java/as-a-library.md) libraries which can be called from your own code.

For instance, if you want to configure and run a pipeline entirely from within your custom Python script, please refer to [programmatic_pipeline.py](./examples/library/programatic_pipeline.py) - a basic mysql to REST programmatic pipeline.

## Types of Integration

Integration can be divided into two concepts based on the method:

- Push-based integration
- Pull-based integration

### Push-based Integration

Push-based integrations allow you to emit metadata directly from your data systems when metadata changes, while pull-based integrations allow you to "crawl" or "ingest" metadata from the data systems by connecting to them and extracting metadata in a batch or incremental-batch manner. Supporting both mechanisms means that you can integrate with all your systems in the most flexible way possible.
Examples of push-based integrations include [Airflow](../docs/lineage/airflow.md), [Spark](../metadata-integration/java/spark-lineage/README.md), [Great Expectations](./integration_docs/great-expectations.md) and [Protobuf Schemas](../metadata-integration/java/datahub-protobuf/README.md). This allows you to get low-latency metadata integration from the "active" agents in your data ecosystem.

### Pull-based Integration

Examples of pull-based integrations include BigQuery, Snowflake, Looker, Tableau and many others.
This document describes the pull-based metadata ingestion system that is built into DataHub for easy integration with a wide variety of sources in your data stack.

## Core Concepts

The following are the core concepts related to ingestion:

- [Sources](source_overview.md) : Data systems from which extract metadata. (e.g. BigQuery, MySQL)
- [Sinks](sink_overview.md) : Destination for metadata (e.g. File, DataHub)
- [Recipe](recipe_overview.md) : The main configuration for ingestion in the form or .yaml file

For more advanced guides, please refer to the following:

- [Developing on Metadata Ingestion](./developing.md)
- [Adding a Metadata Ingestion Source](./adding-source.md)
- [Using Transformers](./docs/transformer/intro.md)
