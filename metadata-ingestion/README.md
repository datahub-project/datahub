# Introduction to Metadata Ingestion

:::tip Find Integration Source
Please see our **[Integrations page](https://docs.datahub.com/integrations)** to browse our ingestion sources and filter on their features.
:::

## Integration Methods

DataHub offers three methods for data ingestion:

- [UI Ingestion](../docs/ui-ingestion.md) : Easily configure and execute a metadata ingestion pipeline through the UI.
- [CLI Ingestion guide](cli-ingestion.md) : Configure the ingestion pipeline using YAML and execute by it through CLI.
- SDK-based ingestion : Use [Python Emitter](./as-a-library.md) or [Java emitter](../metadata-integration/java/as-a-library.md) to programmatically control the ingestion pipelines.

## Types of Integration

Integration can be divided into two concepts based on the method:

### Push-based Integration

Push-based integrations allow you to emit metadata directly from your data systems when metadata changes.
Examples of push-based integrations include [Airflow](../docs/lineage/airflow.md), [Spark](../metadata-integration/java/acryl-spark-lineage/README.md), [Great Expectations](./integration_docs/great-expectations.md) and [Protobuf Schemas](../metadata-integration/java/datahub-protobuf/README.md). This allows you to get low-latency metadata integration from the "active" agents in your data ecosystem.

### Pull-based Integration

Pull-based integrations allow you to "crawl" or "ingest" metadata from the data systems by connecting to them and extracting metadata in a batch or incremental-batch manner.
Examples of pull-based integrations include BigQuery, Snowflake, Looker, Tableau and many others.

## Core Concepts

The following are the core concepts related to ingestion:

- [Sources](source_overview.md): Data systems from which extract metadata. (e.g. BigQuery, MySQL)
- [Sinks](sink_overview.md): Destination for metadata (e.g. File, DataHub)
- [Recipe](recipe_overview.md): The main configuration for ingestion in the form or .yaml file

For more advanced guides, please refer to the following:

- [Developing on Metadata Ingestion](./developing.md)
- [Adding a Metadata Ingestion Source](./adding-source.md)
- [Using Transformers](./docs/transformer/intro.md)
