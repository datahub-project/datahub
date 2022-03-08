# UI Tabs Guide

Some of the tabs in the UI might not be enabled by default. This guide is supposed to tell Admins of DataHub how to enable those UI tabs.

## Datasets
### Stats and Queries Tab

To enable these tabs you need to use one of the usage sources which gets the relevant metadata from your sources and ingests them into DataHub. These usage sources are listed under other sources which support them e.g. [Snowflake source](../../metadata-ingestion/source_docs/snowflake.md), [BigQuery source](../../metadata-ingestion/source_docs/bigquery.md)

### Validation Tab

This tab is enabled if you use [Data Quality Integration with Great Expectations](../../metadata-ingestion/integration_docs/great-expectations.md).

## Common to multiple entities
### Properties Tab

Properties are a catch-all bag for metadata not captured in other aspects stored for a Dataset. These are populated via the various source connectors when [metadata is ingested](../../metadata-ingestion/README.md).