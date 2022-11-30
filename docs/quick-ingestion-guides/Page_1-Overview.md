
# DataHub BigQuery UI Ingestion Guide: Overview

## What You Will Get Out of This Guide

* This guide is for ingestion through the DataHub UI, not for CLI (command line interface) ingestion
* You will have learned how to set up the [BigQuery] connector using the DataHub UI
* At the end of this guide, you will have extracted [Projects](https://cloud.google.com/bigquery/docs/resource-hierarchy#projects), [Datasets](https://cloud.google.com/bigquery/docs/datasets-intro), [Tables](https://cloud.google.com/bigquery/docs/tables-intro), [Views](https://cloud.google.com/bigquery/docs/views-intro), [Materialized Views](https://cloud.google.com/bigquery/docs/materialized-views-intro) from [Source] into your DataHub instance, and set up a recurring link between [BigQuery] and DataHub.
* For the extracted assets will have:
  - usage statistics
  - table level lineage
  - Table and colum level profile statistics
* Set up a recurring link between [BigQuery] and DataHub

:::caution
You will NOT have extracted [Routines](https://cloud.google.com/bigquery/docs/routines), [Search Indexes](https://cloud.google.com/bigquery/docs/search-intro) from [BigQuery], as the connector does not support ingesting these assets

## Next Steps
If that all sounds like what you're looking for, navigate to the [next page](Page_2-Setup.md), where we'll talk about prerequesites

## Advanced Guides and Reference
If you're looking to do something more in-depth, want to use CLI instead of the DataHub UI, or just need to look at
the reference documentation for this connector, use these links:
- https://datahubproject.io/docs/generated/ingestion/sources/bigquery

Bulleted list of more in-depth guides, either for CLI or custom configuration, as well as links to the reference docs-->

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*