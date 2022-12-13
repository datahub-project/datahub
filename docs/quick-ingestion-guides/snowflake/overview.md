---
title: Overview
---
# Snowflake Ingestion Guide: Overview

## What You Will Get Out of This Guide

This guide will help you set up the Snowflake connector through the DataHub UI to begin ingesting metadata into DataHub.

Upon completing this guide, you will have a recurring ingestion pipeline that will extract metadata from Snowflake and load it into DataHub. This will include to following Snowflake asset types:

* Databases
* Schemas
* Tables
* External Tables
* Views
* Materialized Views

This recurring ingestion pipeline will also extract:

* **Usage statistics** to help you understand recent query activity (available if using Snowflake Enterprise edition or above)
* **Table- and Column-level lineage** to automatically define interdependencies between datasets and columns (available if using Snowflake Enterprise edition or above)
* **Table- and column-level profile statistics** to help you understand the shape of the data

:::caution
You will NOT have extracted Stages, Snowpipes, Streams, Tasks, Procedures from Snowflake, as the connector does not support ingesting these assets yet.
:::

## Next Steps

If that all sounds like what you're looking for, navigate to the [next page](setup.md), where we'll talk about prerequisites.

## Advanced Guides and Reference

If you're looking to do something more in-depth, want to use CLI instead of the DataHub UI, or just need to look at the reference documentation for this connector, use these links:

* Learn about CLI Ingestion in the [Introduction to Metadata Ingestion](../../../metadata-ingestion/README.md)
* [Snowflake Ingestion Source](https://datahubproject.io/docs/generated/ingestion/sources/snowflake/#module-snowflake)

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*