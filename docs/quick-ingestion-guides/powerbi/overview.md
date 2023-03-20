---
title: Overview
---
# PowerBI Ingestion Guide: Overview

## What You Will Get Out of This Guide

This guide will help you set up the PowerBI connector to begin ingesting metadata into DataHub.

Upon completing this guide, you will have a recurring ingestion pipeline that will extract metadata from PowerBI and load it into DataHub. This will include to following PowerBI asset types:

* Databases
* Schemas
* Tables
* External Tables
* Views
* Materialized Views

The pipeline will also extract:

* **Usage statistics** to help you understand recent query activity (available if using PowerBI Enterprise edition or above)
* **Table- and Column-level lineage** to automatically define interdependencies between datasets and columns (available if using PowerBI Enterprise edition or above)
* **Table-level profile statistics** to help you understand the shape of the data

:::caution
You will NOT have extracted Stages, Snowpipes, Streams, Tasks, Procedures from PowerBI, as the connector does not support ingesting these assets yet.
:::

### Caveats

By default, DataHub only profiles datasets that have changed in the past 1 day. This can be changed in the YAML editor by setting the value of `profile_if_updated_since_days` to something greater than 1.

Additionally, DataHub only extracts usage and lineage information based on operations performed in the last 1 day. This can be changed by setting a custom value for `start_time` and `end_time` in the YAML editor.

*To learn more about setting these advanced values, check out the [PowerBI Ingestion Source](https://datahubproject.io/docs/generated/ingestion/sources/PowerBI/#module-PowerBI).*

## Next Steps

If that all sounds like what you're looking for, navigate to the [next page](setup.md), where we'll talk about prerequisites.

## Advanced Guides and Reference

If you want to ingest metadata from PowerBI using the DataHub CLI, check out the following resources:

* Learn about CLI Ingestion in the [Introduction to Metadata Ingestion](../../../metadata-ingestion/README.md)
* [PowerBI Ingestion Source](https://datahubproject.io/docs/generated/ingestion/sources/PowerBI/#module-PowerBI)

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*