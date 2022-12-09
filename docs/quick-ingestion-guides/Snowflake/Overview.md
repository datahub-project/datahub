---
title: Overview
---
# Snowflake Ingestion Guide: Overview

## What You Will Get Out of This Guide

* This guide is for ingestion through the DataHub UI, not for CLI (command line interface) ingestion.
* You will have learnt how to set up the Snowflake connector using the DataHub UI.
* At the end of this guide, you will have extracted Databases, Schemas, Tables, External Tables, Views, Materialized Views from Snowflake into your DataHub instance, and set up a recurring link between Snowflake and DataHub.
* For the extracted Tables and Views, you will have:
  * Table level profile
  * Table and column level lineage (available if using Snowflake Enterprise edition or above)
  * Usage statistics (available if using Snowflake Enterprise edition or above)

:::caution
You will NOT have extracted Stages, Snowpipes, Streams, Tasks, Procedures from Snowflake, as the connector does not support ingesting these assets yet.
:::

## Next Steps

If that all sounds like what you're looking for, navigate to the [next page](Setup.md), where we'll talk about prerequisites.

## Advanced Guides and Reference

If you're looking to do something more in-depth, want to use CLI instead of the DataHub UI, or just need to look at the reference documentation for this connector, use this link:

* [Snowflake Ingestion Source](https://datahubproject.io/docs/generated/ingestion/sources/snowflake/#module-snowflake)

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*
