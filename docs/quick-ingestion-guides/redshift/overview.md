---
title: Overview
---
# Redshift Ingestion Guide: Overview

## What You Will Get Out of This Guide

This guide will help you set up the Redshift connector through the DataHub UI to begin ingesting metadata into DataHub.

Upon completing this guide, you will have a recurring ingestion pipeline that will extract metadata from Redshift and load it into DataHub. This will include to following Redshift asset types:

* Database
* Schemas (External and Internal)
* Tables (External and Internal)
* Views

This recurring ingestion pipeline will also extract:

* **Usage statistics** to help you understand recent query activity
* **Table-level lineage** (where available) to automatically define interdependencies between datasets
* **Table- and column-level profile statistics** to help you understand the shape of the data

:::caution
The source currently can ingest one database with one recipe
:::

## Next Steps

If that all sounds like what you're looking for, navigate to the [next page](setup.md), where we'll talk about prerequisites

## Advanced Guides and Reference

If you're looking to do something more in-depth, want to use CLI instead of the DataHub UI, or just need to look at the reference documentation for this connector, use these links:

* Learn about CLI Ingestion in the [Introduction to Metadata Ingestion](../../../metadata-ingestion/README.md)
* [Redshift Ingestion Reference Guide](https://datahubproject.io/docs/generated/ingestion/sources/redshift/#module-redshift)


