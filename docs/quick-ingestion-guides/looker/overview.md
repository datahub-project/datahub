---
title: Overview
---
# Looker & LookML Ingestion Guide: Overview

## What You Will Get Out of This Guide

This guide will help you set up the Looker & LookML connectors to begin ingesting metadata into DataHub.
Upon completing this guide, you will have a recurring ingestion pipeline to extract metadata from Looker & LookML and load it into DataHub. 

### Looker

Looker connector will ingest Looker asset types:

* [Dashboards](https://cloud.google.com/looker/docs/dashboards)
* [Charts](https://cloud.google.com/looker/docs/creating-visualizations)
* [Explores](https://cloud.google.com/looker/docs/reference/param-explore-explore) 
* [Schemas](https://developers.looker.com/api/explorer/4.0/methods/Metadata/connection_schemas) 
* [Owners of Dashboards](https://cloud.google.com/looker/docs/creating-user-defined-dashboards)

:::note

To get complete Looker metadata integration (including Looker views and lineage to the underlying warehouse tables), you must also use the [lookml](https://datahubproject.io/docs/generated/ingestion/sources/looker#module-lookml) connector.

:::


### LookML 

LookMl connector will include the following LookML asset types:

* [LookML views from model files in a project](https://cloud.google.com/looker/docs/reference/param-view-view)
* [Metadata for dimensions](https://cloud.google.com/looker/docs/reference/param-field-dimension)
* [Metadata for measures](https://cloud.google.com/looker/docs/reference/param-measure-types)
* [Dimension Groups as tag](https://cloud.google.com/looker/docs/reference/param-field-dimension-group)

:::note

To get complete Looker metadata integration (including Looker views and lineage to the underlying warehouse tables), you must also use the [looker](https://datahubproject.io/docs/generated/ingestion/sources/looker#module-looker) connector.

:::

## Next Steps
Please continue to the [setup guide](setup.md), where we'll describe the prerequisites.

### Reference

If you want to ingest metadata from Looker using the DataHub CLI, check out the following resources:
* Learn about CLI Ingestion in the [Introduction to Metadata Ingestion](../../../metadata-ingestion/README.md)
* [Looker Ingestion Source](https://datahubproject.io/docs/generated/ingestion/sources/Looker)


