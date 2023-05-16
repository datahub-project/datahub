---
title: Overview
---
# Looker & LookML Ingestion Guide: Overview

## What You Will Get Out of This Guide

This guide will help you set up the Looker & LookML connectors to begin ingesting metadata into DataHub.
 

Upon completing this guide, you will have a recurring ingestion pipeline that will extract metadata from Looker & LookML and load it into DataHub. This will include to following Looker asset types:

**Looker Connector Ingested Assets:**

* Dashboards
* Charts
* Explores 
* Schemas 
* Owners of Dashboards

    To get complete Looker metadata integration (including Looker views and lineage to the underlying warehouse tables), you must also use the `lookml` connector.

*To learn more about setting these advanced values, check out the [Looker Ingestion Source](https://datahubproject.io/docs/generated/ingestion/sources/Looker).*


**LookML Connector Ingested Assets**

* LookML views from model files in a project
* Metadata for dimensions
* Metadata for measures
* Dimension Groups as tag

    To get complete Looker metadata integration (including Looker dashboards and charts and lineage to the underlying Looker  views, you must also use the `looker` connector.

## Next Steps
Continue to the [setup guide](setup.md), where we'll describe the prerequisites.

## Advanced Guides and Reference

If you want to ingest metadata from Looker using the DataHub CLI, check out the following resources:

* Learn about CLI Ingestion in the [Introduction to Metadata Ingestion](../../../metadata-ingestion/README.md)
* [Looker Ingestion Source](https://datahubproject.io/docs/generated/ingestion/sources/Looker)

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*