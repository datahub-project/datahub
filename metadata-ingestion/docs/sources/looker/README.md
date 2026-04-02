## Overview

Looker is a business intelligence and analytics platform. Learn more in the [official Looker documentation](https://cloud.google.com/looker).

DataHub ingests dashboards, charts, explores, and LookML views from Looker. It can also
capture lineage to warehouse tables, usage statistics, ownership from Looker users, and folder
hierarchy as containers.

Use `looker-v2` for new ingestion setups. The older `looker` and `lookml` modules are
deprecated and will be removed in a future release. See the
[migration guide](https://datahubproject.io/docs/how/migrate-looker-to-looker-v2) if you're
on either.

## Concept Mapping

| Looker Concept                  | DataHub Concept             | Notes                                                        |
| ------------------------------- | --------------------------- | ------------------------------------------------------------ |
| Dashboard / Look                | Dashboard / Chart           | Ingested by `looker-v2` (or the deprecated `looker` module). |
| Explore / View model constructs | Dataset and lineage context | Ingested by `looker-v2` (or the deprecated `lookml` module). |
| User, folder, model references  | Ownership/container context | Used to enrich governance metadata and discoverability.      |
