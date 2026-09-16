## Overview

Looker is a business intelligence and analytics platform. Learn more in the [official Looker documentation](https://cloud.google.com/looker).

The DataHub integration for Looker covers BI entities such as dashboards, charts, datasets, and related ownership context. It also captures table- and column-level lineage, usage statistics, ownership, and stateful deletion detection.

## Concept Mapping

| Looker Concept                  | DataHub Concept             | Notes                                                   |
| ------------------------------- | --------------------------- | ------------------------------------------------------- |
| Dashboard / Look                | Dashboard / Chart           | Ingested by the `looker` module.                        |
| Explore / View model constructs | Dataset and lineage context | Ingested by the `lookml` module.                        |
| User, folder, model references  | Ownership/container context | Used to enrich governance metadata and discoverability. |
