## Overview

Looker is a business intelligence and analytics platform. Learn more in the [official Looker documentation](https://cloud.google.com/looker).

The DataHub integration for Looker covers BI entities such as dashboards, charts, datasets, and related ownership context. Depending on module capabilities, it can also capture features such as lineage, usage, profiling, ownership, tags, and stateful deletion detection.

## Concept Mapping

| Looker Concept                  | DataHub Concept             | Notes                                                   |
| ------------------------------- | --------------------------- | ------------------------------------------------------- |
| Dashboard / Look                | Dashboard / Chart           | Ingested by the `looker` module.                        |
| Explore / View model constructs | Dataset and lineage context | Ingested by the `lookml` module.                        |
| User, folder, model references  | Ownership/container context | Used to enrich governance metadata and discoverability. |
