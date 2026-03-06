## Overview

Looker ingestion in DataHub is split into two modules: `looker` for API metadata and `lookml` for model/repository metadata and lineage.

## Concept Mapping

| Looker Concept                  | DataHub Concept             | Notes                                                   |
| ------------------------------- | --------------------------- | ------------------------------------------------------- |
| Dashboard / Look                | Dashboard / Chart           | Ingested by the `looker` module.                        |
| Explore / View model constructs | Dataset and lineage context | Ingested by the `lookml` module.                        |
| User, folder, model references  | Ownership/container context | Used to enrich governance metadata and discoverability. |
