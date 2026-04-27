## Overview

[Hex](https://hex.tech) is a collaborative analytics platform for building SQL notebooks, Python analyses, and shareable data apps. It combines code cells, visualisations, and interactive inputs into a single shareable project.

DataHub's `hex-v2` connector ingests Hex workspace metadata including projects, components, ownership, usage statistics, upstream lineage (resolved directly from SQL cell sources), run history, and AI-readable context documents. Unlike the `hex` connector, `hex-v2` does not require a prior warehouse ingestion to produce lineage — it parses SQL directly from Hex's project YAML export.

## Concept Mapping

| Hex Concept      | DataHub Entity | Notes                                              |
| ---------------- | -------------- | -------------------------------------------------- |
| Workspace        | Container      | Top-level grouping for all projects                |
| Project          | Dashboard      | Primary unit — notebook or published app           |
| Component        | Dashboard      | Reusable shared notebook                           |
| Data Connection  | (internal)     | Resolved to DataHub platform URNs for lineage      |
| SQL Cell         | (lineage edge) | Upstream tables resolved via SQL parsing           |
| Run              | Operation      | Most recent scheduled run recorded as an operation |
| Context Document | Document       | AI-readable summary of the project's cells and SQL |
