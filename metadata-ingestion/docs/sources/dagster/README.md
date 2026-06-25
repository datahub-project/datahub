## Overview

[Dagster](https://dagster.io/) is an orchestrator for the development, production, and observation of data assets. It exposes pipeline and asset metadata through a GraphQL API served by the Dagster webserver.

This connector is a **pull-based** ingestion source: it connects to a running Dagster instance's `/graphql` endpoint and extracts jobs, ops, and Software-Defined Assets as DataHub DataFlow, DataJob, and Dataset entities — including asset lineage, descriptions, ownership, tags, documentation links, and table schemas. It works against both Dagster OSS and Dagster+ (Cloud), and supports stateful deletion detection. It complements the push-based [Dagster plugin](../../../docs/lineage/dagster.md) and emits the same URN conventions, so the two can be used together without creating duplicate entities.

## Concept Mapping

| Source Concept                  | DataHub Concept                                                | Notes                                          |
| ------------------------------- | -------------------------------------------------------------- | ---------------------------------------------- |
| `Job`                           | [DataFlow](../../metamodel/entities/dataFlow.md)               | Platform is `dagster`                          |
| `Op`                            | [DataJob](../../metamodel/entities/dataJob.md)                 | Nested under its job's DataFlow                |
| `Software-Defined Asset`        | [Dataset](../../metamodel/entities/dataset.md)                 | `subTypes` is `Asset`                          |
| Asset dependency                | [Upstream Lineage](../../metamodel/entities/dataset.md)        | Derived from asset `dependencyKeys`            |
| Asset / Job owner               | [Ownership](../../metamodel/entities/dataset.md)               | User email → CorpUser, team → CorpGroup        |
| Asset / Job tag, kind, group    | [Tag](../../metamodel/entities/tag.md)                         | Group emitted as `asset_group:<name>`          |
