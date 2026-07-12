## Overview

[ThoughtSpot](https://www.thoughtspot.com/) is a search- and AI-driven analytics platform used to author Liveboards (interactive dashboards), Answers (ad-hoc queries), and Worksheets (semantic models) over an organization's data warehouse.

The DataHub integration for ThoughtSpot ingests Liveboards as dashboards, Answers and Visualizations as charts, and Worksheets and Tables as datasets. When the ingestion principal can enumerate Orgs, those entities sit under Workspace containers; otherwise the catalog is flat.

Lineage is captured at both the table and column level: from Visualizations and Answers back to their Worksheets, and from federated Worksheets out to the underlying warehouse (Databricks, Snowflake, BigQuery, and similar). The connector also extracts ownership, ThoughtSpot tags, optional view counts, and uses stateful ingestion to soft-delete entities that disappear between runs.

## Concept Mapping

| ThoughtSpot Concept     | DataHub Concept                                 | Notes                                                                                                                                                             |
| ----------------------- | ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Workspace (Org)         | Container (`Workspace` subtype)                 | Parent of Liveboards, Answers, and Worksheets. Requires system-org admin to enumerate via `/orgs/search`.                                                         |
| Liveboard               | Dashboard (`Liveboard` subtype)                 | Formerly called "Pinboard"; the REST API still uses `pinboard` in some legacy field names.                                                                        |
| Answer                  | Chart (`Answer` subtype)                        | Standalone saved searches.                                                                                                                                        |
| Visualization           | Chart (`Visualization` subtype)                 | Charts/tiles embedded inside a Liveboard.                                                                                                                         |
| Worksheet               | Dataset (`Worksheet` subtype)                   | TS's semantic layer — joined logical model over connection tables.                                                                                                |
| Logical View / SQL View | Dataset (`View` subtype)                        | TS-side SQL definitions on top of a connection's schema.                                                                                                          |
| Logical Table           | Dataset (`Table` subtype)                       | Direct one-to-one mapping of a physical source table.                                                                                                             |
| Tag                     | GlobalTag                                       | Resolved to `urn:li:tag:<name>` with URL-encoded names.                                                                                                           |
| Owner / Author          | CorpUser                                        | DATAOWNER ownership; UUIDs in the `author.name` field are skipped (not real usernames).                                                                           |
| Column source reference | UpstreamLineage + FineGrainedLineage            | Worksheet→Table column-level lineage from TS's pre-resolved `columns[*].sources` field on `metadata/search`.                                                      |
| Federated connection    | UpstreamLineage (cross-platform)                | Worksheet → external `databricks` / `snowflake` / `bigquery` / etc. URN with table- and column-level edges; configured per-connection via `external_connections`. |
| View count              | DashboardUsageStatistics / ChartUsageStatistics | Cumulative lifetime counter from `metadata_search` `include_stats`. Emitted by default; set `include_usage_stats: false` to skip.                                 |
