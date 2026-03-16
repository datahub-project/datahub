### Overview

This connector extracts dashboards, charts, models, and collection membership from Metabase. It supports lineage from upstream database tables to charts and dashboards, including column-level lineage for models built with the visual query builder (MBQL).

#### Entities Extracted

| Metabase Entity | DataHub Entity | Notes                        |
| --------------- | -------------- | ---------------------------- |
| Dashboard       | Dashboard      |                              |
| Card / Question | Chart          |                              |
| Model           | Dataset        | SubTypes `["Model", "View"]` |
| Collection      | Tag            | Optional; see config         |

#### Lineage

The connector extracts lineage across all Metabase query types:

##### Native SQL

SQL queries are parsed with DataHub's SQLGlot-based parser to extract table references, including those inside `JOIN` clauses and subqueries. Metabase template variables (`{{variable}}`, `[[WHERE ...]]`) are stripped before parsing.

##### Query Builder (MBQL)

Questions and models built with Metabase's visual query builder store their logic as MBQL — a structured JSON representation. The connector resolves MBQL to upstream database tables and, for **models**, also produces column-level lineage:

- **Table-level**: The `source-table` field and all `joins[].source-table` entries are resolved to DataHub dataset URNs, so lineage covers multi-table join scenarios.
- **Column-level** (models only): `result_metadata[].field_ref` records which MBQL expression produced each output column. The connector resolves these refs to upstream field URNs via `/api/field/{id}`:
  - `["field", id, ...]` — direct pass-through column
  - `["expression", name]` — calculated column; traces back through `query.expressions`
  - `["aggregation", index]` — metric column; traces back through `query.aggregation`. `COUNT(*)` with no explicit field fans-in all resolved upstream columns (matching Tableau lineage behaviour).

##### Nested Queries

Charts or models that reference other cards (`source-table: "card__456"`) are recursively resolved to their ultimate source tables (max depth: 5, to guard against circular references).

##### Dashboard Lineage

The connector rolls up table dependencies from all charts in a dashboard into direct table-to-dashboard lineage edges, deduplicating tables referenced by multiple charts.

#### Collection Tags

Metabase Collections are mapped to DataHub tags:

- Tag format: `metabase_collection_{sanitized_name}` (e.g. "Sales & Marketing" → `metabase_collection_sales_marketing`)
- Tags are applied to dashboards, charts, and models within that collection
- Disable with `extract_collections_as_tags: false`

### Prerequisites

To use this connector, you'll need:

- Metabase version v0.41+ (Models require v0.41+)
- Authentication credentials (either username/password or API key — **API key is recommended**)
- Appropriate permissions to access the Metabase API

#### Authentication

DataHub supports two authentication methods:

1. **API Key** (Recommended) — more secure, no password management required. Generate one under Account Settings → API Keys in your Metabase instance.
2. **Username/Password**
