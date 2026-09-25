### Overview

The `hex` module ingests Hex Projects, Components, workspaces, and upstream lineage directly from the Hex REST API.

### Prerequisites

#### Workspace Name

Open the **workspace switcher dropdown** in the top-left corner of the Hex app — the workspace name (and its slug) is shown next to each workspace entry. Use the slug value for `workspace_name`.

#### Authentication

The connector authenticates with a Hex **Workspace token** issued from `Settings → API → Workspace tokens`. Grant the token these **read-only** scopes:

- `Projects → Read access` — list projects/components and read their detail and run history.
- `Cells → Read access` — read SQL cells for lineage and context documents.
- `Read project queried tables` — lineage from Hex's pre-resolved table list. Available on **Hex Enterprise workspaces only**; skip this scope on lower Hex tiers — the connector falls back to SQL parsing.
- `Data connections → Read access` — map each Hex connection to its warehouse platform/database/schema.
- `Users → Read access` — _optional_, only needed to auto-discover the workspace (org) UUID used in external URLs. Skip this scope and set `workspace_id` in the recipe instead.

No write scopes are required — the connector never modifies state in Hex.

Personal Access Tokens (PATs) also work but ingest with the issuing user's permissions, so projects the user cannot see in Hex will be skipped. Workspace tokens are recommended for production ingestion. See the [Hex API overview](https://learn.hex.tech/docs/api-integrations/api/overview) for the full list of token types.

#### Lineage URN Alignment

Upstream URNs are built from Hex's `/v1/data-connections` response — platform, database, and schema all come from there. Configure `connection_platform_map` (keyed by Hex `dataConnectionId`) in two cases:

- the upstream warehouse was ingested under a `platform_instance` — set the matching `platform_instance` so the URNs collide with the warehouse-ingested ones,
- a Hex connection's type is unrecognized (deleted, custom, or the token lacks scope on `/v1/data-connections`) — set `platform` explicitly so its cells aren't skipped.

See **Connection Platform Resolution** in the sections below for the full configuration shape.
