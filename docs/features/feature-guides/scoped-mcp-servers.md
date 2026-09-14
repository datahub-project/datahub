---
description: "Create scoped custom MCP servers on DataHub Cloud with their own tools, instructions, views, and connection URLs."
---

import FeatureAvailability from '@site/src/components/FeatureAvailability';

# Scoped MCP Servers

<FeatureAvailability saasOnly />

:::info Private Beta
Scoped MCP servers are available only to **Context Platform** private beta customers on DataHub Cloud. [Apply for private beta access](https://datahub.com/private-beta-request/).
:::

## Overview

The default DataHub MCP endpoint exposes your full managed tool surface. **Scoped MCP servers** let platform admins create additional custom servers — each with its own dedicated URL — tailored for a use-case-specific AI agent or chat assistant.

For each custom MCP server you can:

- **Show & Hide Tools** — Determine which tools and operations are exposed to agents built on the MCP server.
- **Provide Custom Server Instructions** — Customize base instructions leveraged by agents using the MCP server.
- **Expose Specific Data Assets & Context Documents** — Provide an optional DataHub Search View that restricts search and lookups to specific data assets and context documents to separate signal from noise.

Clients connect the same way as the default server (OAuth or personal access token), but point at the scoped URL instead. See the [MCP Server guide](./mcp.md) for client setup and authentication.

## Prerequisites

- DataHub Cloud with Context Platform private beta access
- **Manage platform settings** privilege (platform admin)

## Create a Scoped MCP Server

:::info Admins only
Only users with permission to manage platform settings can create or edit MCP servers.
:::

1. Go to **Settings → AI → MCP Servers**.
2. Confirm **Enable MCP Servers** is on (master switch at the top of the page).
3. Click **Create**.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/features/feature-guides/mcp/mcp-servers-management.png" alt="MCP Servers management page in Settings → AI"/>
</p>

_Screenshot: MCP Servers management page (default + scoped servers, master switch, and Create)._

4. Fill in:
   - **Name** — Display name (for example, `Finance MCP Server`)
   - **Slug** — URL-safe id used in the path (for example, `finance`). Cannot be changed after creation.
   - **Description** (optional)
   - **Base instructions** / **Custom instructions** — How the agent should behave for this scope
   - **Tools** — Select tools to expose; leave empty to expose all tools
   - **Scope to View** (optional) — Restrict search/lookups to a saved View
5. Copy the **Connection URL** shown in the form (for example, `https://<tenant>.acryl.io/mcp/finance`).
6. Save.

Point MCP clients at that URL using the same auth patterns as the [default managed MCP server](./mcp.md).

## Edit an MCP Server

1. Go to **Settings → AI → MCP Servers**.
2. Open the server (name link) or use the row menu → **Edit**.
3. Update name, description, instructions, tools, or View as needed. The **slug** (and thus the connection URL path) stays fixed.
4. Save.

You can also:

- **Enable or disable** a server from the table without deleting it
- **Edit the Default** server (no slug) to change tools, instructions, or View for the shared `/mcp` endpoint
- **Delete** a scoped server — its connection URL stops working immediately

## Next steps

- Configure your client connection and auth: [DataHub MCP Server](./mcp.md)
