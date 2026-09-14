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

## Connect a Client to a Scoped Server

A scoped server is just another MCP endpoint: the transport (streamable HTTP) and the auth options (OAuth2 + DCR, or a personal access token) are identical to the default server. The only thing that changes is the URL.

:::caution Use your tenant URL, not `mcp.datahub.com`
The shared entry point `https://mcp.datahub.com/mcp` always resolves to your tenant's **default** MCP server — there is currently no way to select a scoped server through it. To reach a scoped server, point the client at the **Connection URL** from **Settings → AI → MCP Servers** (for example, `https://<tenant>.acryl.io/mcp/finance`).
:::

Copy the Connection URL from the server's detail page rather than assembling it by hand, then follow the steps for your client below.

<details>
  <summary>Claude (web, desktop, mobile)</summary>

Scoped servers are added as a **custom connector**, exactly like the default server — only the URL differs.

1. In claude.ai or Claude Desktop, open **Settings → Connectors** (Team/Enterprise: **Organization settings → Connectors**).
2. Click **Add custom connector**.
3. **Name**: something that identifies the scope, e.g. `DataHub — Finance`. Using the scope in the name matters because you may end up with several DataHub connectors side by side.
4. **Remote MCP server URL**: paste the scoped **Connection URL**, e.g. `https://<tenant>.acryl.io/mcp/finance`.
5. Leave **Advanced settings** (OAuth Client ID / Secret) empty — DataHub registers the client automatically via DCR.
6. Click **Add**, then **Connect**, and complete the DataHub sign-in in the browser window Claude opens.

Because the URL already identifies the tenant, Claude will not prompt you for a DataHub domain the way `mcp.datahub.com` does.

</details>

<details>
  <summary>Claude Code</summary>

```bash
claude mcp add --transport http datahub-finance https://<tenant>.acryl.io/mcp/finance
```

The first tool call returns `401 Unauthorized`; run `/mcp`, select the server, and choose **Authenticate** to complete the OAuth flow. Give each scoped server a distinct name (`datahub-finance`, `datahub-sales`) so they can coexist in the same config.

</details>

<details>
  <summary>Cursor</summary>

Add the scoped URL as its own entry in `~/.cursor/mcp.json` (global) or `.cursor/mcp.json` (project-scoped):

```json
{
  "mcpServers": {
    "datahub-finance": {
      "url": "https://<tenant>.acryl.io/mcp/finance"
    }
  }
}
```

Save, then complete the OAuth flow Cursor opens in your browser.

</details>

<details>
  <summary>Personal access token (service accounts, unattended agents)</summary>

If the client cannot do OAuth, authenticate with a PAT the same way as the default server — swap in the scoped URL:

```json
{
  "mcpServers": {
    "datahub-finance": {
      "command": "npx",
      "args": [
        "-y",
        "mcp-remote",
        "https://<tenant>.acryl.io/mcp/finance",
        "--header",
        "Authorization: Bearer <token>"
      ]
    }
  }
}
```

See [Service Accounts for Agentic Workflows](./mcp.md#service-accounts-for-agentic-workflows) for how to provision the token.

</details>

## Choosing Between Scoping and Permissions

A scoped server shapes **what an agent sees by default** — it is a signal-to-noise tool, not an access control boundary. The attached View narrows search and lookups for that endpoint, but it does not revoke the user's underlying access: the same person can query outside the scope through the default MCP server, the UI, or the API.

If users of a scoped server must be genuinely prevented from reading data outside its scope, enforce that with [Policies](../../authorization/policies.md) on the users or groups involved, and treat the scoped server as an ergonomics layer on top.

## Keeping Agents Inside Their Scope

Restricting the View limits what an agent can retrieve, but it does not by itself stop the agent from answering from its own general knowledge when the catalog has nothing to offer. For use cases where a wrong answer is worse than no answer, state that explicitly in the server's **Custom instructions** — for example:

> You answer questions only from the finance context available through these tools. If the tools return no relevant assets, documents, or queries for a question, say that you do not have the context to answer it and suggest who to ask. Never answer from prior knowledge or infer values that the tools did not return.

Instructions are guidance to the model, not a hard guarantee. Validate the behavior against real questions — including ones you expect to fall outside the scope — and iterate on the wording before rolling the server out broadly.

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

- Client setup and auth for the default server: [DataHub MCP Server](./mcp.md)
- Restrict what users can read, not just what agents see: [Policies](../../authorization/policies.md)
