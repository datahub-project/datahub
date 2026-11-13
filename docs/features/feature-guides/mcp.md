---
description: "The DataHub MCP Server lets AI agents query DataHub metadata via the Model Context Protocol to find assets and traverse lineage."
---

# DataHub MCP Server

The DataHub MCP Server implements the [Model Context Protocol (MCP)](https://modelcontextprotocol.io/introduction), giving AI agents direct access to your DataHub metadata. Search for data assets, traverse lineage, inspect schemas, and generate SQL — all through natural language in tools like Claude, ChatGPT, Cursor, and Windsurf.

Want to learn more about the motivation, architecture, and advanced use cases? Check out our [deep dive blog post](https://datahub.com/blog/datahub-mcp-server-block-ai-agents-use-case/).

## Deployment Options

- [Plugins for Claude, ChatGPT, and Codex](../../dev-guides/agent-context/plugins.md) - One-click setup, available on DataHub Cloud v1.0.2+
- [Managed MCP Server](#managed-mcp-server-usage) - Available on DataHub Cloud v0.3.12+
- [Self-Hosted MCP Server](#self-hosted-mcp-server-usage) - Built into DataHub Core v1.9.0+ at `/mcp`

## Capabilities

**Search for Data** <br />
Find the right data by asking questions in plain English. Supports wildcard matching (`revenue_*`), field searches (`tag:PII`), and boolean logic (`(sales OR revenue) AND quarterly`).

**Dive Deeper** <br />
Get usage stats, ownership, documentation, tags, glossary terms, and quality signals for any table, column, dashboard, & more — so agents can separate signal from noise.

**Lineage & Impact Analysis** <br />
Trace data flow at table and column level, upstream or downstream, across multiple hops. Understand the origins of your data, and plan for upcoming changes.

**Query Analysis & Authoring** <br />
Surface real SQL queries that reference a dataset — see join patterns, common filters, and aggregation behavior — then generate new queries grounded in actual usage.

**Works Where You Work** <br />
Works with Claude, ChatGPT, Cursor, Windsurf, and any other MCP-compatible client. Claude and ChatGPT also have one-click plugins.

**What You Can Ask** <br />
Once connected, try questions like:

- _"What datasets contain customer PII in production?"_
- _"Show me the lineage for analytics.revenue_table"_
- _"Who owns the 'Revenue Dashboard' in Looker?"_
- _"Find all datasets in the marketing domain"_
- _"What's the schema for user_events table?"_
- _"List datasets tagged as 'critical' or 'sensitive'"_

With **DataHub Cloud**, you can also create [scoped custom MCP servers](./scoped-mcp-servers.md) — named endpoints with their own tools, instructions, and views for domain-specific agents (Context Platform private beta).

## Tools

The DataHub MCP Server provides the following tools, grouped by whether they read from or write to DataHub. All tools are annotated with MCP-standard hints (`readOnlyHint`, `destructiveHint`, `idempotentHint`) so compatible clients (e.g. Claude) can surface which tools modify catalog state and prompt for confirmation accordingly.

### Read-Only Tools

These tools only query DataHub and never modify catalog state.

**Discovery & Inspection** — Find entities (datasets, dashboards, users, etc.), pull their full metadata, drill into schemas, trace lineage upstream or downstream at the table and column level, and search across saved documents and glossary term history.

:::info
Document tools (`search_documents`, `grep_documents`) are automatically hidden if no documents exist in the catalog.
:::

<details>
  <summary>Tools</summary>

`search` — Search DataHub using structured keyword search (`/q` syntax) with boolean logic, filters, pagination, and optional sorting by usage metrics.

`get_entities` — Fetch detailed metadata for one or more entities by URN; supports batch retrieval for efficient inspection of search results.

`list_schema_fields` — List schema fields for a dataset with keyword filtering and pagination, useful when search results truncate fields or when exploring large schemas.

`get_me` — Retrieve information about the currently authenticated user, including profile details and group memberships.

`get_lineage` — Retrieve upstream or downstream lineage for any entity (datasets, columns, dashboards, etc.) with filtering, query-within-lineage, pagination, and hop control.

`get_lineage_paths_between` — Retrieve the exact lineage paths between two assets or columns, including intermediate transformations and SQL query information.

`search_documents` — Search for documents using keyword search with filters for platforms, domains, tags, glossary terms, and owners.

`grep_documents` — Search within document content using regex patterns. Useful for finding specific information across multiple documents.

`list_lifecycle_stages` — List the lifecycle stages (e.g. proposed, approved, deprecated) configured for glossary terms.

`get_glossary_term_versions` / `compare_glossary_term_versions` — Inspect version history for a glossary term, or diff two versions to see what changed.

</details>

**SQL & Queries** — Surface real queries that hit a dataset and use that context to draft new SQL grounded in actual usage, joins, and filters.

<details>
  <summary>Tools</summary>

`get_dataset_queries` — Fetch real SQL queries referencing a dataset or column—manual or system-generated—to understand usage patterns, joins, filters, and aggregation behavior.

`find_sql_context` — Locate relevant tables, columns, and example queries for drafting SQL grounded in real catalog usage.

`draft_sql_for_tables` — Draft a SQL query against a specified set of tables using context retrieved from DataHub (schemas, sample queries, lineage).

</details>

**Governance** — Review pending metadata change proposals awaiting approval.

<details>
  <summary>Tools</summary>

`list_pending_proposals` — List metadata change proposals that are pending review.

</details>

### Mutation Tools

:::info
Mutation tools are available on DataHub Cloud v0.3.17+ and on DataHub Core v1.9.0+, where they are [on by default](#self-hosted-write-tools). `TOOLS_IS_MUTATION_ENABLED=false` turns them off. Each tool is annotated with `readOnlyHint: false` so MCP clients can require confirmation before invoking them.
:::

**Metadata Editing** — Apply tags, glossary terms, ownership, domains, descriptions, and structured properties to entities or individual columns. Also handles entity/term lifecycle stage transitions and document authoring.

<details>
  <summary>Tools</summary>

`create_tag` — Create a tag, or return the existing tag with that name (`created: false`).

`add_tags` / `remove_tags` — Add or remove tags from entities or schema fields (columns). Supports bulk operations on multiple entities.

`add_terms` / `remove_terms` — Add or remove glossary terms from entities or schema fields. Useful for applying business definitions and data classification.

`add_owners` / `remove_owners` — Add or remove ownership assignments from entities. Supports different ownership types (technical owner, data owner, etc.).

`create_domain` — Create a domain, or return the existing domain with that name (`created: false`).

`set_domains` / `remove_domains` — Assign or remove domain membership for entities. Each entity can belong to one domain.

`update_description` — Update, append to, or remove descriptions for entities or schema fields. Supports markdown formatting.

`add_structured_properties` / `remove_structured_properties` — Manage structured properties (typed metadata fields) on entities. Supports string, number, URN, date, and rich text value types.

`set_lifecycle_stage` — Set the lifecycle stage (e.g. proposed, approved, deprecated) of an entity or glossary term directly.

`save_document` — Save standalone documents (insights, decisions, FAQs, notes) to DataHub's knowledge base. Documents are organized under a configurable parent folder.

</details>

**Glossary Authoring** — Create new glossary terms, version them over time, and link related terms together.

<details>
  <summary>Tools</summary>

`create_glossary_term` — Create a new glossary term directly.

`create_glossary_term_version` — Create a new version of an existing glossary term to capture changes over time.

`add_related_terms` — Link related glossary terms (e.g. synonyms, contains, inherits-from) to express relationships in the business vocabulary.

</details>

**Proposals (governed workflows)** — Submit changes for review rather than applying them directly, and accept or reject pending proposals. Useful when an agent should suggest, not commit, metadata changes.

<details>
  <summary>Tools</summary>

`propose_create_glossary_term` — Submit a proposal to create a new glossary term, pending approval.

`propose_lifecycle_stage` — Submit a proposal to change an entity's lifecycle stage, pending approval.

`accept_or_reject_proposals` — Accept or reject pending metadata change proposals.

</details>

# Connecting to Managed MCP Server with OAuth - Recommended {#managed-mcp-server-usage}

_Available in DataHub Cloud v1.0.2+_

DataHub Cloud supports **OAuth2 with [Dynamic Client Registration (DCR)](https://datatracker.ietf.org/doc/html/rfc7591)** for MCP, so each DataHub user can connect with their own personal login — including SSO through providers like Okta, Azure AD, and others configured for your tenant. Compatible MCP clients (Claude, Claude Code, Cursor, ChatGPT, Snowflake, Databricks, etc.) discover the auth server, register themselves, and walk you through a browser-based login. Tokens are scoped to the signed-in user and refresh automatically.

### Single Entry Point

Point your MCP client at the universal endpoint:

```
https://mcp.datahub.com/mcp
```

On first connection, the page prompts you for your **DataHub domain** (e.g. `<tenant>` for `https://<tenant>.acryl.io`). Enter it once and the OAuth flow redirects you to your tenant's login, then back to the client — fully authenticated, no token copy-paste required.

<p align="center">
  <img width="70%" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/features/feature-guides/mcp/mcp-tenant-picker.png" alt="The mcp.datahub.com tenant picker prompting for your DataHub domain"/>
</p>

<p align="center">
  <video width="70%" controls>
    <source src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/features/feature-guides/mcp/mcp-demo.mp4" type="video/mp4" />
  </video>
</p>

<details>
  <summary>Use your direct tenant URL instead</summary>

If you'd rather skip the domain prompt at `mcp.datahub.com`, you can point clients directly at your tenant:

```
https://<tenant>.acryl.io/mcp
```

This endpoint also supports OAuth2 + DCR. The only difference is that `mcp.datahub.com/mcp` is a single shared URL you can hand out without knowing the tenant ahead of time — handy for marketplace listings or shared docs.

For on-premises DataHub Cloud, use your DataHub FQDN, e.g. `https://datahub.example.com/mcp`.

</details>

### Configure Your Client

:::note Using a scoped MCP server?
Substitute the scoped server's **Connection URL** wherever these steps use `https://mcp.datahub.com/mcp` — that shared endpoint always resolves to your tenant's **default** server. See [Connect a client to a scoped server](./scoped-mcp-servers.md#connect-a-client-to-a-scoped-server).
:::

<details>
  <summary>Claude (web and desktop)</summary>

The quickest option is the [DataHub Cloud plugin](../../dev-guides/agent-context/plugins.md#claude). To connect a scoped server instead, add a custom connector:

1. Open **Customize → Connectors**. On Team/Enterprise, an owner adds it under **Organization settings → Connectors** and members connect individually.
2. Click **+**, then **Add custom connector**.
3. Name: `DataHub`. Remote MCP server URL: `https://mcp.datahub.com/mcp`. Leave **Advanced settings** empty — DataHub registers the client via DCR.
4. Click **Add**, then **Connect**.
5. Enter your DataHub domain when prompted (e.g. `<tenant>`), sign in, and approve.

:::note
Remote MCP connectors are configured via the Claude UI, not `claude_desktop_config.json` — that file is reserved for local stdio servers. For older Claude Desktop versions without remote MCP support, fall back to the [`mcp-remote` bridge with a PAT](#managed-mcp-server-usage).
:::

</details>

<details>
  <summary>Claude Code</summary>

Claude Code supports OAuth-based remote MCP servers natively, including Dynamic Client Registration:

```bash
claude mcp add --transport http datahub https://mcp.datahub.com/mcp
```

The first time you invoke a DataHub tool, the server responds `401 Unauthorized`. Claude Code flags the server as needing authentication — run `/mcp` inside Claude Code and select **Authenticate** to complete the browser-based OAuth flow. Enter your DataHub domain when prompted; tokens are stored securely and refreshed automatically.

To use your tenant URL directly:

```bash
claude mcp add --transport http datahub https://<tenant>.acryl.io/mcp
```

</details>

<details>
  <summary>Cursor</summary>

Cursor supports remote MCP servers via the `url` field and handles OAuth flows automatically.

1. Open **Cursor → Settings → Cursor Settings → Tools & MCP → New MCP Server** (or edit `~/.cursor/mcp.json` for global config / `.cursor/mcp.json` for project-scoped config).
2. Paste:

   ```json
   {
     "mcpServers": {
       "datahub": {
         "url": "https://mcp.datahub.com/mcp"
       }
     }
   }
   ```

3. Save. Cursor triggers the OAuth flow in your browser using the `cursor://anysphere.cursor-mcp/oauth/callback` callback — enter your DataHub domain and sign in.
4. The MCP settings page should show a green dot and list the DataHub tools.

</details>

<details>
  <summary>ChatGPT</summary>

The quickest option is the [DataHub Cloud plugin](../../dev-guides/agent-context/plugins.md#chatgpt-and-codex). To connect a scoped server instead, add a custom connector. Custom connectors require **Developer Mode** and are available on **Plus, Pro, Team, Enterprise, and Edu** plans (not Free).

1. Enable Developer Mode: profile picture → **Settings → Connectors → Advanced**, then toggle **Developer mode** on. (On Team/Enterprise, an admin must first allow it under **Workspace Settings → Permissions & Roles → Connected Data → Developer mode / Create custom MCP connectors**.)
2. Still under **Settings → Connectors**, click **Add custom connector** (or **Create**).
3. Name: `DataHub`. MCP server URL: `https://mcp.datahub.com/mcp`. Authentication: **OAuth**.
4. Save. ChatGPT walks you through the OAuth flow — enter your DataHub domain and sign in. Pick which DataHub tools to enable for the connector.

</details>

<details>
  <summary>Snowflake Cortex Agents / Snowflake CoWork</summary>

Snowflake exposes external MCP servers to Cortex Agents through an **API Integration** + **External MCP Server** object pair. Both are created via SQL by an `ACCOUNTADMIN`, then the resulting connector is granted to your agent users and added to an agent in Snowsight.

1. Create an API integration using DCR (run as `ACCOUNTADMIN`). `OAUTH_RESOURCE_URL` must be the origin only — no `/mcp` path — because that is the `resource` value DataHub advertises at `/.well-known/oauth-protected-resource`:

   ```sql
   CREATE API INTEGRATION datahub_mcp_api_integration
     API_PROVIDER = external_mcp
     API_ALLOWED_PREFIXES = ('https://mcp.datahub.com')
     API_USER_AUTHENTICATION = (
       TYPE = OAUTH_DYNAMIC_CLIENT,
       OAUTH_RESOURCE_URL = 'https://mcp.datahub.com'
     )
     ENABLED = TRUE;
   ```

2. Create the MCP server object. It is schema-level, so place it where your agent users can reach it:

   ```sql
   CREATE EXTERNAL MCP SERVER <db>.<schema>.datahub_mcp_server
     WITH DISPLAY_NAME = 'DataHub'
     URL = 'https://mcp.datahub.com/mcp'
     API_INTEGRATION = datahub_mcp_api_integration;
   ```

3. Grant `USAGE` to every role that will use the agent. Without this the connector silently never appears — Snowflake hides unauthorized objects rather than raising an error:

   ```sql
   GRANT USAGE ON EXTERNAL MCP SERVER <db>.<schema>.datahub_mcp_server TO ROLE <role>;
   GRANT USAGE ON INTEGRATION datahub_mcp_api_integration TO ROLE <role>;
   ```

4. In Snowsight, navigate to **AI & ML → Agents**, select your agent, choose **MCP Connectors**, and add the DataHub connector.
5. In Snowflake CoWork (formerly Snowflake Intelligence), open the sources panel, select **Connectors**, then **Connect** next to DataHub — Snowflake walks each user through the DataHub OAuth flow and reuses the credential on subsequent calls.

See the [Snowflake agent context guide](../../dev-guides/agent-context/snowflake.md) for end-to-end setup. To point at your tenant instead of the global endpoint, use `https://<tenant>.acryl.io` for both `API_ALLOWED_PREFIXES` and `OAUTH_RESOURCE_URL`, and `https://<tenant>.acryl.io/mcp` for the MCP server `URL`.

</details>

<details>
  <summary>Databricks (Agent Bricks / Genie / AI Playground)</summary>

Databricks registers external MCP servers as **Unity Catalog HTTP connections** behind a managed proxy. The connection then becomes available to Agent Bricks, Genie Code, and AI Playground at `https://<workspace>/api/2.0/mcp/external/<connection_name>`.

1. In your workspace, open **Catalog → External Data → Connections → Create connection** (requires `CREATE CONNECTION` on the metastore).
2. Connection type: **HTTP**. Name: `datahub`. URL: `https://<tenant>.acryl.io/mcp` (your tenant URL is required here — the global `https://mcp.datahub.com/mcp` endpoint is not yet supported by Databricks).
3. Check the **Is MCP connection** box.
4. For **Auth type**, select **Dynamic Client Registration** (DCR per RFC 7591) — Databricks registers a client with DataHub automatically and stores refresh tokens.
5. Save. The first time the agent uses a DataHub tool, each operator authorizes DataHub via the workspace's managed OAuth flow.

:::note
DCR requires the workspace's **Managed MCP Servers** preview to be enabled and the workspace to be in a Model Serving–supported region. The HTTP connection must use streamable HTTP transport (DataHub's `/mcp` endpoint does).
:::

See the [Databricks Agent Bricks](../../dev-guides/agent-context/databricks-agent-bricks.md) and [Databricks Genie](../../dev-guides/agent-context/databricks-genie-code.md) guides for end-to-end agent setup.

</details>

### When to Use PAT Auth Instead

OAuth + DCR is the recommended path for **interactive** clients where a human signs in. Stick with personal access tokens (described below) for:

- **Service accounts** and unattended agentic workflows (CI/CD, scheduled jobs)
- **DataHub Cloud < v1.0.2** or DataHub Core
- MCP clients that don't yet implement OAuth-based remote MCP

# Connecting to Managed MCP Server with Access Tokens

For DataHub Cloud v0.3.12+, you can connect directly to the hosted MCP server endpoint — no local installation required.

:::info
The managed MCP server endpoint is only available with DataHub Cloud v0.3.12+. DataHub Core serves its own [MCP endpoint](#self-hosted-mcp-server-usage).
:::

:::note Streamable HTTP Only
DataHub's managed MCP server uses the [streamable HTTP transport](https://modelcontextprotocol.io/docs/concepts/transports). Some older MCP clients (e.g. chatgpt.com) may only support the deprecated SSE transport — for those, use [mcp-remote](https://github.com/geelen/mcp-remote) to bridge the gap.
:::

### Prerequisites

- The URL of your DataHub Cloud instance, e.g. `https://<tenant>.acryl.io`
- A [personal access token](../../authentication/personal-access-tokens.md)

### Connecting & Authenticating

Your managed MCP server URL is:

```
https://<tenant>.acryl.io/mcp
```

Authenticate by passing your token as a Bearer token in the `Authorization` header:

```
Authorization: Bearer <token>
```

:::caution The `?token=` query parameter no longer works
Requests that include a `token` query parameter get a `400 Bad Request`. A token in the URL
shows up in proxy and CDN logs, in browser history, and in `Referer` headers. If your MCP
client can't set headers itself, use `mcp-remote` with `--header` (shown below) and it will
send the header for you.
:::

<details>
  <summary>On-Premises DataHub Cloud</summary>

For on-premises DataHub Cloud, replace `<tenant>.acryl.io` with your DataHub FQDN, e.g. `https://datahub.example.com/mcp`.

</details>

### Configure

<details>
  <summary>Claude Desktop</summary>

1. Open your `claude_desktop_config.json` file. You can find it by navigating to Claude Desktop -> Settings -> Developer -> Edit Config.
1. Update the file to include the following content. Be sure to replace `<tenant>` and `<token>` with your own values.

```json
{
  "mcpServers": {
    "datahub-cloud": {
      "command": "npx",
      "args": [
        "-y",
        "mcp-remote",
        "https://<tenant>.acryl.io/mcp",
        "--header",
        "Authorization: Bearer <token>"
      ]
    }
  }
}
```

</details>

<details>
  <summary>Claude Code</summary>

Claude Code natively supports streamable HTTP, so no proxy or additional dependencies are needed.

Run the following command, replacing `<tenant>` and `<token>` with your own values:

```bash
claude mcp add --transport http datahub-cloud \
  "https://<tenant>.acryl.io/mcp" \
  --header "Authorization: Bearer <token>"
```

For a detailed walkthrough, see the [Claude integration guide](../../dev-guides/agent-context/claude.md).

</details>

<details>
  <summary>Cursor</summary>

1. Make sure you're using Cursor v1.1 or newer.
2. Navigate to Cursor -> Settings -> Cursor Settings -> MCP -> add a new MCP server.
3. Enter the following into the file, replacing `<tenant>` and `<token>` with your own values:

```json
{
  "mcpServers": {
    "datahub-cloud": {
      "url": "https://<tenant>.acryl.io/mcp",
      "headers": {
        "Authorization": "Bearer <token>"
      }
    }
  }
}
```

4. Once you've saved the file, confirm that the MCP settings page shows a green dot and the DataHub tools listed.

For a detailed walkthrough, see the [Cursor integration guide](../../dev-guides/agent-context/cursor.md).

</details>

<details>
  <summary>Gemini CLI</summary>

```bash
gemini mcp add --transport http \
  --header "Authorization: Bearer <token>" \
  datahub-cloud \
  "https://<tenant>.acryl.io/mcp"
```

For a detailed walkthrough, see the [Gemini CLI integration guide](../../dev-guides/agent-context/gemini-cli.md).

</details>

<details>
  <summary>Other</summary>

Most AI tools support remote MCP servers. Provide the hosted MCP server URL:

```
https://<tenant>.acryl.io/mcp
```

with the header `Authorization: Bearer <token>`.

Make sure authentication mode is _not_ set to "OAuth" (if applicable).

For clients that don't yet support remote MCP servers, use `mcp-remote`:

- Command: `npx`
- Args: `-y mcp-remote https://<tenant>.acryl.io/mcp --header "Authorization: Bearer <token>"`

</details>

## Service Accounts for Agentic Workflows

For autonomous or agentic workflows — such as CI/CD pipelines, scheduled scripts, or AI agents that run without human intervention — we recommend using a [Service Account](service-accounts.md) rather than a personal access token.

**Setup:**

1. Create a service account in **Settings > Users & Groups > Service Accounts**
2. Generate an access token for the service account
3. Use that token when configuring the MCP server connection

**Scoping search with a Default View** _(DataHub Cloud v1.0.0+ / DataHub Core v1.6.0+)_**:**

Service accounts support a **Default View** that restricts which data assets the MCP server searches across. This is configured directly from the Service Accounts management screen (the "Default View" column). When set, all searches performed by the MCP server using that service account's token will be scoped to the selected view — useful for limiting an agent's visibility to a specific domain, platform, or team's assets.

:::tip
Combine a service account with a default view to create a tightly-scoped MCP connection — for example, a "Snowflake Production" view for an agent that only needs access to production Snowflake datasets.
:::

## Self-Hosted MCP Server Usage {#self-hosted-mcp-server-usage}

_Built into DataHub Core v1.9.0+_

DataHub Core serves MCP itself. The `datahub-ai-service` container runs the MCP tools, and the DataHub frontend serves them at `/mcp`, on the same host and port as the UI. There is nothing to install for each user: point an MCP client at

```
<your-datahub-url>/mcp
```

where `<your-datahub-url>` is the address you open DataHub at, for example `http://localhost:9002/mcp` on a quickstart or `https://datahub.example.com/mcp` behind your ingress.

`datahub-ai-service` works with GMS v1.7.0 and later. It reads the version of the GMS it is connected to and hides the tools that version is too old for, so an older GMS serves fewer tools instead of failing calls.

Still running the standalone `mcp-server-datahub`? See [Migrating from `mcp-server-datahub`](#migrating-from-mcp-server-datahub).

### Turn It On {#turn-it-on}

The MCP server is off until you turn it on with `MCP_SERVER_ENABLED=true` on GMS (`datahub-gms`). While it is off, `/mcp` returns 404 and the UI hides the MCP settings page and the Connect AI Tools home page module. The `datahub-ai-service` container still runs, serving only `/health`. After changing the flag, restart GMS; the service notices within a minute.

- **Quickstart:** `MCP_SERVER_ENABLED=true datahub docker quickstart`
- **Helm:** add `MCP_SERVER_ENABLED: "true"` to the GMS environment (`datahub-gms.extraEnvs`).
- **Other deployments:** set the environment variable on the GMS container.

A GMS older than v1.9.0 doesn't have the flag, so `datahub-ai-service` serves MCP whenever it is connected to one.

### Quickstart

`datahub docker quickstart` starts `datahub-ai-service` with the rest of DataHub. The service's own port (9005) is bound to the host's loopback interface only. Clients use the frontend's route:

```
http://localhost:9002/mcp
```

Quickstart runs with [Metadata Service Authentication](../../authentication/introducing-metadata-service-authentication.md) off, so no token is needed. For example, with Claude Code:

```bash
claude mcp add --transport http datahub http://localhost:9002/mcp
```

See [Connect a Client](#connect-a-client) for other clients.

:::caution
While GMS authentication is off, every MCP call runs as DataHub's system user, and the write tools are on. Anyone who can reach port 9002 can read and change your metadata through `/mcp`. Keep an auth-off quickstart on your own machine, and turn on authentication before you expose it. See [Move To Production](../../quickstart.md#mcp-server).
:::

### Kubernetes (Helm)

The [DataHub Helm chart](https://github.com/acryldata/datahub-helm) deploys `datahub-ai-service` as a subchart. It is on by default (`global.datahub.ai.enabled`), and the chart points the frontend at it. Clients connect to your DataHub URL:

```
https://<your-datahub-host>/mcp
```

The chart turns on Metadata Service Authentication by default, so each client sends its own [personal access token](../../authentication/personal-access-tokens.md) on every request:

```
Authorization: Bearer <your-access-token>
```

A tool call can stream its response for several minutes. Load balancers and ingress controllers often close a response that stays open, or quiet, for 60 seconds, so raise their timeouts for `/mcp`. The [chart's documentation](https://github.com/acryldata/datahub-helm) describes the settings for ingress controllers, AWS ALB, GKE and Gateway API `HTTPRoute`. To turn MCP off, set `global.datahub.ai.enabled: false`.

<details>
  <summary>Other deployments</summary>

Run the `acryldata/datahub-ai-service` image next to GMS, using the same version tag as your other DataHub images:

1. On the service, set `DATAHUB_GMS_URL` to GMS (for example `http://datahub-gms:8080`) and `DATAHUB_AI_TRUSTED_PROXIES` to the frontend's host name or address, so the service believes the host the frontend forwards.
2. On the frontend, set `DATAHUB_AI_SERVICE_URL` to the service (for example `http://datahub-ai-service:9005`). While it is unset, `/mcp` returns 404.

The service answers `GET /health` without authentication, for load balancer and Kubernetes probes.

</details>

### Connect a Client {#connect-a-client}

Users with the **Manage Platform Settings** privilege find ready-to-copy setup for each client under **Settings > Platform > AI > MCP Server**, in **Connect a client**, filled in with your URL and authentication. The same setups are below.

Replace `<your-datahub-url>` with the address you open DataHub at. When GMS authentication is on, replace `<your-access-token>` with a [personal access token](../../authentication/personal-access-tokens.md). When it is off, leave the `Authorization` header out.

<details>
  <summary>Claude Code</summary>

```bash
claude mcp add --transport http datahub <your-datahub-url>/mcp \
  --header "Authorization: Bearer <your-access-token>"
```

Run `claude mcp list` to check the connection. See also the [Claude integration guide](../../dev-guides/agent-context/claude.md).

</details>

<details>
  <summary>Claude and Claude Desktop (custom connector)</summary>

The simplest option for claude.ai and Claude Desktop, with nothing to install, when DataHub has a public HTTPS URL (see the note below). In Claude, open **Customize > Connectors**, select **Add**, then **Add custom connector**:

1. Enter a name, such as `DataHub`, and the remote MCP server URL, `<your-datahub-url>/mcp`. Click **Continue**.
2. For **Authentication**, choose **No sign-in**.
3. When GMS authentication is on, open **Request headers**, choose `authorization`, and enter `Bearer <your-access-token>`. Claude sends the value exactly as entered, so keep the `Bearer ` prefix.
4. Click **Add**.

On Team and Enterprise plans, an Owner adds the connector for everyone under **Organization settings > Connectors**, and its request header is shared: every member calls DataHub as the token's user. Use a token whose permissions suit everyone, such as a [service account's](#service-accounts-for-agentic-workflows), or have each person connect with their own token through Claude Code or Claude Desktop with `mcp-remote`. Request headers are in beta and not every Claude organization has them yet; if the dialog doesn't show **Request headers**, use the `mcp-remote` setup below. See Anthropic's [Add a connector that isn't in the directory](https://claude.com/docs/connectors/custom/add-unlisted).

</details>

<details>
  <summary>Cursor</summary>

Add to `~/.cursor/mcp.json` (or `.cursor/mcp.json` in a project):

```json
{
  "mcpServers": {
    "datahub": {
      "url": "<your-datahub-url>/mcp",
      "headers": {
        "Authorization": "Bearer <your-access-token>"
      }
    }
  }
}
```

The MCP settings page should show a green dot and list the DataHub tools. See also the [Cursor integration guide](../../dev-guides/agent-context/cursor.md).

</details>

<details>
  <summary>Claude Desktop (local, with mcp-remote)</summary>

Use this when Anthropic's cloud can't reach DataHub (a private URL, or plain `http://`), or when your Claude organization doesn't offer connector request headers yet. The [`mcp-remote`](https://github.com/geelen/mcp-remote) bridge runs on your machine and needs Node.js. Open **Settings > Developer > Edit Config** and add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "datahub": {
      "command": "npx",
      "args": [
        "-y",
        "mcp-remote",
        "<your-datahub-url>/mcp",
        "--header",
        "Authorization:${AUTH_HEADER}"
      ],
      "env": {
        "AUTH_HEADER": "Bearer <your-access-token>"
      }
    }
  }
}
```

`mcp-remote` expands `${AUTH_HEADER}` itself; passing the header through an environment variable avoids Claude Desktop splitting it on the space. Without a token, keep only `"args": ["-y", "mcp-remote", "<your-datahub-url>/mcp"]`. `mcp-remote` refuses a plain `http://` URL for any host but `localhost` unless you add `--allow-http`; use that only on a network you trust.

</details>

<details>
  <summary>VS Code</summary>

Add to `.vscode/mcp.json`:

```json
{
  "servers": {
    "datahub": {
      "type": "http",
      "url": "<your-datahub-url>/mcp",
      "headers": {
        "Authorization": "Bearer <your-access-token>"
      }
    }
  }
}
```

</details>

<details>
  <summary>Gemini CLI</summary>

```bash
gemini mcp add --transport http \
  --header "Authorization: Bearer <your-access-token>" \
  datahub \
  "<your-datahub-url>/mcp"
```

See also the [Gemini CLI integration guide](../../dev-guides/agent-context/gemini-cli.md).

</details>

<details>
  <summary>Other</summary>

Any client that supports the [Streamable HTTP transport](https://modelcontextprotocol.io/docs/concepts/transports):

- URL: `<your-datahub-url>/mcp`
- Transport: Streamable HTTP
- Header: `Authorization: Bearer <your-access-token>` (when GMS authentication is on)

Set the client's authentication mode to a header or bearer token, not OAuth. For a client that only starts local (stdio) servers, use `mcp-remote` as in the Claude Desktop example, or the [stdio server](#run-locally-over-stdio).

</details>

:::note claude.ai and the Claude API need a public HTTPS URL
Custom connectors in claude.ai and Claude Desktop, and the Claude API's MCP connector, call your server from Anthropic's cloud, not from your computer. They can reach only a public HTTPS URL: never `localhost`, a VPN, or a private network. For a private DataHub, use a client that connects from your machine (Claude Code, Cursor, VS Code, or Claude Desktop with `mcp-remote`). If you do publish `/mcp`, turn on GMS authentication first, so every request needs an access token.
:::

### Authentication {#self-hosted-authentication}

`datahub-ai-service` follows GMS's authentication mode (`DATAHUB_AI_AUTH_MODE=auto`, the default):

| GMS authentication (`METADATA_SERVICE_AUTH_ENABLED`) | What `/mcp` needs                                                                  | Calls run as                               |
| ---------------------------------------------------- | ---------------------------------------------------------------------------------- | ------------------------------------------ |
| On                                                   | `Authorization: Bearer <token>` on every request; without a valid token, a 401     | The token's user, with that user's rights  |
| Off                                                  | Nothing                                                                            | DataHub's system user, with all privileges |
| Unknown, because GMS can't be reached                | `/mcp` answers 503 until GMS answers; an unchecked token is never treated as valid | -                                          |

- **Each client uses its own token.** Unlike the standalone `mcp-server-datahub`, the service never holds a credential of its own: it refuses to start when `DATAHUB_GMS_TOKEN` is set, and each HTTP client sends its own personal access token. For an unattended agent, use a [service account's](#service-accounts-for-agentic-workflows) token. The [stdio server](#run-locally-over-stdio) still accepts `DATAHUB_GMS_TOKEN`, because it serves one user.
- **Tokens go in the header.** A request with a `token` query parameter gets a 400: a token in a URL ends up in proxy and access logs.
- **Revocation takes up to 5 minutes.** A verified token is trusted for `DATAHUB_MCP_TOKEN_CACHE_TTL_SECONDS` (300 by default) before GMS checks it again.
- **Require authentication.** Set `DATAHUB_AI_AUTH_MODE=required` on the service to make `/mcp` unavailable (503) whenever GMS authentication is off or can't be determined, instead of serving it token-free.
- **Host checks while authentication is off.** `/mcp` then answers only requests addressed to an allowed host (`DATAHUB_AI_ALLOWED_HOSTS`, `localhost`, `127.0.0.1` and `[::1]` by default) and, when they carry an `Origin`, from an allowed origin (`DATAHUB_AI_ALLOWED_ORIGINS`); anything else gets a 403. This stops a web page from reaching `/mcp` through DNS rebinding. It is not authentication: any program that can reach the port can send those headers.

### Manage MCP from the Settings Page {#mcp-settings-page}

Users with the **Manage Platform Settings** privilege manage MCP under **Settings > Platform > AI > MCP Server**:

- **Enable MCP Server** turns the server at `/mcp` on or off for all users. While it is off, `/mcp` returns 404.
- **Edit** the server to choose its tools, by tool or by group (**Read metadata**, **Write metadata**; with none selected, every tool is served), and its instructions: base instructions (**None**, **Small** or **Large**) followed by your own custom instructions.
- **Connect a client** shows the setup for each MCP client, with your URL and authentication.

How the service applies these settings:

- Changes apply to new MCP requests within 60 seconds. Clients cache the tool list and instructions, so a client may need a refresh or reconnect to show them.
- The tool selection filters both the tool list and direct calls: a tool that isn't selected can't be called by name either.
- If the service can't read the settings, `/mcp` fails closed with a 503 rather than serving tools the settings might have turned off.
- `TOOLS_IS_MUTATION_ENABLED=false` on the service always wins: the settings page can't turn write tools back on.
- The tool picker offers the tools the service serves and the connected GMS can run, from `/ai/catalog/tools` on the frontend (signed-in users only).
- `GET /health` on the service reports the settings state (`enabled`, `disabled`, `unavailable`, or `unknown` before the first request), never the settings themselves.
- Against a GMS that has no MCP settings API (one older than the service), the defaults apply: MCP on, every tool, default instructions.

### Write Tools {#self-hosted-write-tools}

Write tools (the [mutation tools](#mutation-tools) above) are **on by default**. Each one runs with the caller's own privileges, so it can change only what that user could change in the UI; while GMS authentication is off, that is the system user, who can change anything. To turn them off:

- On the settings page, edit the server and deselect its **Write metadata** tools, or
- Set `TOOLS_IS_MUTATION_ENABLED=false` on `datahub-ai-service`. This removes every write tool, whatever the settings page says.

Writes behave predictably when retried:

- `create_tag` and `create_domain` are idempotent by name. Calling them for a name that already exists returns the existing entity with `created: false` instead of making a second one, so an agent can create-then-apply without checking first.
- `create_tag` sets a color with a second write. If the tag is created but its color can't be set, the call still succeeds and reports the problem in `color_error`.
- A write is never retried automatically after an ambiguous response (for example a timeout after GMS received the request). The tool returns an error that says how to check whether the write was applied. The service retries a GMS request only when GMS never ran it: a refused connection or a 429.

### Tool Switches

These environment variables on `datahub-ai-service` (or the stdio server) choose which tools are registered, before any settings-page selection:

| Variable                         | Default  | Effect                                                                                          |
| -------------------------------- | -------- | ----------------------------------------------------------------------------------------------- |
| `TOOLS_IS_MUTATION_ENABLED`      | `true`   | `false` removes every write tool                                                                |
| `SAVE_DOCUMENT_TOOL_ENABLED`     | `true`   | `save_document` and `update_document`                                                           |
| `SAVE_DOCUMENT_RESTRICT_UPDATES` | `false`  | `true` lets `update_document` edit only documents in the shared folder                          |
| `SAVE_DOCUMENT_PARENT_TITLE`     | `Shared` | Title of the folder `save_document` creates documents in                                        |
| `SAVE_DOCUMENT_ORGANIZE_BY_USER` | `false`  | `true` puts each user's saved documents in their own sub-folder                                 |
| `LIST_SCHEMA_FIELDS_ENABLED`     | `true`   | `list_schema_fields`                                                                            |
| `GREP_ENTITIES_ENABLED`          | `true`   | `grep_entities`                                                                                 |
| `ENTITY_VERSIONING_ENABLED`      | `false`  | Glossary term version tools (`create_glossary_term_version`, `get_glossary_term_versions`, ...) |
| `SEMANTIC_SEARCH_ENABLED`        | `false`  | `true` adds semantic search to `search_documents`; GMS must have semantic search set up         |

### Streaming and JSON Responses

By default (`DATAHUB_AI_MCP_RESPONSE_MODE=sse`) each response streams as server-sent events, so progress reaches the client during a slow call and a keep-alive is sent every 15 seconds. Set `json` to return one plain JSON body instead, only behind a proxy that can't stream responses; give that proxy an idle timeout longer than the slowest tool call (a tool call may keep calling GMS for `DATAHUB_AI_TOOL_TIME_LIMIT_SECONDS`, 300 by default).

The frontend waits up to `DATAHUB_AI_PROXY_TIMEOUT_SECONDS` (600) for the service's response headers, closes a stream that sends nothing for `DATAHUB_AI_PROXY_STREAM_IDLE_TIMEOUT_SECONDS` (600), and serves at most `DATAHUB_AI_PROXY_MAX_IN_FLIGHT` (128) MCP calls at once, separately from the UI's requests. Over that limit, `/mcp` answers 503.

### Telemetry, Usage Events and Metrics

- **Anonymous telemetry.** Like the DataHub CLI, the service sends anonymous usage telemetry to DataHub's product analytics (Mixpanel): one event when it starts and one per tool call, with the tool's name, the MCP client's name and version, timings, the result's size, the error's class when a call fails, and the GMS version. Events never include tool arguments, results, error messages, or who called. Turn them off with `DATAHUB_TELEMETRY_ENABLED=false`. See [DataHub Telemetry](../../deploy/telemetry.md).
- **Usage events in DataHub.** Each tool call is recorded in DataHub's own usage analytics, attributed to the caller: the tool, its group (`read` or `write`), the MCP client, the duration, the result size and the error's class, never its arguments or results. Turn this off with `DATAHUB_AI_USAGE_EVENTS_ENABLED=false`.
- **Prometheus metrics.** The service serves metrics at `/metrics` on its own port (9005), without authentication, like `/health`. The frontend doesn't proxy it, so `/metrics` is reachable only where port 9005 is. Turn it off with `DATAHUB_AI_METRICS_ENABLED=false`.

<details>
  <summary>datahub-ai-service configuration reference</summary>

| Variable                              | Default                     | Meaning                                                                          |
| ------------------------------------- | --------------------------- | -------------------------------------------------------------------------------- |
| `DATAHUB_GMS_URL`                     | required                    | GMS base URL; may include a path prefix                                          |
| `DATAHUB_AI_PORT`                     | `9005`                      | Port to listen on                                                                |
| `DATAHUB_AI_MCP_ENABLED`              | `true`                      | Serve MCP at `/mcp`                                                              |
| `DATAHUB_AI_MCP_RESPONSE_MODE`        | `sse`                       | `sse` streams responses; `json` returns one body                                 |
| `DATAHUB_AI_AUTH_MODE`                | `auto`                      | `auto` follows GMS; `required` serves `/mcp` only while GMS authentication is on |
| `DATAHUB_AI_ALLOWED_HOSTS`            | `localhost,127.0.0.1,[::1]` | Hosts `/mcp` answers while GMS authentication is off (`host` or `host:port`)     |
| `DATAHUB_AI_ALLOWED_ORIGINS`          | `http(s)://` those hosts    | Origins `/mcp` accepts while GMS authentication is off                           |
| `DATAHUB_AI_TRUSTED_PROXIES`          | none                        | IPs, networks or host names whose `X-Forwarded-Host` is believed                 |
| `DATAHUB_MCP_TOKEN_CACHE_TTL_SECONDS` | `300`                       | How long a verified token is trusted before GMS checks it again                  |
| `DATAHUB_AI_GMS_TIMEOUT_SECONDS`      | `30`                        | Connect and read timeout for each GMS request                                    |
| `DATAHUB_AI_TOOL_TIME_LIMIT_SECONDS`  | `300`                       | How long one tool call may keep sending requests to GMS                          |
| `DATAHUB_AI_USAGE_EVENTS_ENABLED`     | `true`                      | Record each tool call as a DataHub usage event                                   |
| `DATAHUB_AI_METRICS_ENABLED`          | `true`                      | Serve Prometheus metrics at `/metrics`                                           |
| `DATAHUB_TELEMETRY_ENABLED`           | `true`                      | Send anonymous usage telemetry                                                   |

</details>

### Run Locally over stdio {#run-locally-over-stdio}

`datahub-mcp-stdio` serves the same tools to one local user over stdin/stdout, with no service to install. The MCP client starts it as a subprocess. It needs [`uv`](https://github.com/astral-sh/uv) (`curl -LsSf https://astral.sh/uv/install.sh | sh`):

```json
{
  "mcpServers": {
    "datahub": {
      "command": "uvx",
      "args": ["--from", "datahub-core-mcp-tools", "datahub-mcp-stdio"],
      "env": {
        "DATAHUB_GMS_URL": "<your-gms-url>",
        "DATAHUB_GMS_TOKEN": "<your-access-token>"
      }
    }
  }
}
```

Without `DATAHUB_GMS_URL` it uses the `~/.datahubenv` that `datahub init` writes. Leave out `DATAHUB_GMS_TOKEN` when GMS authentication is off. The [tool switches](#tool-switches) apply, tools the connected DataHub is too old for are hidden, and it sends the same anonymous telemetry (`DATAHUB_TELEMETRY_ENABLED=false` turns it off). It has no settings page and records no usage events.

### Migrating from `mcp-server-datahub` {#migrating-from-mcp-server-datahub}

The standalone [`mcp-server-datahub`](https://github.com/acryldata/mcp-server-datahub) is deprecated. Its tools now ship with DataHub. Pick one:

- **Keep a local (stdio) server.** Change one command: `uvx mcp-server-datahub@latest` becomes `uvx --from datahub-core-mcp-tools datahub-mcp-stdio`. The environment (`DATAHUB_GMS_URL`, `DATAHUB_GMS_TOKEN`) or `~/.datahubenv` authenticates it as before.
- **Move to the HTTP endpoint.** Remove the local server and point the client at `<your-datahub-url>/mcp`, as in [Connect a Client](#connect-a-client). Nothing runs on your machine, and admins control the tools from the settings page.

Write tools are on by default in both, where `mcp-server-datahub` had them off: set `TOOLS_IS_MUTATION_ENABLED=false` to keep the old behavior. Some tools also changed their inputs or outputs; see [Breaking Changes](../../how/breaking-changes.md).

<details>
  <summary>Claude Desktop</summary>

Before:

```json
{
  "mcpServers": {
    "datahub": {
      "command": "<full-path-to-uvx>",
      "args": ["mcp-server-datahub@latest"],
      "env": {
        "DATAHUB_GMS_URL": "<your-gms-url>",
        "DATAHUB_GMS_TOKEN": "<your-access-token>"
      }
    }
  }
}
```

After, over stdio:

```json
{
  "mcpServers": {
    "datahub": {
      "command": "<full-path-to-uvx>",
      "args": ["--from", "datahub-core-mcp-tools", "datahub-mcp-stdio"],
      "env": {
        "DATAHUB_GMS_URL": "<your-gms-url>",
        "DATAHUB_GMS_TOKEN": "<your-access-token>"
      }
    }
  }
}
```

Or over HTTP, with the [Claude Desktop setup](#connect-a-client) under Connect a Client.

</details>

<details>
  <summary>Claude Code</summary>

Before:

```bash
claude mcp add datahub \
  -e DATAHUB_GMS_URL="<your-gms-url>" \
  -e DATAHUB_GMS_TOKEN="<your-access-token>" \
  -- uvx mcp-server-datahub@latest
```

After, over stdio:

```bash
claude mcp remove datahub
claude mcp add datahub \
  -e DATAHUB_GMS_URL="<your-gms-url>" \
  -e DATAHUB_GMS_TOKEN="<your-access-token>" \
  -- uvx --from datahub-core-mcp-tools datahub-mcp-stdio
```

Or over HTTP:

```bash
claude mcp remove datahub
claude mcp add --transport http datahub <your-datahub-url>/mcp \
  --header "Authorization: Bearer <your-access-token>"
```

</details>

<details>
  <summary>Cursor</summary>

Before, in `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "datahub": {
      "command": "uvx",
      "args": ["mcp-server-datahub@latest"],
      "env": {
        "DATAHUB_GMS_URL": "<your-gms-url>",
        "DATAHUB_GMS_TOKEN": "<your-access-token>"
      }
    }
  }
}
```

After, over stdio:

```json
{
  "mcpServers": {
    "datahub": {
      "command": "uvx",
      "args": ["--from", "datahub-core-mcp-tools", "datahub-mcp-stdio"],
      "env": {
        "DATAHUB_GMS_URL": "<your-gms-url>",
        "DATAHUB_GMS_TOKEN": "<your-access-token>"
      }
    }
  }
}
```

Or over HTTP:

```json
{
  "mcpServers": {
    "datahub": {
      "url": "<your-datahub-url>/mcp",
      "headers": {
        "Authorization": "Bearer <your-access-token>"
      }
    }
  }
}
```

</details>

#### From a Shared HTTP Deployment {#shared-http-deployment}

A shared `mcp-server-datahub-http` deployment (`http://<host>:8000/mcp`) is replaced by DataHub's own `/mcp` route. It authenticates the same way, with each user's own token, so clients change only their URL from `http://<host>:8000/mcp` to `<your-datahub-url>/mcp`. Remove the old deployment once clients have moved.

### Troubleshooting

| Symptom                                  | Cause and fix                                                                                                                                                                                                                                              |
| ---------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `/mcp` returns 404                       | MCP is off on the settings page (the master switch or the Default row), `DATAHUB_AI_MCP_ENABLED` is `false`, or the frontend has no `DATAHUB_AI_SERVICE_URL`.                                                                                              |
| `/mcp` returns 503                       | `datahub-ai-service` is down or still starting, it can't reach GMS, it can't read the MCP settings, or `DATAHUB_AI_AUTH_MODE=required` while GMS authentication is off. Check the service's logs and `GET /health` on port 9005.                           |
| 401                                      | GMS authentication is on and the request has no token, or GMS rejected it. Send a valid personal access token in the `Authorization` header.                                                                                                               |
| 403 `forbidden_host`                     | GMS authentication is off and DataHub was reached at a host name other than `localhost`. Use `localhost`, or add the host to `DATAHUB_AI_ALLOWED_HOSTS` (and its origin to `DATAHUB_AI_ALLOWED_ORIGINS` for browser-based clients) on a network you trust. |
| 400 on every request                     | The token is in the URL (`?token=`). Send it in the `Authorization` header.                                                                                                                                                                                |
| A tool is missing                        | It was deselected on the settings page, turned off by a [tool switch](#tool-switches), or needs a newer GMS. After a settings change, refresh or reconnect the client.                                                                                     |
| Responses cut off after 60 seconds or so | A load balancer or ingress timeout. Raise it for `/mcp` (see [Kubernetes](#kubernetes-helm)).                                                                                                                                                              |
| `spawn uvx ENOENT` (stdio server)        | The client can't find `uvx`. Replace `uvx` in the command with the full path from `which uvx`.                                                                                                                                                             |
