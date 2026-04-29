import FeatureAvailability from '@site/src/components/FeatureAvailability';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Analytics Agent

<FeatureAvailability />

An open-source agent that lets you ask data questions in plain English and get SQL, results, and charts back — grounded in your DataHub catalog. Apache 2.0, bring your own LLM.

<p align="center">
  <img width="85%" src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/analytics-agent/screenshot-chat.png" alt="Analytics Agent answering a data question with a chart"/>
</p>

## What you can do

| Capability                           | What it does                                                                                                                                                                |
| ------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Plain English → SQL → Chart**      | Ask _"Top 5 categories by revenue last quarter"_ — the agent searches DataHub for context, writes SQL, runs it, and auto-renders a chart. No SQL required.                  |
| **Follow up naturally**              | _"Make it a pie chart"_, _"filter to Q3"_, _"break that down by region"_ — the agent maintains full conversation context across turns.                                      |
| **See why the answer is what it is** | Every tool call and SQL step is visible and expandable. No black box.                                                                                                       |
| **Know when to trust it**            | A live context quality score (1–5) tells you how well your DataHub catalog supported each answer. Hover for the LLM's reasoning.                                            |
| **Improve your catalog from chat**   | Type `/improve-context` to get a numbered list of documentation improvements the agent wishes it had. Approve the ones you want, and the agent writes them back to DataHub. |

## Quickstart

The fastest way to try Analytics Agent. The script spins up a local DataHub instance, loads the [Olist e-commerce](https://github.com/datahub-project/static-assets/tree/main/datasets/olist-ecommerce) sample dataset, and launches the agent — so you can try it end-to-end without connecting your own data.

**You'll need:**

- **Docker** (running)
- **Python 3.11+**
- **DataHub CLI** — `pip install acryl-datahub`
- **`uv`** — `brew install uv` or [install from docs.astral.sh/uv](https://docs.astral.sh/uv/)
- **An LLM API key** from Anthropic, OpenAI, or Google

:::warning Operating system support
Analytics Agent is tested on **macOS** and **Linux**. **Windows users** should run setup through [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install) — the quickstart and `just` runner are bash-based.
:::

```bash
git clone https://github.com/datahub-project/analytics-agent.git
cd analytics-agent
bash quickstart.sh
```

:::info Expect 15–25 minutes on a fresh machine
Most of that time is Docker pulling DataHub images on first run. Subsequent runs take 3–6 minutes.
:::

When it finishes, open **http://localhost:8100**. A two-step setup wizard will:

1. Ask you to name your agent
2. Ask you to pick a provider, model, and API key

(If you already have `ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, or `GOOGLE_API_KEY` set in your shell, the wizard is skipped.)

**Try your first question:**

> _"What are the top product categories by number of orders?"_

You should see the agent search DataHub, write SQL, and render a chart in 10–20 seconds.

## Manual setup

Use this when you're connecting Analytics Agent to your own DataHub instance and warehouse — instead of running the bundled local DataHub + Olist demo from Quickstart.

:::info Manual setup is different from Quickstart
Quickstart spins up a local DataHub instance and loads sample data inside Docker. Manual setup runs the agent natively (no Docker) against an existing DataHub instance and warehouse you already have. **Use Quickstart to evaluate; use Manual for real deployments.**
:::

### Step 1 — Clone and install

You'll need Python 3.11+, Node.js 18+, [`uv`](https://docs.astral.sh/uv/), [`pnpm`](https://pnpm.io/), and [`just`](https://just.systems/).

```bash
git clone https://github.com/datahub-project/analytics-agent.git
cd analytics-agent
just install        # runs: uv sync + pnpm install
```

<details>
<summary>Without <code>just</code></summary>

```bash
uv sync
cd frontend && pnpm install && cd ..
```

</details>

Copy the config templates:

```bash
cp .env.example .env
cp config.yaml.example config.yaml
```

All secrets go in `.env`. The `config.yaml` holds connection topology — no credentials there.

### Step 2 — Connect to DataHub

Analytics Agent works with any DataHub instance. Cloud users get additional context capabilities (automations, semantic enrichments) that improve answer quality.

<Tabs>
  <TabItem value="cloud" label="DataHub Cloud" default>

```bash
# Authenticate via the DataHub CLI (writes to ~/.datahubenv)
datahub init --sso \
  --host https://your-org.acryl.io/gms \
  --token-duration ONE_MONTH
```

Analytics Agent reads `~/.datahubenv` automatically. No extra config needed.

  </TabItem>
  <TabItem value="selfhosted" label="DataHub Core (self-hosted)">

```bash
# Authenticate via the DataHub CLI
datahub init \
  --host http://your-datahub-host:8080 \
  --username datahub \
  --password datahub
```

Or set the environment variables directly in `.env`:

```bash
DATAHUB_GMS_URL=http://your-datahub-host:8080
DATAHUB_GMS_TOKEN=your-token-here
```

  </TabItem>
</Tabs>

### Step 3 — Configure your LLM

Add one of the following to your `.env`:

<Tabs>
  <TabItem value="anthropic" label="Anthropic (recommended)" default>

```bash
LLM_PROVIDER=anthropic
ANTHROPIC_API_KEY=sk-ant-...
```

Get an API key at [console.anthropic.com](https://console.anthropic.com/).

  </TabItem>
  <TabItem value="openai" label="OpenAI">

```bash
LLM_PROVIDER=openai
OPENAI_API_KEY=sk-...
```

Get an API key at [platform.openai.com](https://platform.openai.com/).

  </TabItem>
  <TabItem value="google" label="Google">

```bash
LLM_PROVIDER=google
GOOGLE_API_KEY=...
```

Get an API key at [aistudio.google.com](https://aistudio.google.com/).

  </TabItem>
  <TabItem value="bedrock" label="AWS Bedrock">

```bash
LLM_PROVIDER=bedrock
AWS_REGION=us-west-2
LLM_MODEL=us.anthropic.claude-sonnet-4-5-20250929-v1:0
```

Bedrock requires full inference-profile model IDs (e.g. `us.anthropic.claude-sonnet-4-5-20250929-v1:0`). The standard AWS credential chain (env vars, `~/.aws/credentials`, IAM role) is used by default. To override, set `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (and optionally `AWS_SESSION_TOKEN` for STS).

  </TabItem>
</Tabs>

### Step 4 — Add a SQL engine

Define a connection upfront in `config.yaml`, or add one from the **Settings UI** after starting.

<Tabs>
  <TabItem value="snowflake" label="Snowflake" default>

```yaml
# config.yaml
engines:
  - type: snowflake
    name: prod
    connection:
      account: "${SNOWFLAKE_ACCOUNT}" # e.g. xy12345.us-east-1
      user: "${SNOWFLAKE_USER}"
      warehouse: "${SNOWFLAKE_WAREHOUSE}"
      database: "${SNOWFLAKE_DATABASE}"
      schema: "${SNOWFLAKE_SCHEMA}"
```

<details>
<summary>Snowflake authentication options</summary>

Snowflake supports five authentication methods:

| Method                          | How to configure                                                                                                                                                                   |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Password**                    | Username + password in the connection form or `.env`                                                                                                                               |
| **Private key (RSA)**           | [Generate a key pair](https://docs.snowflake.com/en/user-guide/key-pair-auth), upload the public key to Snowflake, then set `SNOWFLAKE_PRIVATE_KEY` (base64-encoded PEM) in `.env` |
| **SSO (browser)**               | Settings → Connections → Authentication → SSO — opens a browser login flow                                                                                                         |
| **PAT (Personal Access Token)** | Settings → Connections → Authentication → PAT                                                                                                                                      |
| **OAuth**                       | Settings → Connections → Authentication → OAuth — browser-based OAuth flow                                                                                                         |

</details>

  </TabItem>
  <TabItem value="mysql" label="MySQL">

```yaml
# config.yaml
engines:
  - type: mysql
    name: analytics_db
    connection:
      host: "${MYSQL_HOST}"
      port: 3306
      user: "${MYSQL_USER}"
      password: "${MYSQL_PASSWORD}"
      database: "${MYSQL_DATABASE}"
```

  </TabItem>
  <TabItem value="duckdb" label="DuckDB">

Add a DuckDB connection from **Settings → Connections → Add Connection**. Point it at a local `.duckdb` file. No authentication required.

  </TabItem>
  <TabItem value="sqlalchemy" label="SQLAlchemy (other)">

```yaml
# config.yaml
engines:
  - type: sqlalchemy
    name: my_db
    connection:
      url: "postgresql+psycopg2://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:5432/${DB_NAME}"
```

Any dialect supported by [SQLAlchemy](https://docs.sqlalchemy.org/en/20/dialects/) works — PostgreSQL, BigQuery, Redshift, and more.

  </TabItem>
</Tabs>

### Step 5 — Start the server

```bash
just start
```

<details>
<summary>Without <code>just</code></summary>

```bash
cd frontend && pnpm build && cd ..
uv run uvicorn analytics_agent.main:app --port 8100
```

</details>

Database migrations run automatically on startup — no manual `alembic upgrade` needed for first launch.

Open **http://localhost:8100**, complete the setup wizard if prompted, and start asking questions.

## Using Analytics Agent

### Writing good questions

The agent performs best when your DataHub catalog has documentation. But even without it, these practices help:

- **Be specific about the metric** — _"Revenue by product category"_ is clearer than _"show me sales data"_.
- **Mention the time range** — _"Last 30 days"_, _"Q3 2024"_, _"year to date"_.
- **Name the dimensions you care about** — _"Broken down by region and platform"_.
- **Follow up freely** — you don't need to repeat context. _"Filter that to mobile only"_ works after a chart is on screen.

### Context quality score

Every answer shows a context score from 1 to 5 in the chat status bar (visible after the second message in a conversation).

| Score   | What it means                                                                        |
| ------- | ------------------------------------------------------------------------------------ |
| **5**   | The agent found detailed documentation for every concept in your question.           |
| **3–4** | Partial documentation — some tables or metrics were well-documented, others weren't. |
| **1–2** | The agent had to rely mostly on schema introspection and naming conventions.         |

**Hover the score** to see what the agent found, what was missing, and what it had to infer. A low score tells you exactly where to focus catalog documentation.

### `/improve-context` — write back to your catalog

Type `/improve-context` after any conversation. The agent reflects on what it just answered, identifies gaps, and proposes a numbered list of improvements — typically 3–5 items, each labeled `[New doc]`, `[Update existing doc]`, or `[Fix description]`.

Approve which proposals to publish:

- `all` — accept everything
- Specific numbers (e.g. `1, 3`) — accept only those
- `none` — skip publishing

After approval, the agent writes the changes to DataHub: entity and column descriptions, glossary updates, or new Reference documents. Your DataHub user/token must have permissions to edit entity descriptions and manage documentation.

Writes are powered by the **Save correction** skill, which is enabled by default. If you've toggled it off, the agent falls back to presenting the proposed updates as copyable markdown so you can apply them manually.

This is the loop: ask → answer → identify gap → improve catalog → next answer is better.

### Write-back skills

Two write-back skills are available, both **enabled by default**:

| Skill                | What it does                                                                                                                                                                                                   | How to invoke                               |
| -------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| **Publish analysis** | Saves the analysis as a DataHub Document (subtype Analysis) in the Knowledge Base, under Shared → Analyses with private, team, or org-wide visibility.                                                         | Natural language: _"publish this analysis"_ |
| **Save correction**  | Writes corrections back to DataHub — either updating entity or column descriptions directly, or creating Reference documents. Used by `/improve-context` to apply approved proposals; also invokable directly. | Natural language: _"save this correction"_  |

Toggle them under **Settings → Connections** → click your DataHub connection card.

### Customizing the agent

The system prompt lives at `backend/src/analytics_agent/prompts/system_prompt.md`. Edit it to add:

- Preferred table naming conventions for your org
- Business rules the agent should always follow
- Output format preferences (e.g. always show a table alongside the chart)

Changes take effect on the next request — no server restart needed. You can also override the prompt per-instance under **Settings → Prompt**.

## How it works

Analytics Agent sits between your people, your DataHub catalog, and your SQL warehouse. When you ask a question, it doesn't go straight to the database — it reads your documentation first, runs the SQL, and writes back what it learns.

<p align="center">
  <img src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/analytics-agent/how-it-works.svg" alt="Analytics Agent architecture: a question flows from the user to the agent, which reads context from DataHub Core or Cloud, executes SQL on a warehouse, streams the response to the browser, and writes context improvements back to DataHub via /improve-context"/>
</p>

The agent follows a strict priority order every time you ask something:

1. **Search business documentation first** — DataHub for definitions, metrics, domain knowledge. What your docs say is authoritative over naming conventions.
2. **Discover datasets** — searches the asset catalog for relevant tables, dashboards, or pipelines.
3. **Inspect schemas and metadata** — column descriptions, owners, tags, classifications.
4. **Check lineage** — picks the right table (e.g. a `PROD` view instead of a `STAGING` source).
5. **Review query history** — sees how a dataset has been queried before.
6. **Write and execute SQL** — only after gathering context.

:::note
If the agent finds a conflict between what your docs say and what the data shows — for example, a metric defined as "trailing 30 days" but no recent rows in the data — it stops and asks you rather than silently overriding your documentation.
:::

## Configuration reference

<details>
<summary>LLM model defaults</summary>

Defaults are set per-provider. The agent uses the same provider for all four model tiers.

| Provider                    | Main agent (`LLM_MODEL`)                       | Chart / Quality / Delight                     |
| --------------------------- | ---------------------------------------------- | --------------------------------------------- |
| **Anthropic** (recommended) | `claude-sonnet-4-6`                            | `claude-haiku-4-5-20251001`                   |
| **OpenAI**                  | `gpt-4o`                                       | `gpt-4o-mini`                                 |
| **Google**                  | `gemini-2.0-flash`                             | `gemini-1.5-flash`                            |
| **AWS Bedrock**             | `us.anthropic.claude-sonnet-4-5-20250929-v1:0` | `us.anthropic.claude-haiku-4-5-20251001-v1:0` |

Model tiers:

| Tier    | Env var             | Used for                                   |
| ------- | ------------------- | ------------------------------------------ |
| Main    | `LLM_MODEL`         | SQL reasoning, agent thinking              |
| Chart   | `CHART_LLM_MODEL`   | Vega-Lite chart generation                 |
| Quality | `QUALITY_LLM_MODEL` | Context quality scoring                    |
| Delight | `DELIGHT_LLM_MODEL` | Conversation titles, time-of-day greetings |

For complex multi-table queries or large schemas, try a stronger model on `LLM_MODEL` (e.g. `claude-opus-4-7` if using Anthropic). The other three tiers don't need a large model.

</details>

<details>
<summary>All environment variables</summary>

```bash
# ── DataHub ──────────────────────────────────────────────────────────
DATAHUB_GMS_URL=https://your-org.acryl.io/gms    # overrides ~/.datahubenv
DATAHUB_GMS_TOKEN=eyJhbGci...                     # overrides ~/.datahubenv

# ── LLM ──────────────────────────────────────────────────────────────
LLM_PROVIDER=anthropic                  # anthropic | openai | google | bedrock
ANTHROPIC_API_KEY=sk-ant-...
LLM_MODEL=claude-sonnet-4-6
CHART_LLM_MODEL=claude-haiku-4-5-20251001
QUALITY_LLM_MODEL=claude-haiku-4-5-20251001
DELIGHT_LLM_MODEL=claude-haiku-4-5-20251001

# ── SQL engines ───────────────────────────────────────────────────────
ENGINES_CONFIG=./config.yaml
SQL_ROW_LIMIT=500                       # max rows returned per query

# ── Storage ───────────────────────────────────────────────────────────
DATABASE_URL=sqlite+aiosqlite:///./data/dev.db    # default (local dev)
# DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/analytics

# ── Server ────────────────────────────────────────────────────────────
LOG_LEVEL=INFO
SSE_KEEPALIVE_INTERVAL=15
```

</details>

<details>
<summary><code>just</code> commands</summary>

| Command                | What it does                                                             |
| ---------------------- | ------------------------------------------------------------------------ |
| `just install`         | Install all Python and Node dependencies                                 |
| `just start`           | Build the frontend (if stale) and start the backend at `:8100`           |
| `just start port=8102` | Start on a custom port                                                   |
| `just stop`            | Kill the backend process                                                 |
| `just dev`             | Hot-reload backend only (no frontend build)                              |
| `just dev-full`        | Hot-reload backend + Vite HMR frontend at `:5173`                        |
| `just nuke`            | Wipe the SQLite database (server stays stopped — run `just start` after) |
| `just logs`            | Tail `/tmp/analytics_agent.log`                                          |
| `just test`            | Run unit tests                                                           |
| `just build`           | Force a frontend rebuild                                                 |

</details>

## Production deployment

<details>
<summary>Switch to PostgreSQL</summary>

The default SQLite database is fine for local use and testing. For production, switch to PostgreSQL so conversation history survives restarts and scales across multiple users:

```bash
# .env
DATABASE_URL=postgresql+asyncpg://user:pass@your-db-host:5432/analytics
```

Migrations run automatically on server startup against whatever database is configured.

</details>

<details>
<summary>Run as a service</summary>

```bash
uv run uvicorn analytics_agent.main:app \
  --host 0.0.0.0 \
  --port 8100 \
  --workers 2
```

Use a process manager like [systemd](https://systemd.io/) or [supervisord](http://supervisord.org/) to keep the server running after reboots, and put an HTTPS termination proxy ([nginx](https://nginx.org/), [Caddy](https://caddyserver.com/)) in front of it.

</details>

<details>
<summary>Docker</summary>

Build and run from source:

```bash
docker build -f docker/Dockerfile -t analytics-agent .
docker run -p 8100:8100 --env-file .env analytics-agent
```

Or pull the pre-built image from GitHub Container Registry:

```bash
docker pull ghcr.io/datahub-project/analytics-agent:main
docker run -p 8100:8100 --env-file .env ghcr.io/datahub-project/analytics-agent:main
```

Available tags: `:main` (latest from main branch), `:sha-<short-hash>` (specific commit), `:<version>` (release tags).

</details>

<details>
<summary>Updating</summary>

```bash
git pull
uv sync
cd frontend && pnpm install && pnpm build && cd ..
just stop && just start
```

</details>

## Troubleshooting

### Backend won't start

**Symptom:** Server exits immediately or throws an import error.

**Check:**

- `.env` exists and has at minimum `LLM_PROVIDER` and the matching API key
- Your Python version is 3.11+: `python --version`
- Dependencies are installed: `uv sync`

```bash
# Surface import errors explicitly
uv run python -c "import analytics_agent.main"
```

### "Connection refused" on DataHub

**Symptom:** The agent returns errors about not being able to reach DataHub.

**Check:**

- Run `datahub check server` to verify your DataHub CLI credentials
- If you set `DATAHUB_GMS_URL` in `.env`, confirm the URL includes `/gms` (e.g. `https://your-org.acryl.io/gms`, not just `https://your-org.acryl.io`)
- Test the connection directly:

```bash
curl -s -X POST http://localhost:8100/api/settings/connections/datahub/test
```

### Low context quality scores

**Symptom:** Score is consistently 1–2 even for questions about your core metrics.

**Cause:** The agent can't find relevant documentation in DataHub for the tables or metrics you're asking about.

**Fix:** Type `/improve-context` after any low-scoring answer. The agent will give you a numbered list of specific documentation to add. Approve the proposals you want, and the score will rise on future questions.

<details>
<summary>More troubleshooting</summary>

### SQL errors on execution

**Symptom:** The agent writes SQL but execution fails.

**Check:**

- Open **Settings → Connections** and click **Test** next to the engine
- Confirm the warehouse, database, and schema in your config match what exists in the warehouse
- For Snowflake: verify the user has `USAGE` on the warehouse and `SELECT` on the tables

### Charts not rendering

**Symptom:** The agent returns a text answer but no chart appears.

**Check:**

- Expand the SQL step in the conversation to verify the query returned rows. If the result is empty, the chart generator has nothing to plot — refine your question or check your date filters.
- If rows are present but no chart appears, check the browser console for JavaScript errors.

### AWS Bedrock `ValidationException`

**Symptom:** Requests to Bedrock fail with `ValidationException: The provided model identifier is invalid`.

**Cause:** Bedrock requires full inference-profile IDs, not native Anthropic model IDs.

**Fix:** Use the full inference-profile ID for your region. For example:

```
us.anthropic.claude-sonnet-4-5-20250929-v1:0   ✓
claude-sonnet-4-6                               ✗
```

Find the correct IDs in the [AWS Bedrock documentation](https://docs.aws.amazon.com/bedrock/latest/userguide/inference-profiles-support.html).

### `/improve-context` proposals show as markdown instead of writing to DataHub

**Symptom:** After you approve proposals, the agent shows copyable markdown blocks instead of writing changes to DataHub.

**Cause:** The Save correction skill has been toggled off.

**Fix:** Open **Settings → Connections** → click your DataHub connection card → toggle Save correction back on.

If Save correction is enabled but writes still fail, check:

- The DataHub user/token in your config has permissions to edit entity descriptions and manage documentation
- For DataHub Cloud, verify the token hasn't expired (`datahub init` to refresh)

### "Unexpected tool_use_id" errors in logs

**Symptom:** You see `tool_use_id` errors in the server logs, often after restarting mid-conversation.

**Cause:** LangChain requires that every `ToolMessage` in history matches a `tool_use` block in the preceding `AIMessage`. This can get out of sync if a conversation was interrupted.

**Fix:** Start a new conversation. If the issue persists across all conversations, run `just nuke` followed by `just start` to reset the database.

</details>

## FAQ

**How long does the quickstart take?**

Plan for 15–25 minutes on a fresh machine. Most of that time is Docker pulling DataHub images on first run (3–5 GB). Subsequent runs take 3–6 minutes since images are cached.

**Can I connect multiple warehouses?**

Yes. Add multiple connections in **Settings → Connections**. Each conversation lets you choose which engine to use from the welcome screen.

**Does it work with my existing DataHub catalog or do I need to add documentation first?**

It works with any DataHub instance. Without documentation, the agent falls back to schema introspection and naming conventions — scores will be lower but it will still function. Documentation improves accuracy significantly, and `/improve-context` helps you figure out exactly what to add.

**Can multiple people use one instance?**

Yes. Analytics Agent is a shared server. Each browser session gets its own conversation history. For production multi-user deployments, use PostgreSQL and put authentication in front of it.

<details>
<summary>More FAQ</summary>

**Is conversation history saved?**

Yes. All conversations are stored in the configured database and accessible from the sidebar across restarts.

**Can I self-host the LLM?**

If your LLM is accessible via an OpenAI-compatible API, set `LLM_PROVIDER=openai` and override `LLM_MODEL` with your model name. You may also need to set a custom `OPENAI_API_BASE` — check the [LangChain ChatOpenAI docs](https://python.langchain.com/docs/integrations/chat/openai/) for the env var name.

**Can I use Analytics Agent without DataHub?**

Analytics Agent is designed around DataHub as its metadata context layer. Without a DataHub connection, the agent falls back to schema introspection only — there's no business documentation, no lineage, and no quality score. It'll still generate SQL, but the accuracy on business-level questions drops significantly.

**Does Windows work?**

Not natively. Use [WSL2](https://learn.microsoft.com/en-us/windows/wsl/install) — the bash-based quickstart and `just` runner won't work in PowerShell or CMD.

**How do I reset everything and start fresh?**

```bash
just nuke
just start
```

`just nuke` wipes the local SQLite database (conversations, connections, settings) and stops the server. Run `just start` after to launch it clean.

</details>

## Next steps

- **Improve your catalog** — Run `/improve-context` after a few conversations to identify which DataHub documentation will have the biggest impact on answer quality.
- **Connect your warehouse** — If you used the quickstart, replace the sample Olist connection with your own in **Settings → Connections**.
- **Customize the agent** — Edit `backend/src/analytics_agent/prompts/system_prompt.md` to add org-specific business rules and table naming conventions.
- **Contribute** — Analytics Agent is open source. Issues, PRs, and discussions welcome at [github.com/datahub-project/analytics-agent](https://github.com/datahub-project/analytics-agent).
