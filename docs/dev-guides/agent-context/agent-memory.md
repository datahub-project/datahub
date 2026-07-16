# Agent Memory

Give your DataHub agent **memory that persists across sessions** by pairing the
[MCP Server](../../features/feature-guides/mcp.md) with an external, MCP-compatible
memory server. Your agent reads live context from DataHub and recalls what it
learned on previous runs — so it stops re-diagnosing the same tables and stops
re-proposing fixes a human already rejected.

**Memory vs. Tools vs. Skills**: The [MCP Server](../../features/feature-guides/mcp.md)
gives agents _tools_ (search a catalog, read a schema, apply a tag).
[Skills](./skills.md) give agents _instructions_ (how to chain those tools into
workflows). **Memory** gives agents _continuity_ — a record of what they observed
and decided last time, so each run builds on the last instead of starting cold.

:::info Pattern, not a product
This guide describes a pattern that works with **any** MCP-compatible memory
server. The examples use [Perseus Vault](https://github.com/Perseus-Computing-LLC/perseus-vault)
(open source, Apache-2.0) as a concrete reference, the same way the
[LangChain guide](./langchain.md) uses OpenAI as a concrete LLM. Substitute your
own memory server freely.
:::

## Why memory?

Without memory, a metadata agent connects to DataHub, reads the graph, acts, and
forgets. The next run it starts from zero. With memory, common agent workflows get
qualitatively better:

- **Schema drift / breaking changes** — persist a schema fingerprint each run;
  next run, diff the live schema against the remembered one to catch a dropped or
  retyped column *before* it breaks downstream consumers.
- **Governance that doesn't repeat itself** — when an owner rules "this column is a
  salted hash, not PII," remember that decision so the agent stops re-flagging it.
- **Regression detection** — remember which issues were fixed, and re-alert only
  when a resolved issue comes back.

_"Run the governance agent again and it's smarter, not repetitive."_

## How it works

Wire **two MCP servers** into one agent: DataHub for what exists now, and a memory
server for what happened last time.

```
        ┌──────────────────────────────┐
        │            Your agent         │
        │  read → recall → reason →     │
        │        act → remember         │
        └───────┬───────────────┬───────┘
      MCP       │               │      MCP
    ┌───────────┘               └────────────┐
    ▼                                         ▼
┌───────────────────┐               ┌──────────────────────┐
│  DataHub MCP       │  reads/writes │  Memory MCP server    │
│  Server            │               │  (store / recall)     │
│  search, lineage,  │               │  cross-session,       │
│  tags, description │               │  persistent           │
└───────────────────┘               └──────────────────────┘
```

## Prerequisites

- A DataHub instance and the [MCP Server](../../features/feature-guides/mcp.md)
  configured (Cloud or self-hosted).
- Any MCP-compatible memory server with `store` and `recall` tools. This guide uses
  [Perseus Vault](https://github.com/Perseus-Computing-LLC/perseus-vault) as the
  example.
- An MCP client / agent runtime that supports **multiple** MCP servers at once
  (Claude Code, Claude Desktop, Cursor, or a custom agent — see the
  [framework guides](./agent-context.md)).

## Step 1: Run a memory MCP server

Start your memory server as a local stdio MCP server (see your memory server's
docs for its exact command). It should expose, at minimum, a way to store a keyed
memory and to recall by query.

## Step 2: Connect both servers to your agent

Point your agent at both MCP servers. For a client that reads an `mcpServers` map
(e.g. Claude Desktop / Claude Code), that looks like:

```json
{
  "mcpServers": {
    "datahub": {
      "command": "uvx",
      "args": ["mcp-server-datahub@latest"],
      "env": {
        "DATAHUB_GMS_URL": "http://localhost:8080",
        "DATAHUB_GMS_TOKEN": "<your-token>",
        "TOOLS_IS_MUTATION_ENABLED": "true"
      }
    },
    "memory": {
      "command": "<your-memory-server-binary>",
      "args": ["mcp"]
    }
  }
}
```

Your agent now discovers DataHub's tools (`search`, `get_lineage`,
`list_schema_fields`, `add_tags`, `update_description`, …) **and** the memory
server's `store` / `recall` tools in the same session.

## Step 3: The read → recall → reason → act → remember loop

Structure each run so memory is consulted before acting and updated after:

```text
for each dataset in scope:
    live   = datahub.read(urn)          # schema + lineage + owners + tags (MCP)
    past   = memory.recall(urn)         # prior fingerprint + decisions (MCP)
    diff   = reason(live, past)         # drift / gaps / regressions
    if diff.actionable and not vetoed_by(past):   # honor remembered decisions
        datahub.write(diff)             # add_tags / update_description / ...
    memory.store(fingerprint(live), diff, decisions)   # remember for next run
```

The `vetoed_by(past)` check is what makes the agent stop repeating itself: before
acting, it consults memory for a human decision (or a previously-rejected fix) that
should suppress the action.

## What to remember

A practical, minimal memory schema for a governance/observability agent:

| Memory | Contents | Used for |
| --- | --- | --- |
| Schema fingerprint | dataset URN + sorted `(column, type)` hash | cross-session drift detection |
| Decision | URN + subject + verdict (e.g. `not_pii`) + source | suppressing re-flagged, owner-resolved issues |
| Issue | URN + subject + status (`open`/`fixed`/`regressed`) | regression detection |
| Owner feedback | verbatim owner note | learning human corrections |

Namespace memory keys by dataset URN so recall stays precise, and consider a memory
server that supports decay so transient noise fades while confirmed decisions persist.

## Next steps

- Start from a [framework guide](./agent-context.md) to pick your agent runtime.
- Review the [MCP Server tools](../../features/feature-guides/mcp.md) your agent can
  read from and write back to.
- Combine memory with [DataHub Skills](./skills.md) so the agent has both the
  workflow instructions and the cross-session continuity to run them well.
