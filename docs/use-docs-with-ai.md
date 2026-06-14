---
title: Use DataHub Docs with AI Tools
sidebar_label: Use Docs with AI
description: Connect DataHub documentation to Claude, Cursor, ChatGPT, and other AI assistants for accurate answers in your editor.
---

# Connect DataHub Docs to AI Tools

Query DataHub docs directly from your AI assistant. We publish a machine-readable index of the documentation so AI coding tools can return accurate, current answers about DataHub — without you switching tabs.

## What We Publish

DataHub maintains an [`llms.txt`](https://docs.datahub.com/llms.txt) file: a machine-readable index of the documentation, written for AI tools.

```
https://docs.datahub.com/llms.txt
```

---

## Quick Start by Tool

### Cursor

Add DataHub docs as a custom source:

1. Open **Settings → Features → Docs**
2. Click **+ Add new doc**
3. Enter `https://docs.datahub.com` as the URL

Cursor will index the site. Reference it in chat with `@DataHub`.

### Claude Code

Reference the index in your prompts:

```bash
claude "Using https://docs.datahub.com/llms.txt as reference, how do I set up DataHub ingestion from Snowflake?"
```

Or add the URL to your project's `CLAUDE.md` so Claude Code uses it on every turn.

### Claude (Web & Desktop)

Paste the `llms.txt` URL into the chat:

> Use https://docs.datahub.com/llms.txt as reference. How do I write a custom ingestion source in DataHub?

Claude will fetch the index and the relevant linked pages.

### ChatGPT

With browsing enabled, paste the `llms.txt` URL into your chat. ChatGPT will use it as a navigation aid for the rest of the conversation.

### GitHub Copilot (VS Code)

In VS Code, reference DataHub docs in Copilot Chat using `#fetch`:

```
#fetch https://docs.datahub.com/llms.txt explain DataHub's metadata model
```

---

## Beyond Docs: AI Access to Your Data Context

Connecting AI to docs is one layer. DataHub also provides AI access to your **metadata**:

- **[MCP Server](features/feature-guides/mcp.md)** — Plug Claude, Cursor, or any MCP-compatible client directly into your DataHub instance. Query lineage, find PII, search assets in natural language.
- **[Agent Context Kit](dev-guides/agent-context/agent-context.md)** — Pre-built integrations for LangChain, Cursor, Claude, Gemini CLI, Vertex AI, Snowflake Cortex, Databricks Genie, and Microsoft Copilot Studio.
- **[Ask DataHub](features/feature-guides/ask-datahub.md)** _(Cloud)_ — Natural-language search across your metadata.
- **[Analytics Agent](features/feature-guides/analytics-agent.md)** — Open-source agent (Apache 2.0, bring your own LLM) that turns plain-English data questions into SQL, results, and charts — grounded in your DataHub catalog.

---

## Feedback

This is an early step toward making DataHub docs first-class for AI workflows. If your AI tool isn't covered above or you have ideas for what to add next, let us know:

- [Slack Community](https://datahub.com/slack)
- [GitHub Discussions](https://github.com/datahub-project/datahub/discussions)
- [Open an issue](https://github.com/datahub-project/datahub/issues/new/choose)
