<p align="center">
  <a href="https://datahub.com">
    <img alt="DataHub" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-logo-color-mark.svg" height="150" />
  </a>
</p>

<p align="center">
  <a href="https://github.com/datahub-project/datahub/actions/workflows/build-and-test.yml"><img src="https://github.com/datahub-project/datahub/actions/workflows/build-and-test.yml/badge.svg" alt="Build Status" /></a>
  <a href="https://pypi.org/project/acryl-datahub/"><img src="https://img.shields.io/pypi/v/acryl-datahub.svg" alt="PyPI Version" /></a>
  <a href="https://pypi.org/project/acryl-datahub/"><img src="https://img.shields.io/pypi/dm/acryl-datahub.svg" alt="PyPI Downloads" /></a>
  <a href="https://hub.docker.com/r/acryldata/datahub-gms"><img src="https://img.shields.io/docker/pulls/acryldata/datahub-gms.svg" alt="Docker Pulls" /></a>
  <a href="https://datahub.com/slack"><img src="https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social" alt="Join Slack" /></a>
  <a href="https://github.com/datahub-project/datahub/stargazers"><img src="https://img.shields.io/github/stars/datahub-project/datahub.svg?style=social&label=Star" alt="GitHub Stars" /></a>
  <a href="https://github.com/datahub-project/datahub/blob/master/LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License" /></a>
</p>

<p align="center">
  <a href="https://demo.datahub.com"><b>Live Demo</b></a> ·
  <a href="https://docs.datahub.com"><b>Docs</b></a> ·
  <a href="https://datahub.com/slack"><b>Slack</b></a>
</p>

---

DataHub transforms enterprise data into trusted context, enabling intelligent decision making by humans and AI agents. The company was founded by the creators of the popular DataHub open source product that has more than 16,000 community members and 750+ contributors and is used by thousands of organizations. The company’s flagship product, DataHub Cloud, is the leading context management platform trusted by the Global 2000 to ensure that context is always relevant, reliable and continuously refreshed across the entire data estate.

---

## See DataHub in action

<p align="center">
  <a href="https://demo.datahub.com">
    <img width="90%" src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/demos/datahub-tour.gif" alt="DataHub Product Tour" />
  </a>
</p>

<p align="center">
  <i>Updated product tour GIF coming soon.</i>
</p>

---

Trusted in production by teams at Netflix, Visa, Etsy, Slack, Apple, FIS, Miro, and [3,000+ organizations worldwide →](https://datahub.com/resources/customer-stories/) · [See all adopters](ADOPTERS.md)

---

## Pick your path

### DataHub OSS

Self-host on your own infrastructure. Apache 2.0 licensed. Full access to the metadata graph, 150+ integrations, column-level lineage, governance, and discovery. Best for teams that want full control and are comfortable running their own stack.

[Run it locally ↓](#quick-start) · [Quickstart guide →](https://docs.datahub.com/docs/quickstart) · [Deploy on Kubernetes →](https://docs.datahub.com/docs/deploy/kubernetes)

---

### DataHub Cloud

Managed, SLA-backed, enterprise-ready. Access data observability and the full Context Platform: Context Intelligence, Context Hub, and native agent integrations out of the box. No infrastructure to run.

[Start a free trial →](https://datahub.com/free-trial/) · [Compare OSS vs Cloud →](https://docs.datahub.com/docs/managed-datahub/managed-datahub-overview)

---

## Core capabilities

Some capabilities below are available only in DataHub Cloud. [Compare OSS vs Cloud →](https://docs.datahub.com/docs/managed-datahub/managed-datahub-overview)

### [Context Platform](https://datahub.com/products/context-platform/)

Turn your data estate into a trusted knowledge base for AI agents.

- **Context Ingestion** pulls metadata from 150+ integrations, dbt, Power BI, Confluence, Notion into a unified context graph
- **Context Intelligence** mines years of query history to build a semantic index of how your organization actually uses its data; no manual authoring required
- **Context Hub** - a workspace where domain experts review, approve, and enrich AI-proposed context before it reaches any agent
- **Context Activation** serves validated context to any agent via Model Context Protocol (MCP), GraphQL, API, or SDK

### [Discovery](https://datahub.com/products/data-discovery/)

Find the right data, fast.

- Universal search using natural language across your entire data estate
- Column-level lineage to trace data from source to consumption
- Data profiling: schema, statistics, ownership, usage in one place

### [Governance](https://datahub.com/products/data-governance/)

Make data trustworthy at scale.

- Business glossary with shared definitions, owned and versioned
- Ownership and stewardship for every asset
- Access policies and compliance controls

### [Observability](https://datahub.com/products/data-observability/)

Know when something breaks before your users do.

- Data quality assertions and monitoring
- Freshness checks and SLA tracking
- Incident tracking and root cause lineage

[→ See the full product tour at datahub.com](https://datahub.com/product-tour/)

---

## [Why DataHub](https://datahub.com/products/cloud-vs-core/)

- **Cross-platform by design.** DataHub started at LinkedIn in 2019 to manage metadata at hyperscale. That foundation with column-level lineage across 150+ integrations, spanning your entire data estate is what makes trusted context possible. Context is only as good as the lineage underneath it, and lineage is only as good as its coverage.

- **Battle-tested at scale.** Born at LinkedIn to handle one of the largest data estates in the world. Manages 10M+ assets in production today.

- **Accuracy you can measure.** Context Intelligence mines your existing query history to build a semantic index from day one with no manual authoring, no months of workshops. Customers report text-to-SQL accuracy improving from 50% to 90% after connecting DataHub. 119% more AI/ML models reach production when teams can trust their data. _([IDC, March 2026](https://datahub.com/roi/))_

- **Discovery that actually works.** Business users find trusted data in five minutes, down from 50 — a 91% reduction in search time. _([IDC, March 2026](https://datahub.com/roi/))_

- **Open by default, extensible by design.** Apache 2.0. Built on open standards: MCP for agent delivery, GraphQL and REST APIs. Bring your own agents, your own LLM, your own stack. Join our Slack community with 16,000+ members.

---

## Quick start

1. [**Try the live demo →**](https://demo.datahub.com) No installation required.

2. **Run locally:**

   Requires Docker (8GB RAM) and Python 3.10+.

   ```sh
   pip install acryl-datahub
   datahub docker quickstart
   # → http://localhost:9002 (username: datahub, password: datahub)
   ```

   [Full quickstart guide →](https://docs.datahub.com/docs/quickstart)

3. **Connect your AI assistant via MCP:** add the DataHub MCP server to your MCP client (Claude Desktop, Cursor, and more).

   ```sh
   uvx mcp-server-datahub@latest
   ```

   [MCP server setup →](https://docs.datahub.com/docs/features/feature-guides/mcp)

---

## Integrations

150+ production-grade integrations across your full data stack.

[See all integrations →](https://docs.datahub.com/integrations)

---

## Built by the community

DataHub has 16,000+ community members and 750+ contributors across 3,600+ organizations. Every connector, integration, and improvement you use was built by people like you.

| Join [Slack](https://datahub.com/slack) community                                  | Watch demos and events on [YouTube](https://www.youtube.com/@DataHubCloud) |
| :--------------------------------------------------------------------------------- | :------------------------------------------------------------------------- |
| Register to [Monthly Town Hall](https://datahub.com/community/datahub-town-halls/) | Read our [Blog](https://datahub.com/blog/)                                 |

Ready to contribute? See [CONTRIBUTING.md](https://github.com/datahub-project/datahub/blob/master/docs/CONTRIBUTING.md) for setup and guidelines.

---

Explore DataHub Now

[Live Demo](https://demo.datahub.com) · [Docs](https://docs.datahub.com) · [Slack](https://datahub.com/slack) · [LinkedIn](https://www.linkedin.com/company/datahub-cloud/) · [X](https://x.com/DataHubCloud) · [Security](https://docs.datahub.com/docs/security) · [Feature Requests](https://datahubspace.slack.com/archives/C02FWNS2F08)

### License

Apache License 2.0. See [LICENSE](https://github.com/datahub-project/datahub/blob/master/LICENSE).

```
Copyright 2015-2026 LinkedIn Corporation
Copyright 2025-Present DataHub Project Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
```
