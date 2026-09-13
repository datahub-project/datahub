
# Core vs Cloud: Comparison Guide

This guide compares DataHub Core (OSS) and DataHub Cloud features and platform differences. DataHub Cloud builds on the DataHub Core foundation with enterprise-grade capabilities including AI automation, advanced governance, operational reliability, and production support for mid-to-large organizations. Cloud also offers a fully managed service with 99.5%+ SLA-backed availability, dedicated support, enhanced security, training services, and flexible deployment options.

## Context Platform

> How context gets into DataHub, enriched, curated, and served to agents.

### Context Ingestion

Connect sources and pull technical, business, and unstructured context into DataHub.

| Feature Name                                                  | Core | Cloud | Business Value                                                            |                                                               Link                                                               |
| :------------------------------------------------------------ | :--: | :---: | :------------------------------------------------------------------------ | :------------------------------------------------------------------------------------------------------------------------------: |
| **140+ Source Connectors with Unified Search**                |  ✔  |  ✔   | Connect entire data ecosystem                                             |                                          [Docs](/integrations)                                           |
| **dbt Metrics & Semantic Model Ingestion**                    |  ✔  |  ✔   | Bring in metric definitions and semantic models from dbt                  |                       [Docs](/docs/generated/ingestion/sources/dbt#semantic-views)                       |
| **BI Tool Glossary & Definitions Ingestion**                  |  ✔  |  ✔   | Ingest business glossaries and definitions from Power BI, Tableau, Looker |                                                                                                                                  |
| **Unstructured Document Ingestion (Notion, Confluence, etc)** |  ✔  |  ✔   | Pull institutional knowledge from wikis, docs, and collaboration tools    |           [Docs](/docs/features/feature-guides/context/context-documents#importing-documents)            |
| **Chunking & Semantic Embedding**                             |  ✔  |  ✔   | Auto-chunk and embed all context for real-time semantic retrieval         |                                                                                                                                  |
| **Continuous Technical Metadata Sync**                        |  ✔  |  ✔   | Keep the context graph current as data and definitions evolve             |                                                                                                                                  |
| **GraphQL & MCP Retrieval**                                   |  ✔  |  ✔   | Serve context to any agent or tool via GraphQL, MCP, or API               | [GraphQL](/docs/api/graphql/overview) · [MCP](/docs/features/feature-guides/mcp) |

### Context Intelligence

Mine and generate context automatically instead of authoring it by hand.

| Feature Name                                                      | Core | Cloud | Business Value                                                                          |                                           Link                                            |
| :---------------------------------------------------------------- | :--: | :---: | :-------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------: |
| **Cross-Platform Query History Mining**                           |  ❌  |  ✔   | Turn years of warehouse query history into a structured semantic index                  |                                                                                           |
| **Metrics & Semantic Models**                                     |  ✔  |  ✔   | Search and browse business metrics alongside datasets, pipelines, and lineage           | [Docs](/docs/features/feature-guides/metrics-and-semantic-models) |
| **Context Documents Generation**                                  |  ❌  |  ✔   | Auto-generate context documents capturing proven joins, filters, metric definitions     |  [Docs](/docs/features/feature-guides/context/context-documents)  |
| **AI Documentation Generation**                                   |  ❌  |  ✔   | Auto-document tables and columns at scale without manual authoring                      |                 [Docs](/docs/automations/ai-docs)                 |
| **Automated Context Updates**                                     |  ❌  |  ✔   | Continuously refresh context as schemas, queries, and business definitions change       |                                                                                           |
| **Automated Eval Execution (Benchmark Q&A & Regression Testing)** |  ❌  |  ✔   | Run benchmark Q&A and regression tests to catch accuracy drift before it reaches agents |                                                                                           |

### Context Hub

Review, disambiguate, and approve context before agents rely on it.

| Feature Name                                                     | Core | Cloud | Business Value                                                                           |                                  Link                                  |
| :--------------------------------------------------------------- | :--: | :---: | :--------------------------------------------------------------------------------------- | :--------------------------------------------------------------------: |
| **Collaborative Workflow for Reviewing and Editing Context**     |  ❌  |  ✔   | Give domain experts a structured inbox to approve, reject, or refine AI-proposed context |                                                                        |
| **Business Context Disambiguation & Domain-Specific Resolution** |  ❌  |  ✔   | Resolve conflicting definitions at the domain level before they reach agents             |                                                                        |
| **Simulation & Validation Before Context Publishing**            |  ❌  |  ✔   | Preview how context changes affect text-to-SQL results before going live                 |                                                                        |
| **Change Proposals: Documentation, Glossary, Tags & Ownership**  |  ❌  |  ✔   | Controlled vocabulary and documentation changes with audit trail                         | [Docs](/docs/managed-datahub/change-proposals) |

### Context Activation

Serve verified context to agents, tools, and custom workflows.

| Feature Name                                                    | Core | Cloud | Business Value                                                                                                      |                                       Link                                       |
| :-------------------------------------------------------------- | :--: | :---: | :------------------------------------------------------------------------------------------------------------------ | :------------------------------------------------------------------------------: |
| **DataHub Hosted MCP Server**                                   |  ❌  |  ✔   | Connect AI tools directly to your data catalog                                                                      |        [Docs](/docs/features/feature-guides/mcp)         |
| **Scoped MCP Servers**                                          |  ❌  |  ✔   | Create purpose-built MCP servers with curated tools and scoped access to specific data assets and context documents | [Docs](/docs/features/feature-guides/scoped-mcp-servers) |
| **Skills Library (Ask DataHub, SQL grounding, lineage lookup)** |  ✔  |  ✔   | Pre-built skills for accurate SQL generation and data discovery                                                     |      [Docs](/docs/dev-guides/agent-context/skills)       |
| **Full API & SDK**                                              |  ✔  |  ✔   | Integrate DataHub context into any custom agent or workflow                                                         |   [Docs](/docs/dev-guides/agent-context/agent-context)   |
| **Native Agent Surfaces (Claude, Snowflake, Databricks, etc.)** |  ✔  |  ✔   | Surface verified context inside the tools your team already uses                                                    |      [Docs](/docs/dev-guides/agent-context/claude)       |
| **Ask DataHub AI Agent + Plugins**                              |  ❌  |  ✔   | Find trustworthy metrics, generate accurate SQL, debug data quality issues, understand data impact                  |    [Docs](/docs/features/feature-guides/ask-datahub)     |

## Discovery & Search

> How people and agents find, understand, and navigate assets across the estate.

| Feature Name                                   | Core | Cloud | Business Value                                                                                                                                                                                                  |                                                    Link                                                     |
| :--------------------------------------------- | :--: | :---: | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------------------------------------: |
| **140+ Source Connectors with Unified Search** |  ✔  |  ✔   | Connect entire data ecosystem                                                                                                                                                                                   |                                [Docs](/integrations)                                |
| **Ask DataHub AI Agent + Plugins**             |  ❌  |  ✔   | <ul><li>Find trustworthy data metrics</li><li>Generate Accurate SQL</li><li>Debug data quality issues</li><li>Understand impact of data changes</li><li>Human-in-the-loop approval for metadata edits</li></ul> |                  [Docs](/docs/features/feature-guides/ask-datahub)                  |
| **Agent Registry**                             |  ❌  |  ✔   | Catalog AI agents, skills, tools, and MCP servers with lineage to the data they consume                                                                                                                         |                [Docs](/docs/features/feature-guides/agent-registry)                 |
| **Service Catalog**                            |  ❌  |  ✔   | Catalog repositories, services, and APIs as governed entities connected to your data lineage graph                                                                                                              |                [Docs](/docs/features/feature-guides/service-catalog)                |
| **DataHub Hosted MCP Server**                  |  ❌  |  ✔   | Connect AI tools directly to your data catalog                                                                                                                                                                  |                      [Docs](/docs/features/feature-guides/mcp)                      |
| **Enhanced Usage-Aware Search Ranking**        |  ❌  |  ✔   | Surface most relevant data first                                                                                                                                                                                |               [Docs](/docs/how/search#example-1-ranking-by-tagsterms)               |
| **Column-Level Lineage & Impact Analysis**     |  ✔  |  ✔   | Understand data dependencies                                                                                                                                                                                    |                    [Docs](/docs/features/feature-guides/lineage)                    |
| **Lineage-Based Propagation**                  |  ❌  |  ✔   | Auto-enrich downstream datasets                                                                                                                                                                                 |               [Docs](/docs/automations/docs-propagation#introduction)               |
| **Context Documents**                          |  ✔  |  ✔   | Create & semantically search across unstructured docs                                                                                                                                                           |           [Docs](/docs/features/feature-guides/context/context-documents)           |
| **Import Context Documents from GitHub**       |  ✔  |  ✔   | Import GitHub repositories as semantically-searchable context documents, editable in DataHub; DataHub Cloud syncs edits back via PRs                                                                            | [Docs](/docs/features/feature-guides/context/context-documents#importing-documents) |
| **AI Documentation Generation**                |  ❌  |  ✔   | Auto-document tables & columns                                                                                                                                                                                  |                          [Docs](/docs/automations/ai-docs)                          |
| **Logical Models UI**                          |  ✔  |  ✔   | Create and manage logical models from the UI without API or SDK access                                                                                                                                          |            [Docs](/docs/features/feature-guides/logical-models/overview)            |
| **Logical Model Propagation**                  |  ❌  |  ✔   | Propagate tags, terms, documentation & ownership from logical models to their physical assets                                                                                                                   |     [Docs](/docs/features/feature-guides/logical-models/centralized-management)     |
| **Personalized Home and Asset Views**          |  ❌  |  ✔   | Customize home page and asset summaries for a personalized data experience                                                                                                                                      |     [Docs](/docs/features/feature-guides/custom-asset-summaries#custom-modules)     |
| **Multi-Channel Notifications**                |  ❌  |  ✔   | Stay informed where you work (Email, Slack, & Teams)                                                                                                                                                            | [Docs](/docs/incidents/incidents/#enabling-slack-notifications-datahub-cloud-only)  |

## Data Observability

> How data health is monitored, alerted on, and resolved when something breaks.

| Feature Name                                                         | Core | Cloud | Business Value                                                           |                                                           Link                                                            |
| :------------------------------------------------------------------- | :--: | :---: | :----------------------------------------------------------------------- | :-----------------------------------------------------------------------------------------------------------------------: |
| **Quality & Health Status on Asset Profiles**                        |  ✔  |  ✔   | See quality at a glance                                                  |                          [Docs](/docs/managed-datahub/observe/overview)                           |
| **AI Anomaly Detection**                                             |  ❌  |  ✔   | Catch issues automatically                                               |                      [Docs](/docs/managed-datahub/observe/anomaly-detection)                      |
| **Freshness, Volume, Schema & Column Monitoring, Custom SQL Checks** |  ❌  |  ✔   | Ensure timely and correct data                                           |                    [Docs](/docs/managed-datahub/observe/freshness-assertions)                     |
| **Data Contracts**                                                   |  ✔  |  ✔   | Define quality expectations                                              |            [Docs](/docs/managed-datahub/observe/data-contract#what-is-a-data-contract)            |
| **Data Health Dashboard**                                            |  ❌  |  ✔   | Quality overview at scale                                                |                    [Docs](/docs/managed-datahub/observe/data-health-dashboard)                    |
| **Notifications for Data Assertions**                                |  ❌  |  ✔   | Real-time quality alerts                                                 |                    [Docs](/docs/managed-datahub/subscription-and-notification)                    |
| **Secure In-VPC Quality Validation**                                 |  ❌  |  ✔   | Your data never leaves your network                                      |                        [Docs](/docs/managed-datahub/remote-executor/about)                        |
| **Pipeline Circuit Breakers (API)**                                  |  ❌  |  ✔   | Validate data quality programmatically before reads or writes            |                      [Docs](/docs/managed-datahub/observe/data-contract#api)                      |
| **Data Observability Agent**                                         |  ❌  |  ✔   | Identify and resolve monitoring gaps on critical datasets                | [Docs](/docs/managed-datahub/observe/data-health-dashboard#data-observability-agent-private-beta) |
| **Monitoring Rules**                                                 |  ❌  |  ✔   | Automatically apply monitoring to datasets matching your custom criteria |           [Docs](/docs/managed-datahub/observe/data-health-dashboard#monitoring-rules)            |
| **Incident Management**                                              |  ✔  |  ✔   | Create and track data incidents                                          |                                 [Docs](/docs/incidents/incidents)                                 |

## Data Governance

> How ownership, vocabulary, compliance, and access are defined and enforced.

| Feature Name                                                                   | Core | Cloud | Business Value                                                               |                                               Link                                                |
| :----------------------------------------------------------------------------- | :--: | :---: | :--------------------------------------------------------------------------- | :-----------------------------------------------------------------------------------------------: |
| **Data Ownership Management**                                                  |  ✔  |  ✔   | Clear accountability                                                         | [Docs](/docs/metadata-integration/java/docs/sdk-v2/dataset-entity#owners) |
| **Business Glossary**                                                          |  ✔  |  ✔   | Common data language                                                         |                     [Docs](/learn/business-glossary)                      |
| **Bi-Directional Metadata Sync**                                               |  ❌  |  ✔   | Keep metadata current                                                        |             [Docs](/docs/automations/bigquery-metadata-sync)              |
| **Compliance Forms and Workflow Engine**                                       |  ❌  |  ✔   | Track regulatory compliance                                                  |     [Docs](/docs/features/feature-guides/compliance-forms/analytics)      |
| **Metadata Tests**                                                             |  ❌  |  ✔   | Validate governance rules                                                    |                    [Docs](/docs/tests/metadata-tests)                     |
| **Change Proposals: Documentation, Glossary, Tags, Terms, and Data Ownership** |  ❌  |  ✔   | Controlled vocabulary changes                                                |              [Docs](/docs/managed-datahub/change-proposals)               |
| **Access Request Workflows**                                                   |  ❌  |  ✔   | Self-service data access                                                     |         [Docs](/docs/managed-datahub/workflows/access-workflows)          |
| **Action Workflows**                                                           |  ❌  |  ✔   | Approval flows beyond access requests (certification, governance, proposals) |         [Docs](/docs/managed-datahub/workflows/action-workflows)          |

## Enterprise & Security

> Availability guarantees, network isolation, and access controls for production use.

| Feature Name                      | Core Available | Cloud Available | Business Value          |
| :-------------------------------- | :------------: | :-------------: | :---------------------- |
| **99.5% Uptime SLA**              |       ❌       |       ✔        | Guaranteed availability |
| **Fine-grained Access Control**   |       ❌       |       ✔        | Secure by default       |
| **AWS PrivateLink Support**       |       ❌       |       ✔        | Network isolation       |
| **IP Address Restrictions**       |       ❌       |       ✔        | Access control          |
| **In-VPC Remote Ingestion Agent** |       ❌       |       ✔        | Data security control   |

## Implementation & Support

> How teams get deployed, onboarded, and supported day to day.

| Feature Name                                                                                               | Core Available | Cloud Available | Business Value                                                                                                                                           |
| :--------------------------------------------------------------------------------------------------------- | :------------: | :-------------: | :------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Fully Managed Cloud Deployment**                                                                         |       ❌       |       ✔        | Zero maintenance cloud-hosted instance                                                                                                                   |
| **Dedicated Customer Success**                                                                             |       ❌       |       ✔        | Expert guidance                                                                                                                                          |
| **Guided Implementation & Onboarding**                                                                     |       ❌       |       ✔        | Smooth rollout                                                                                                                                           |
| **[Multi-language Support](/docs/features/feature-guides/multi-language-support)** |       ✔       |       ✔        | Use DataHub in your browser's language: <br /> _GA:_ German <br /> _Beta:_ Spanish, Brazilian Portuguese, French, Italian, Norwegian, Swedish, Hungarian |
| **Private Slack Support Channel**                                                                          |       ❌       |       ✔        | Direct access to experts                                                                                                                                 |
| **Community Support**                                                                                      |       ✔       |       ✔        | Peer assistance                                                                                                                                          |
| **OSS Contribution Fast-Track**                                                                            |       ❌       |       ✔        | Community Contribution Support to DataHub Apache 2.0 Project                                                                                             |

<a href="https://datahub.com/get-datahub-cloud/" style={{ display: 'inline-block', padding: '10px 20px', margin: '10px 0', backgroundColor: '#007bff', color: 'white', borderRadius: '5px', textDecoration: 'none', textAlign: 'center' }}>
See DataHub Cloud In Action
</a>
