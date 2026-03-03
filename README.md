<!--HOSTED_DOCS_ONLY
import useBaseUrl from '@docusaurus/useBaseUrl';

export const Logo = (props) => {
  return (
    <div style={{ display: "flex", justifyContent: "center", padding: "20px", height: "190px" }}>
      <img
        alt="DataHub Logo"
        src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-logo-color-mark.svg"
        {...props}
      />
    </div>
  );
};

<Logo />

<!--
HOSTED_DOCS_ONLY-->
<p align="center">
<a href="https://datahub.com">
<img alt="DataHub" src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-logo-color-mark.svg" height="150" />
</a>
</p>
<!-- -->

# The #1 Open Source AI Data Catalog

_Enterprise-grade metadata platform enabling discovery, governance, and observability across your entire data ecosystem_

<p align="center">
  <a href="https://github.com/datahub-project/datahub/actions/workflows/build-and-test.yml">
    <img src="https://github.com/datahub-project/datahub/actions/workflows/build-and-test.yml/badge.svg" alt="Build Status" />
  </a>
  <a href="https://pypi.org/project/acryl-datahub/">
    <img src="https://img.shields.io/pypi/v/acryl-datahub.svg" alt="PyPI Version" />
  </a>
  <a href="https://pypi.org/project/acryl-datahub/">
    <img src="https://img.shields.io/pypi/dm/acryl-datahub.svg" alt="PyPI Downloads" />
  </a>
  <a href="https://hub.docker.com/r/linkedin/datahub-gms">
    <img src="https://img.shields.io/docker/pulls/linkedin/datahub-gms.svg" alt="Docker Pulls" />
  </a>
  <br />
  <a href="https://datahub.com/slack?utm_source=github&utm_medium=readme&utm_campaign=github_readme">
    <img src="https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social" alt="Join Slack" />
  </a>
  <a href="https://www.youtube.com/channel/UC3qFQC5IiwR5fvWEqi_tJ5w">
    <img src="https://img.shields.io/youtube/channel/subscribers/UC3qFQC5IiwR5fvWEqi_tJ5w?style=social&logo=youtube&label=Subscribe" alt="YouTube Subscribers" />
  </a>
  <a href="https://datahub.com/blog/">
    <img src="https://img.shields.io/badge/blog-read-red.svg?style=social&logo=medium" alt="DataHub Blog" />
  </a>
  <a href="https://github.com/datahub-project/datahub/graphs/contributors">
    <img src="https://img.shields.io/github/contributors/datahub-project/datahub.svg" alt="Contributors" />
  </a>
  <a href="https://github.com/datahub-project/datahub/stargazers">
    <img src="https://img.shields.io/github/stars/datahub-project/datahub.svg?style=social&label=Star" alt="GitHub Stars" />
  </a>
  <a href="https://github.com/datahub-project/datahub/blob/master/LICENSE">
    <img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License" />
  </a>
</p>

<p align="center">
  <a href="https://docs.datahub.com/docs/quickstart"><b>Quick Start</b></a> •
  <a href="https://demo.datahub.com"><b>Live Demo</b></a> •
  <a href="https://docs.datahub.com"><b>Documentation</b></a> •
  <a href="https://feature-requests.datahubproject.io/roadmap"><b>Roadmap</b></a> •
  <a href="https://datahub.com/slack"><b>Slack Community</b></a> •
  <a href="https://www.youtube.com/@datahubproject"><b>YouTube</b></a>
</p>

<p align="center">
  <i>Built with ❤️ by <a href="https://datahub.com">DataHub</a> and <a href="https://engineering.linkedin.com">LinkedIn</a></i>
</p>

---

<p align="center">
  <a href="https://demo.datahub.com">
    <img width="90%" src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/demos/datahub-tour.gif" alt="DataHub Product Tour" />
  </a>
</p>

<p align="center">
  <i>Search, discover, and understand your data with DataHub's unified metadata platform</i>
</p>

---

### 🤖 **NEW: Connect AI Agents to DataHub via Model Context Protocol (MCP)**

<p align="center">
  <a href="https://youtu.be/aVWJsw7RJ8c?t=568">
    <img width="600" src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/demos/mcp-demo.gif" alt="DataHub MCP Demo - Query metadata with AI agents" />
  </a>
  <br/>
  <i>▶️ Click to watch full demo on YouTube</i>
</p>

Connect your AI coding assistants (Cursor, Claude Desktop, Cline) directly to DataHub.
Query metadata with natural language: _"What datasets contain PII?"_ or _"Show me lineage for this table"_

**Quick setup:**

```bash
npx -y @acryldata/mcp-server-datahub init
```

[Learn more →](https://github.com/acryldata/mcp-server-datahub)

---

## What is DataHub?

> **🔍 Finding the right DataHub?** This is the **open-source metadata platform** at [datahub.com](https://datahub.com) (GitHub: [datahub-project/datahub](https://github.com/datahub-project/datahub)). It was previously hosted at `datahubproject.io`, which now redirects to [datahub.com](https://datahub.com). This project is **not related to** [datahub.io](https://datahub.io), which is a separate public dataset hosting service. See the [FAQ](#-frequently-asked-questions) below.

**DataHub is the #1 open-source AI data catalog** that enables discovery, governance, and observability across your entire data ecosystem. Originally built at LinkedIn, DataHub now powers data discovery at thousands of organizations worldwide, managing millions of data assets.

**The Challenge:** Modern data stacks are fragmented across dozens of tools—warehouses, lakes, BI platforms, ML systems, AI agents, orchestration engines. Finding the right data, understanding its lineage, and ensuring governance is like searching through a maze blindfolded.

**The DataHub Solution:** DataHub acts as the central nervous system for your data stack—connecting all your tools through real-time streaming or batch ingestion to create a unified metadata graph. Unlike static catalogs, DataHub keeps your metadata fresh and actionable—powering both human teams and AI agents.

![DataHub for Humans and AI](https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/datahub_for_human_and_ai.png)

### Why DataHub?

- **🚀 Battle-Tested at Scale:** Born at LinkedIn to handle hyperscale data, now proven at thousands of organizations worldwide managing millions of data assets
- **⚡ Real-Time Streaming:** Metadata updates in seconds, not hours or days
- **🤖 AI-Ready:** Native support for AI agents via MCP, LLM integrations, and context management
- **🔌 Pioneering Ingestion Architecture:** Flexible push/pull framework (widely adopted by other catalogs) with 80+ production-grade connectors extracting deep metadata—column lineage, usage stats, profiling, and quality metrics
- **👨‍💻 Developer-First:** Rich APIs (GraphQL, OpenAPI), Python + Java SDKs, CLI tools
- **🏢 Enterprise Ready:** Battle-tested security, authentication, authorization, and audit trails
- **🌍 Open Source:** Apache 2.0 licensed, vendor-neutral, community-driven

---

## 🧠 The Context Foundation

Essential for modern data teams and reliable AI agents:

- **[Context Management Is the Missing Piece in the Agentic AI Puzzle](https://datahub.com/blog/context-management-is-the-missing-piece-in-the-agentic-ai-puzzle/)** - Why context management is essential for deploying reliable AI agents at scale
- **[Data Lineage: What It Is and Why It Matters](https://datahub.com/blog/data-lineage-what-it-is-and-why-it-matters/)** - Understanding the map of how data flows through your organization
- **[What is Metadata Management?](https://datahub.com/blog/what-is-metadata-management/)** - A comprehensive guide for enterprise data leaders

---

## 📑 Table of Contents

- [FAQ](#-frequently-asked-questions)
- [See DataHub in Action](#-see-datahub-in-action)
- [Quick Start](#-quick-start-60-seconds)
- [Installation Options](#-installation-options)
- [Architecture](#-architecture-overview)
- [Use Cases & Examples](#-use-cases--examples)
- [Trusted By](#-trusted-by-industry-leaders)
- [Ecosystem](#-datahub-ecosystem)
- [Community](#-community--support)
- [Contributing](#-contributing)
- [Resources](#-resources--learning)
- [License](#-license)

---

## ❓ Frequently Asked Questions

<details>
<summary><b>Is this the same project as datahub.io?</b></summary>

No. [datahub.io](https://datahub.io) is a completely separate project — a public dataset hosting service with no affiliation to this project. DataHub (this project) is an open-source metadata platform for data discovery, governance, and observability, hosted at [datahub.com](https://datahub.com) and developed at [github.com/datahub-project/datahub](https://github.com/datahub-project/datahub).

</details>

<details>
<summary><b>What happened to datahubproject.io?</b></summary>

DataHub was previously hosted at `datahubproject.io`. That domain now redirects to [datahub.com](https://datahub.com). All documentation has moved to [docs.datahub.com](https://docs.datahub.com/docs/quickstart). If you find references to `datahubproject.io` in blog posts or tutorials, they refer to this same project — just under its former domain.

</details>

<details>
<summary><b>Is DataHub related to LinkedIn's internal DataHub?</b></summary>

Yes. DataHub was originally built at LinkedIn to manage metadata at scale across their data ecosystem. LinkedIn open-sourced DataHub in 2020. It has since grown into an independent community project under the [datahub-project](https://github.com/datahub-project) GitHub organization, now hosted at [datahub.com](https://datahub.com).

</details>

<details>
<summary><b>How do I install the DataHub metadata platform?</b></summary>

```bash
pip install acryl-datahub
datahub docker quickstart
```

See the [Quick Start](#-quick-start-60-seconds) section below for full instructions. The PyPI package is [`acryl-datahub`](https://pypi.org/project/acryl-datahub/).

</details>

---

## 🎨 See DataHub in Action

<table>
  <tr>
    <td width="50%">
      <img src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/search/search-results-page.png" alt="Universal Search" width="100%"/>
      <p align="center"><b>🔍 Universal Search</b><br/>Find any data asset instantly across your entire stack</p>
    </td>
    <td width="50%">
      <img src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/lineage/column-level-lineage-v3.png" alt="Column-Level Lineage" width="100%"/>
      <p align="center"><b>📊 Column-Level Lineage</b><br/>Trace data flow from source to consumption</p>
    </td>
  </tr>
  <tr>
    <td width="50%">
      <img src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/feature-dataset-stats.png" alt="Rich Dataset Profiles" width="100%"/>
      <p align="center"><b>📋 Rich Dataset Profiles</b><br/>Schema, statistics, documentation, and ownership</p>
    </td>
    <td width="50%">
      <img src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/feature-tags-terms-domains.png" alt="Governance Dashboard" width="100%"/>
      <p align="center"><b>🏛️ Governance Dashboard</b><br/>Manage policies, tags, and compliance</p>
    </td>
  </tr>
</table>

**▶️ Watch DataHub in Action:**

- [5-Minute Product Tour](https://www.youtube.com/channel/UC3qFQC5IiwR5fvWEqi_tJ5w) (YouTube)
- [Try Live Demo](https://demo.datahub.com) (No installation required)

---

## 🚀 Quick Start

### Option 1: Try the Hosted Demo (Fastest)

No installation required. Explore a fully-loaded DataHub instance with sample data instantly:

**🌐 [Launch Live Demo: demo.datahub.com](https://demo.datahub.com)**

### Option 2: Run Locally with Python (Recommended)

Get DataHub running on your machine in under 2 minutes:

```bash
# Prerequisites: Docker Desktop with 8GB+ RAM allocated

# Upgrade pip and install DataHub CLI
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub

# Launch DataHub locally via Docker
datahub docker quickstart

# Access DataHub at http://localhost:9002
# Default credentials: datahub / datahub
```

**Note:** You can also use `uv` or other Python package managers instead of pip.

**What's included:**

- ✅ **Full Stack:** GMS backend, React UI, Elasticsearch, MySQL, and Kafka.
- ✅ **Sample Data:** Pre-loaded datasets, lineage, and owners for exploration.
- ✅ **Ingestion Ready:** Fully prepared to connect your own local or cloud data sources.

### Option 3: Run from Source (For Contributors)

Best for advanced users who want to modify the core codebase or run directly from the repository:

```bash
# Clone the repository
git clone https://github.com/datahub-project/datahub.git
cd datahub

# Start all services with docker-compose
./docker/quickstart.sh

# Access DataHub at http://localhost:9002
# Default credentials: datahub / datahub
```

### Next Steps

- **🔌 Connect Your Data:** Explore our [Ingestion Guides](https://docs.datahub.com/docs/metadata-ingestion) for Snowflake, BigQuery, dbt, and more.
- **📚 Learn the Basics:** Walk through the [Getting Started Guide](https://docs.datahub.com/docs/quickstart)
- **🎓 DataHub Academy:** Deep dive with our [Advanced Tutorials](https://docs.datahub.com/docs/quickstart)

---

## 📦 Installation Options

DataHub supports three deployment models:

- **[Managed SaaS (DataHub Cloud)](https://datahub.com/get-datahub-cloud/)** — zero infrastructure, SLA-backed, enterprise-ready
- **[Self-hosted via Docker](https://docs.datahub.com/docs/quickstart)** — ideal for development and small teams
- **[Kubernetes (Helm)](docs/deploy/kubernetes.md)** — recommended for production self-hosted deployments

**→ [See all deployment guides (AWS, Azure, GCP, environment variables)](docs/deploy/)**

---

## 🏗️ Architecture Overview

- ✅ **Streaming-First:** Real-time metadata updates via Kafka
- ✅ **API-First:** All features accessible via APIs
- ✅ **Extensible:** Plugin architecture for custom entity types
- ✅ **Scalable:** Proven to 10M+ assets and O(1B) relationships at LinkedIn and other companies in production
- ✅ **Cloud-Native:** Designed for Kubernetes deployment

**→ [Full architecture breakdown: components, storage layer, APIs, and design decisions](docs/architecture/architecture.md)**

---

## 💻 Use Cases & Examples

<details>
<summary><b>Example 1: Ingest Metadata from Snowflake</b></summary>

**Use Case:** Extract table metadata, column schemas, and usage statistics from Snowflake data warehouse.

**Prerequisites:**

- DataHub instance running (local or remote)
- Snowflake account with read permissions
- DataHub CLI installed (`pip install 'acryl-datahub[snowflake]'`)

```yaml
# snowflake_recipe.yml
source:
  type: snowflake
  config:
    # Connection details
    account_id: "xy12345.us-east-1"
    warehouse: "COMPUTE_WH"
    username: "${SNOWFLAKE_USER}"
    password: "${SNOWFLAKE_PASSWORD}"

    # Optional: Filter specific databases
    database_pattern:
      allow:
        - "ANALYTICS_DB"
        - "MARKETING_DB"

sink:
  type: datahub-rest
  config:
    server: "http://localhost:8080"
```

```bash
# Run ingestion
datahub ingest -c snowflake_recipe.yml

# Expected output:
# ✓ Connecting to Snowflake...
# ✓ Discovered 150 tables in ANALYTICS_DB
# ✓ Discovered 75 tables in MARKETING_DB
# ✓ Ingesting metadata...
# ✓ Successfully ingested 225 datasets to DataHub
```

**What gets ingested:**

- Table and view schemas (columns, data types, descriptions)
- Table statistics (row counts, size, last modified)
- Lineage information (upstream/downstream tables)
- Usage statistics (query frequency, top users)

</details>

---

<details>
<summary><b>Example 2: Search for Datasets via Python SDK</b></summary>

**Use Case:** Programmatically search DataHub catalog and retrieve dataset metadata.

**Prerequisites:**

- DataHub instance accessible
- Python 3.8+ installed
- DataHub Python package installed (`pip install 'acryl-datahub[datahub-rest]'`)

```python
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph

# Initialize DataHub client
config = DatahubClientConfig(server="http://localhost:8080")
graph = DataHubGraph(config)

# Search for datasets containing "customer"
# Returns up to 10 most relevant results
results = graph.search(
    entity="dataset",
    query="customer",
    count=10
)

# Process and display results
for result in results:
    print(f"Found: {result.entity.urn}")
    print(f"  Name: {result.entity.name}")
    print(f"  Platform: {result.entity.platform}")
    print(f"  Description: {result.entity.properties.description}")
    print("---")

# Example output:
# Found: urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customer_profiles,PROD)
#   Name: customer_profiles
#   Platform: snowflake
#   Description: Aggregated customer data from CRM and transactions
# ---
```

**Response format:** Each result contains:

- `urn`: Unique resource identifier for the dataset
- `name`: Human-readable dataset name
- `platform`: Source platform (snowflake, bigquery, etc.)
- `properties`: Additional metadata (description, tags, owners, etc.)

</details>

---

<details>
<summary><b>Example 3: Query Lineage via GraphQL</b></summary>

**Use Case:** Retrieve upstream and downstream dependencies for a specific dataset.

**Prerequisites:**

- DataHub GMS endpoint accessible
- Dataset URN available from search or ingestion

**GraphQL Query:**

```graphql
query GetLineage {
  dataset(
    urn: "urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customer_profiles,PROD)"
  ) {
    # Get upstream dependencies (source tables)
    upstream: lineage(input: { direction: UPSTREAM }) {
      entities {
        urn
        ... on Dataset {
          name
          platform {
            name
          }
        }
      }
    }

    # Get downstream dependencies (consuming tables/dashboards)
    downstream: lineage(input: { direction: DOWNSTREAM }) {
      entities {
        urn
        type
        ... on Dataset {
          name
          platform {
            name
          }
        }
        ... on Dashboard {
          dashboardId
          tool
        }
      }
    }
  }
}
```

**Execute via cURL:**

```bash
curl -X POST http://localhost:8080/api/graphql \
  -H "Content-Type: application/json" \
  -d '{"query": "query GetLineage { ... }"}'
```

**Response structure:**

- `upstream`: Array of datasets that feed into this dataset
- `downstream`: Array of datasets, dashboards, or ML models that consume this dataset
- Each entity includes URN, type, and basic metadata

</details>

---

<details>
<summary><b>Example 4: Add Documentation via Python API</b></summary>

**Use Case:** Programmatically add or update dataset documentation and custom properties.

**Prerequisites:**

- DataHub Python SDK installed
- Write permissions to DataHub instance
- Dataset already exists in DataHub (from ingestion)

```python
from datahub.metadata.schema_classes import DatasetPropertiesClass
from datahub.emitter.mce_builder import make_dataset_urn
from datahub.emitter.rest_emitter import DatahubRestEmitter

# Create emitter to send metadata to DataHub
emitter = DatahubRestEmitter("http://localhost:8080")

# Create dataset URN (unique identifier)
dataset_urn = make_dataset_urn(
    platform="snowflake",
    name="analytics.customer_profiles",
    env="PROD"
)

# Define dataset properties
properties = DatasetPropertiesClass(
    description="""
    Customer profiles aggregated from CRM and transaction data.

    **Update Schedule:** Updated nightly via Airflow DAG `customer_profile_etl`
    **Data Retention:** 7 years for compliance
    **Owner:** Data Platform Team
    """,
    customProperties={
        "owner_team": "data-platform",
        "update_frequency": "daily",
        "data_sensitivity": "PII",
        "upstream_dag": "customer_profile_etl",
        "business_domain": "customer_analytics"
    }
)

# Emit metadata to DataHub
emitter.emit_mcp(
    entityUrn=dataset_urn,
    aspectName="datasetProperties",
    aspect=properties
)

print(f"✓ Successfully updated documentation for {dataset_urn}")
```

**What this does:**

1. Adds rich markdown documentation visible in DataHub UI
2. Sets custom properties for governance and discovery
3. Makes dataset searchable by custom property values
4. Enables filtered searches (e.g., "show me all PII datasets")

</details>

---

<details>
<summary><b>Example 5: Connect AI Coding Assistants via Model Context Protocol</b></summary>

**Use Case:** Enable AI agents (Cursor, Claude Desktop, Cline) to query DataHub metadata directly from your IDE or development environment.

**Prerequisites:**

- DataHub instance running and accessible
- MCP-compatible AI tool installed (Cursor, Claude Desktop, Cline, etc.)
- Node.js 18+ installed

**Quick Setup:**

```bash
# Initialize MCP server for DataHub
npx -y @acryldata/mcp-server-datahub init

# Follow the interactive prompts to configure:
# - DataHub GMS endpoint (e.g., http://localhost:8080)
# - Authentication token (if required)
# - MCP server settings
```

**Configure your AI tool:**

For **Claude Desktop**, add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "datahub": {
      "command": "npx",
      "args": ["-y", "@acryldata/mcp-server-datahub"]
    }
  }
}
```

For **Cursor**, configure in Settings → Features → MCP Servers

**What you can ask your AI:**

- _"What datasets contain customer PII in production?"_
- _"Show me the lineage for analytics.revenue_table"_
- _"Who owns the 'Revenue Dashboard' in Looker?"_
- _"Find all datasets in the marketing domain"_
- _"What's the schema for user_events table?"_
- _"List datasets tagged as 'critical' or 'sensitive'"_

**Example conversation:**

```
You: "What datasets are owned by the data-platform team?"

AI: Based on DataHub metadata, here are the datasets owned by data-platform:
- urn:li:dataset:(urn:li:dataPlatform:snowflake,analytics.customer_profiles,PROD)
  Name: customer_profiles
  Platform: Snowflake
  Description: Aggregated customer data from CRM and transactions

- urn:li:dataset:(urn:li:dataPlatform:bigquery,marketing.campaign_performance,PROD)
  Name: campaign_performance
  Platform: BigQuery
  Description: Marketing campaign metrics and ROI tracking

[... more results]
```

**Benefits:**

- ✅ Query metadata without leaving your IDE
- ✅ Natural language interface (no SQL/GraphQL needed)
- ✅ Real-time access to DataHub's metadata graph
- ✅ Understand data context while coding
- ✅ Discover relevant datasets for your task

📖 **Full Documentation:** [MCP Server for DataHub](https://github.com/acryldata/mcp-server-datahub)

</details>

---

### Common Use Cases

| Use Case               | Description                                         | Learn More                                                                            |
| ---------------------- | --------------------------------------------------- | ------------------------------------------------------------------------------------- |
| 🔍 **Data Discovery**  | Help users find the right data for analytics and ML | [Guide](https://docs.datahub.com/docs/features)                                       |
| 📊 **Impact Analysis** | Understand downstream impact before making changes  | [Lineage Docs](https://docs.datahub.com/docs/features/feature-guides/lineage)         |
| 🏛️ **Data Governance** | Enforce policies, classify PII, manage access       | [Governance Guide](https://docs.datahub.com/docs/authorization/access-policies-guide) |
| 🔔 **Data Quality**    | Monitor freshness, volumes, schema changes          | [Quality Checks](https://docs.datahub.com/docs/api/tutorials/assertions)              |
| 📚 **Documentation**   | Centralize data documentation and knowledge         | [Docs Features](https://docs.datahub.com/docs/features)                               |
| 👥 **Collaboration**   | Foster data culture with discussions and ownership  | [Collaboration](https://docs.datahub.com/docs/features)                               |

---

## 📝 DataHub in Action

Learn from teams using DataHub in production and get practical guidance:

<table>
  <tr>
    <td width="33%">
      <h3><a href="https://datahub.com/blog/metadata-in-action-tips-and-tricks-from-the-field/">🏆 Best Practices from the Field</a></h3>
      <p>Real-world metadata strategies from teams at Grab, Slack, and Checkout.com who manage data at scale.</p>
      <sub><i>Case Studies</i></sub>
    </td>
    <td width="33%">
      <h3><a href="https://datahub.com/blog/the-what-why-and-how-of-data-contracts/">📋 Data Contracts: How to Use Them</a></h3>
      <p>Practical guide to implementing data contracts between producers and consumers for quality and accountability.</p>
      <sub><i>Implementation Guide</i></sub>
    </td>
    <td width="33%">
      <h3><a href="https://datahub.com/blog/datahub-mcp-server-block-ai-agents-use-case/">🤖 How Block Powers AI Agents with DataHub</a></h3>
      <p>Real-world case study: scaling data governance and AI operations across 50+ platforms using MCP.</p>
      <sub><i>AI Case Study</i></sub>
    </td>
  </tr>
</table>

<p align="center">
  <a href="https://datahub.com/blog/"><b>→ Explore all posts on our blog</b></a>
</p>

---

## 🏢 Trusted by Industry Leaders

**3,000+ organizations** run DataHub in production worldwide — across both open-source deployments and DataHub Cloud — from hyperscale tech companies to regulated financial institutions and healthcare providers.

### By Industry

**🛒 E-Commerce & Retail:** Etsy • Experius • Klarna • LinkedIn • MediaMarkt Saturn • Uphold • Wealthsimple • Wolt

**🏥 Healthcare & Life Sciences:** CVS Health • IOMED • Optum

**✈️ Travel & Transportation:** Cabify • DFDS • Expedia Group • Hurb • Peloton • Viasat

**📚 Education & EdTech:** ClassDojo • Coursera • Udemy

**💰 Financial Services:** Banksalad • Block • Chime • FIS • Funding Circle • GEICO • Inter&Co • N26 • Santander • Shanghai HuaRui Bank • Stash • Visa

**🎮 Gaming, Entertainment & Streaming:** Netflix • Razer • Showroomprive • TypeForm • UKEN Games • Zynga

**🚀 Technology & SaaS:** Adevinta • Apple • Digital Turbine • DPG Media • Foursquare • Geotab • HashiCorp • hipages • inovex • KPN • Miro • MYOB • Notion • Okta • Rippling • Saxo Bank • Slack • ThoughtWorks • Twilio • Wikimedia • WP Engine

**📊 Data & Analytics:** ABLY • DefinedCrowd • Grofers • Haibo Technology • Moloco • PITS Global Data Recovery Services • SpotHero

_And thousands more across DataHub Core and DataHub Cloud._

### Featured Case Studies

- 📰 **Optum:** [Data Mesh via DataHub](https://datahub.com/customer-stories/optum/)
- 🏦 **Saxo Bank:** [Enabling Data Discovery in Data Mesh](https://medium.com/datahub-project/enabling-data-discovery-in-a-data-mesh-the-saxo-journey-451b06969c8f)
- 🚗 **SpotHero:** [Data Discoverability at Scale](https://www.slideshare.net/MaggieHays/data-discoverability-at-spothero)

**Using DataHub?** Please feel free to add your organization to the list if we missed it — open a [PR](https://github.com/datahub-project/datahub/pulls) or let us know on [Slack](https://datahub.com/slack).

---

## 🌐 DataHub Ecosystem

DataHub is part of a rich ecosystem of tools and integrations.

### Official Repositories

| Repository                                                            | Description                                                     | Links                                            |
| --------------------------------------------------------------------- | --------------------------------------------------------------- | ------------------------------------------------ |
| **[datahub](https://github.com/datahub-project/datahub)**             | Core platform: metadata model, services, connectors, and web UI | [Docs](https://docs.datahub.com/docs/quickstart) |
| **[datahub-actions](https://github.com/acryldata/datahub-actions)**   | Framework for responding to metadata changes in real-time       | [Guide](https://docs.datahub.com/docs/actions)   |
| **[datahub-helm](https://github.com/acryldata/datahub-helm)**         | Production-ready Helm charts for Kubernetes deployment          | [Charts](https://helm.datahubproject.io/)        |
| **[static-assets](https://github.com/datahub-project/static-assets)** | Logos, images, and brand assets for DataHub                     | -                                                |

### Community Plugins & Integrations

| Project                                                                                         | Description                                      | Maintainer |
| ----------------------------------------------------------------------------------------------- | ------------------------------------------------ | ---------- |
| **[datahub-tools](https://github.com/makenotion/datahub-tools)**                                | Python tools for GraphQL endpoint interaction    | Notion     |
| **[dbt-impact-action](https://github.com/acryldata/dbt-impact-action)**                         | GitHub Action for dbt change impact analysis     | Acryl Data |
| **[business-glossary-sync-action](https://github.com/acryldata/business-glossary-sync-action)** | Sync business glossary via GitHub PRs            | Acryl Data |
| **[mcp-server-datahub](https://github.com/acryldata/mcp-server-datahub)**                       | Model Context Protocol server for AI integration | Acryl Data |
| **[meta-world](https://github.com/acryldata/meta-world)**                                       | Recipes, custom sources, and transformations     | Community  |

### Integrations by Category

**📊 BI & Analytics:** Tableau • Looker • Power BI • Superset • Metabase • Mode • Redash

**🗄️ Data Warehouses:** Snowflake • BigQuery • Redshift • Databricks • Synapse • ClickHouse

**🔄 Data Orchestration:** Airflow • dbt • Dagster • Prefect • Luigi

**🤖 ML Platforms:** SageMaker • MLflow • Feast • Kubeflow • Weights & Biases

**🔗 Data Integration:** Fivetran • Airbyte • Stitch • Matillion

[View all 80+ integrations →](https://docs.datahub.com/integrations)

---

## 💬 Community & Support

Join thousands of data practitioners building with DataHub!

### 🗓️ Town Halls

**Next Town Hall:**

- 🎟️ [Register for the next Town Hall](https://luma.com/zp3h4ex8)

**Last Town Hall:**

- 📺 [Powering AI Agents with DataHub Context](https://youtu.be/dqZNV09yvA0?si=IWUKhLm0Xa_PoYsy) (January 2026)

[→ View all past recordings](https://www.youtube.com/playlist?list=PLdCtLs64vZvHTXGqybmOfyxXbGDn2Reb9)

### 💬 Get Help & Connect

| Channel                | Purpose                                  | Link                                                                         |
| ---------------------- | ---------------------------------------- | ---------------------------------------------------------------------------- |
| **Slack Community**    | Real-time chat, questions, announcements | [Join 14,000+ members](https://datahub.com/slack)                            |
| **GitHub Discussions** | Technical discussions, feature requests  | [Start a Discussion](https://github.com/datahub-project/datahub/discussions) |
| **GitHub Issues**      | Bug reports, feature requests            | [Open an Issue](https://github.com/datahub-project/datahub/issues)           |
| **Stack Overflow**     | Technical Q&A (tag: `datahub`)           | [Ask a Question](https://stackoverflow.com/questions/tagged/datahub)         |
| **YouTube**            | Tutorials, demos, talks                  | [Subscribe](https://www.youtube.com/@datahubproject)                         |
| **LinkedIn**           | Company updates, blogs                   | [Follow Us](https://linkedin.com/company/datahubproject)                     |
| **Twitter/X**          | Quick updates, community highlights      | [Follow @datahubproject](https://twitter.com/datahubproject)                 |

### 📧 Stay Updated

- 📝 [Read the Blog](https://datahub.com/blog/) - Deep dives and case studies
- 📖 [Monthly Release Notes](https://docs.datahub.com/docs/releases) - What's new

### 🎓 Learning Resources

- **[DataHub Quickstart](https://docs.datahub.com/docs/quickstart)** - Get started in 15 minutes
- **[API Documentation](https://docs.datahub.com/docs/api/datahub-apis)** - GraphQL & REST API reference
- **[Architecture Guide](https://docs.datahub.com/docs/architecture/architecture)** - Deep dive into internals
- **[Video Tutorials](https://www.youtube.com/@datahubproject)** - Step-by-step guides

---

## 🤝 Contributing

We ❤️ contributions from the community! See **[CONTRIBUTING.md](docs/CONTRIBUTING.md)** for setup, guidelines, and ways to get involved.

Browse [Good First Issues](https://github.com/datahub-project/datahub/labels/good-first-issue) to get started!

---

## 📚 Resources & Learning

### 📰 Featured Content

**Blog Posts & Articles:**

- [DataHub: Popular Metadata Architectures Explained](https://engineering.linkedin.com/blog/2020/datahub-popular-metadata-architectures-explained) - LinkedIn Engineering
- [Open Sourcing DataHub](https://engineering.linkedin.com/blog/2020/open-sourcing-datahub--linkedins-metadata-search-and-discovery-p) - LinkedIn Engineering
- [Enabling Data Discovery in Data Mesh](https://medium.com/datahub-project/enabling-data-discovery-in-a-data-mesh-the-saxo-journey-451b06969c8f) - Saxo Bank
- [Data Discoverability at SpotHero](https://www.slideshare.net/MaggieHays/data-discoverability-at-spothero) - SpotHero
- [Emerging Architectures for Modern Data Infrastructure](https://future.com/emerging-architectures-for-modern-data-infrastructure-2020/) - a16z

**Conference Talks:**

- [The Evolution of Metadata: LinkedIn's Journey](https://speakerdeck.com/shirshanka/the-evolution-of-metadata-linkedins-journey-strata-nyc-2019) - Strata 2019
- [Driving DataOps Culture with DataHub](https://www.youtube.com/watch?v=ccsIKK9nVxk) - DataOps Unleashed 2021
- [Journey of Metadata at LinkedIn](https://www.youtube.com/watch?v=OB-O0Y6OYDE) - Crunch Conference 2019
- [DataHub Journey with Expedia Group](https://www.youtube.com/watch?v=ajcRdB22s5o) - Expedia

**Podcasts:**

- [Bringing The Power Of The Real-Time Metadata Graph To Everyone](https://www.dataengineeringpodcast.com/acryl-data-datahub-metadata-graph-episode-230/) - Data Engineering Podcast

### 🔗 Important Links

| Resource                  | URL                                                |
| ------------------------- | -------------------------------------------------- |
| 📖 Official Documentation | https://docs.datahub.com                           |
| 🏠 Project Website        | https://datahub.com                                |
| 🌐 Live Demo              | https://demo.datahub.com                           |
| 📊 Roadmap                | https://feature-requests.datahubproject.io/roadmap |
| 🗓️ Town Hall Schedule     | https://docs.datahub.com/docs/townhalls            |
| 💬 Slack Community        | https://datahub.com/slack                          |
| 📺 YouTube Channel        | https://youtube.com/@datahubproject                |
| 📝 Blog                   | https://datahub.com/blog/                          |
| 🔗 LinkedIn               | https://www.linkedin.com/company/72009941          |
| 🐦 Twitter/X              | https://twitter.com/datahubproject                 |
| 🔒 Security               | https://docs.datahub.com/docs/security             |

---

## 📄 License

DataHub is open source software released under the **[Apache License 2.0](https://github.com/datahub-project/datahub/blob/master/LICENSE)**.

```
Copyright 2015-2025 LinkedIn Corporation
Copyright 2025-Present DataHub Project Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
```

**What this means:**

- ✅ Commercial use allowed
- ✅ Modification allowed
- ✅ Distribution allowed
- ✅ Patent use allowed
- ✅ Private use allowed

**Learn more:** [Choose a License - Apache 2.0](https://choosealicense.com/licenses/apache-2.0/)

---

<p align="center">
  <b>⭐ If you find DataHub useful, please star the repository! ⭐</b>
</p>

<p align="center">
  Made with ❤️ by the DataHub community
</p>
