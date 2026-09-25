---
description: "Overview of DataHub features for data discovery, lineage, profiling, governance, contracts, and collaborative metadata management."
hide_title: true
slug: /features
---

import FeatureCardSection from '@site/src/pages/docs/\_components/FeatureCardSection';
import QuickstartCTA from '@site/src/pages/docs/\_components/QuickstartCTA';

# What is DataHub?

DataHub is a modern data catalog designed to streamline metadata management, data discovery, and data governance. It enables users to efficiently explore and understand their data, track data lineage, profile datasets, and establish data contracts.
This extensible metadata management platform is built for developers to tame the complexity of their rapidly evolving data ecosystems and for data practitioners to leverage the total value of data within their organization.

<p align="center">
    <img 
        alt="DataHub Integrations" 
        src="/img/diagrams/datahub-flow-diagram-light.png" 
        style={{ padding: "2rem" }} 
    />
</p>

## Quickstart

<QuickstartCTA/>

## Key Features

<FeatureCardSection/>

## See DataHub in Action

<p align="center">
  <a href="https://demo.datahub.com">
    <img width="90%" src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/demos/datahub-tour.gif" alt="DataHub Product Tour" />
  </a>
</p>

<p align="center">
  <i>Search, discover, and understand your data with DataHub's unified metadata platform</i>
</p>

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

- [YouTube Channel](https://www.youtube.com/@DataHubCloud)
- [Try Live Demo](https://demo.datahub.com) (No installation required)

## Why DataHub?

**The Challenge:** Modern data stacks are fragmented across dozens of tools—warehouses, lakes, BI platforms, ML systems, AI agents, orchestration engines. Finding the right data, understanding its lineage, and ensuring governance is like searching through a maze blindfolded.

**The DataHub Solution:** DataHub acts as the central nervous system for your data stack—connecting all your tools through real-time streaming or batch ingestion to create a unified metadata graph. Unlike static catalogs, DataHub keeps your metadata fresh and actionable—powering both human teams and AI agents.

<p align="center">
  <img width="90%" src="https://raw.githubusercontent.com/datahub-project/static-assets/refs/heads/main/imgs/datahub_for_human_and_ai.png" alt="DataHub for Humans and AI" />
</p>

- **🚀 Battle-Tested at Scale:** Born at LinkedIn to handle hyperscale data, now proven at thousands of organizations worldwide managing millions of data assets
- **⚡ Real-Time Streaming:** Metadata updates in seconds, not hours or days
- **🤖 AI-Ready:** Native support for AI agents via MCP, LLM integrations, and context management
- **🔌 Pioneering Ingestion Architecture:** Flexible push/pull framework (widely adopted by other catalogs) with 150+ production-grade integrations extracting deep metadata—column lineage, usage stats, profiling, and quality metrics
- **👨‍💻 Developer-First:** Rich APIs (GraphQL, OpenAPI), Python + Java SDKs, CLI tools
- **🏢 Enterprise Ready:** Battle-tested security, authentication, authorization, and audit trails
- **🌍 Open Source:** Apache 2.0 licensed, vendor-neutral, community-driven

## Common Use Cases

| Use Case               | Description                                         | Learn More                                                 |
| ---------------------- | --------------------------------------------------- | ---------------------------------------------------------- |
| 🔍 **Data Discovery**  | Help users find the right data for analytics and ML | [Search Guide](how/search.md)                              |
| 📊 **Impact Analysis** | Understand downstream impact before making changes  | [Lineage Docs](features/feature-guides/lineage.md)         |
| 🏛️ **Data Governance** | Enforce policies, classify PII, manage access       | [Governance Guide](authorization/access-policies-guide.md) |
| 🔔 **Data Quality**    | Monitor freshness, volumes, schema changes          | [Quality Checks](api/tutorials/assertions.md)              |
| 📚 **Documentation**   | Centralize data documentation and knowledge         | [Documentation Guide](api/tutorials/descriptions.md)       |
| 👥 **Collaboration**   | Foster data culture with discussions and ownership  | [Posts & Announcements](posts.md)                          |

## Get Started

### Deployment

To get started with DataHub, you can use a simple CLI command. Alternatively, you can deploy the instance to production using Docker or Helm charts.

- [Quickstart](quickstart.md)
- [Self-hosting DataHub with Kubernetes](deploy/kubernetes.md)
- [DataHub Cloud](managed-datahub/managed-datahub-overview.md)

### Ingestion

DataHub supports ingestion by UI and CLI.

- [UI-based Ingestion](ui-ingestion.md)
- [CLI-based Ingestion](../metadata-ingestion/cli-ingestion.md)

## Join the Community

For additional information and assistance, feel free to visit one of these channels!

- [Slack](https://datahubspace.slack.com)
- [Blog](https://medium.com/datahub-project/)
- [LinkedIn](https://www.linkedin.com/company/acryl-data/)
- Our champions - [Data Practitioners Guild](https://datahub.com/guild/) & [DataHub Champions](https://datahub.com/champions)

See also: [Community & Support](community.md) · [FAQ](faq.md) · [Ecosystem](ecosystem.md) · [Adopters](../ADOPTERS.md)
