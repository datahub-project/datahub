
# OSS vs Cloud: Comparison Guide

This guide compares DataHub Open Source (OSS) and DataHub Cloud features and platform differences. DataHub Cloud builds on the OSS foundation with enterprise-grade capabilities including AI automation, advanced governance, operational reliability, and production support for mid-to-large organizations. Cloud also offers a fully managed service with 99.5%+ SLA-backed availability, dedicated support, enhanced security, training services, and flexible deployment options.

## Discovery & Search

| Feature Name                                  | OSS | Cloud | Business Value                                                                                                                                            |                                                    Link                                                    |
| :-------------------------------------------- | :-: | :---: | :-------------------------------------------------------------------------------------------------------------------------------------------------------- | :--------------------------------------------------------------------------------------------------------: |
| **70+ Source Connectors with Unified Search** | ✔  |  ✔   | Connect entire data ecosystem                                                                                                                             |                               [Docs](/integrations)                                |
| **Ask DataHub AI Agent**                      | ❌  |  ✔   | <ul><li>Find trustworthy data metrics</li><li>Generate Accurate SQL</li><li>Debug data quality issues</li><li>Understand impact of data changes</li></ul> |                 [Docs](/docs/features/feature-guides/ask-datahub)                  |
| **DataHub Hosted MCP Server**                 | ❌  |  ✔   | Connect AI tools directly to your data catalog                                                                                                            |                     [Docs](/docs/features/feature-guides/mcp)                      |
| **Enhanced Usage-Aware Search Ranking**       | ❌  |  ✔   | Surface most relevant data first                                                                                                                          |              [Docs](/docs/how/search#example-1-ranking-by-tagsterms)               |
| **Column-Level Lineage & Impact Analysis**    | ✔  |  ✔   | Understand data dependencies                                                                                                                              |                   [Docs](/docs/features/feature-guides/lineage)                    |
| **Lineage-Based Propagation**                 | ❌  |  ✔   | Auto-enrich downstream datasets                                                                                                                           |              [Docs](/docs/automations/docs-propagation#introduction)               |
| **Context Documents**                         | ✔  |  ✔   | Create & semantically search across unstructured docs                                                                                                     |          [Docs](/docs/features/feature-guides/context/context-documents)           |
| **AI Documentation Generation**               | ❌  |  ✔   | Auto-document tables & columns                                                                                                                            |                         [Docs](/docs/automations/ai-docs)                          |
| **Personalized Home and Asset Views**         | ❌  |  ✔   | Customize home page and asset summaries for a personalized data experience                                                                                |    [Docs](/docs/features/feature-guides/custom-asset-summaries#custom-modules)     |
| **Multi-Channel Notifications**               | ❌  |  ✔   | Stay informed where you work (Email, Slack, & Teams)                                                                                                      | [Docs](/docs/incidents/incidents/#enabling-slack-notifications-datahub-cloud-only) |

## Data Observability

| Feature Name                                                         | OSS | Cloud | Business Value                                                                                      |                                                           Link                                                            |
| :------------------------------------------------------------------- | :-: | :---: | :-------------------------------------------------------------------------------------------------- | :-----------------------------------------------------------------------------------------------------------------------: |
| **Observe AI Agent**                                                 | ❌  |  ✔   | Seamlessly sets up the right checks, troubleshoots broken data, and prepares health reports for you | [Docs](/docs/managed-datahub/observe/data-health-dashboard#data-observability-agent-private-beta) |
| **AI Anomaly Detection for Assertions**                              | ❌  |  ✔   | Catch issues automatically                                                                          |                      [Docs](/docs/managed-datahub/observe/anomaly-detection)                      |
| **Freshness, Volume, Schema & Column Monitoring, Custom SQL Checks** | ❌  |  ✔   | Ensure timely data                                                                                  |                    [Docs](/docs/managed-datahub/observe/freshness-assertions)                     |
| **Data Health Dashboard**                                            | ❌  |  ✔   | Quality overview at scale                                                                           |                    [Docs](/docs/managed-datahub/observe/data-health-dashboard)                    |
| **Notifications for Data Assertions**                                | ❌  |  ✔   | Real-time quality alerts                                                                            |                    [Docs](/docs/managed-datahub/subscription-and-notification)                    |
| **Secure In-VPC Quality Validation**                                 | ❌  |  ✔   | Metadata never leaves your network                                                                  |                                                                                                                           |
| **Pipeline Circuit Breakers (API)**                                  | ❌  |  ✔   | Validate data quality programmatically before reads or writes                                       |                      [Docs](/docs/managed-datahub/observe/data-contract#api)                      |
| **Data Contracts**                                                   | ✔  |  ✔   | Define quality expectations                                                                         |            [Docs](/docs/managed-datahub/observe/data-contract#what-is-a-data-contract)            |
| **Quality & Health Status on Asset Profiles**                        | ✔  |  ✔   | See quality at a glance                                                                             |                                                                                                                           |

## Data Governance

| Feature Name                                                                     | OSS | Cloud | Business Value                |                                                   Link                                                   |
| :------------------------------------------------------------------------------- | :-: | :---: | :---------------------------- | :------------------------------------------------------------------------------------------------------: |
| **Data Ownership Management**                                                    | ✔  |  ✔   | Clear accountability          |    [Docs](/docs/metadata-integration/java/docs/sdk-v2/dataset-entity#owners)     |
| **Business Glossary**                                                            | ✔  |  ✔   | Common data language          |                         [Docs](/learn/business-glossary)                         |
| **Bi-Directional Metadata Sync**                                                 | ❌  |  ✔   | Keep metadata current         |                 [Docs](/docs/automations/bigquery-metadata-sync)                 |
| **Compliance Forms and Workflow Engine**                                         | ❌  |  ✔   | Track regulatory compliance   |         [Docs](/docs/features/feature-guides/compliance-forms/analytics)         |
| **Metadata Tests**                                                               | ❌  |  ✔   | Validate governance rules     |                        [Docs](/docs/tests/metadata-tests)                        |
| **Approval Workflows: Documentation, Glossary, Tags, Terms, and Data Ownership** | ❌  |  ✔   | Controlled vocabulary changes | [Docs](/docs/managed-datahub/change-proposals#proposing-tags-and-glossary-terms) |
| **Access Request Workflows**                                                     | ❌  |  ✔   | Self-service data access      | [Docs](/docs/managed-datahub/workflows/access-workflows#faq-and-troubleshooting) |

## Enterprise & Security

| Feature Name                      | OSS Available | Cloud Available | Business Value          |
| :-------------------------------- | :-----------: | :-------------: | :---------------------- |
| **99.5% Uptime SLA**              |      ❌       |       ✔        | Guaranteed availability |
| **Fine-grained Access Control**   |      ❌       |       ✔        | Secure by default       |
| **AWS PrivateLink Support**       |      ❌       |       ✔        | Network isolation       |
| **IP Address Restrictions**       |      ❌       |       ✔        | Access control          |
| **In-VPC Remote Ingestion Agent** |      ❌       |       ✔        | Data security control   |

## Implementation & Support

| Feature Name                           | OSS Available | Cloud Available | Business Value                                               |
| :------------------------------------- | :-----------: | :-------------: | :----------------------------------------------------------- |
| **Fully Managed Cloud Deployment**     |      ❌       |       ✔        | Zero maintenance cloud-hosted instance                       |
| **Dedicated Customer Success**         |      ❌       |       ✔        | Expert guidance                                              |
| **Guided Implementation & Onboarding** |      ❌       |       ✔        | Smooth rollout                                               |
| **Private Slack Support Channel**      |      ❌       |       ✔        | Direct access to experts                                     |
| **Community Support**                  |      ✔       |       ✔        | Peer assistance                                              |
| **OSS Contribution Fast-Track**        |      ❌       |       ✔        | Community Contribution Support to DataHub Apache 2.0 Project |

<a href="https://datahub.com/get-datahub-cloud/" style={{ display: 'inline-block', padding: '10px 20px', margin: '10px 0', backgroundColor: '#007bff', color: 'white', borderRadius: '5px', textDecoration: 'none', textAlign: 'center' }}>
See DataHub Cloud In Action
</a>
