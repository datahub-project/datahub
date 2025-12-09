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

# DataHub: The Data Discovery Platform for AI & Data Context Management

### Built with ❤️ by <img src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/datahub-logo-color-mark.svg" width="20"/> [DataHub](https://datahub.com) and <img src="https://docs.datahub.com/img/LI-In-Bug.png" width="20"/> [LinkedIn](https://engineering.linkedin.com)

<div>
  <a target="_blank" href="https://github.com/datahub-project/datahub/blob/master/LICENSE">
    <img alt="Apache 2.0 License" src="https://img.shields.io/badge/License-Apache_2.0-blue.svg?label=license&labelColor=133554&color=1890ff" /></a>
  <a target="_blank" href="https://pypi.org/project/acryl-datahub/">
    <img alt="PyPI" src="https://img.shields.io/pypi/dm/acryl-datahub?label=downloads&labelColor=133554&color=1890ff" /></a>
  <a target="_blank" href="https://github.com/datahub-project/datahub/pulse">
    <img alt="GitHub commit activity" src="https://img.shields.io/github/commit-activity/m/datahub-project/datahub?label=commits&labelColor=133554&color=1890ff" /></a>
  <br />
  <a target="_blank" href="https://datahub.com/slack?utm_source=github&utm_medium=readme&utm_campaign=github_readme">
    <img alt="Slack" src="https://img.shields.io/badge/slack-join_community-red.svg?logo=slack&labelColor=133554&color=1890ff" /></a>
  <a href="https://www.youtube.com/channel/UC3qFQC5IiwR5fvWEqi_tJ5w">
    <img alt="YouTube" src="https://img.shields.io/youtube/channel/subscribers/UC3qFQC5IiwR5fvWEqi_tJ5w?style=flat&logo=youtube&label=subscribers&labelColor=133554&color=1890ff"/></a>
  <a href="https://medium.com/datahub-project/">
    <img alt="Medium" src="https://img.shields.io/badge/blog-DataHub-red.svg?style=flat&logo=medium&logoColor=white&labelColor=133554&color=1890ff" /></a>
</div>

## Overview

Open-source data catalog for search, lineage, and governance. 100+ connectors, real-time metadata streaming, and enterprise-grade reliability — all under the Apache 2.0 license.

### Docs: [docs.datahub.com](https://docs.datahub.com/)

[Quickstart](https://docs.datahub.com/docs/quickstart) |
[Features](https://datahub.com/products/) |
[Adoption](https://datahub.com/resources/?2004611554=dh-stories) |
[Demo](https://demo.datahub.com/) |
[Town Hall](https://datahub.com/events/)

## What is DataHub?

**DataHub is an open-source metadata platform** for data discovery, observability, and governance across your entire data stack. Built by LinkedIn and proven at enterprise scale (1,000,000+ datasets), DataHub provides a unified catalog where teams can find, understand, and trust their data.

Modern data stacks are fragmented across dozens of tools. DataHub solves this by acting as a **real-time metadata graph** that continuously streams metadata from all your data sources, creating a single source of truth.

## Why Choose DataHub Over Alternatives?

- **Built for Scale**: Proven at LinkedIn managing 1,000,000+ datasets, 10M+ daily queries
- **Real-Time Streaming**: Metadata updates in seconds, not hours or days
- **Universal Connectors**: [100+ integrations](https://docs.datahub.com/integrations) for warehouses, databases, BI, ML, orchestration
- **Developer-First**: Rich APIs (GraphQL, REST), Python SDK, CLI tools
- **Enterprise Ready**: Battle-tested security, authentication, authorization, and audit trails
- **Open Source**: [Apache 2.0 licensed](./LICENSE), vendor-neutral, community-driven

## Core Features

<p align="center">
  <a href="https://datahub.com/products/data-discovery/">
    <img alt="DataHub Discovery" src="https://raw.githubusercontent.com/datahub-project/datahub/master/docs-website/static/img/quickstart_discovery.png" height="150" />
  </a>
  <a href="https://datahub.com/products/data-governance">
    <img alt="DataHub Governance" src="https://raw.githubusercontent.com/datahub-project/datahub/master/docs-website/static/img/quickstart_governance.png" height="150" />
  </a>
  <a href="https://datahub.com/products/data-observability">
    <img alt="DataHub Observability" src="https://raw.githubusercontent.com/datahub-project/datahub/master/docs-website/static/img/quickstart_observability.png" height="150" />
  </a>
</p>

| Features                                                                                | Description                                                                                                |
| --------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| 🔍 [**Data Discovery**](https://datahub.com/products/data-discovery/)                   | Effortlessly discover and get context on trustworthy data                                                  |
| 👁️ [**Data Observability**](https://datahub.com/products/data-observability)            | Detect, resolve, and prevent data quality issues before they impact your business                          |
| 🏛️ [**Data Governance**](https://datahub.com/products/data-governance)                  | Ensure every data asset is accounted for by continuously fulfilling governance standards                   |
| 📊 [**Impact Analysis**](https://docs.datahub.com/docs/act-on-metadata/impact-analysis) | Understand downstream impact before making changes - [Lineage Docs](https://docs.datahub.com/docs/lineage) |

## How Do I Get Started with DataHub?

Please follow the [DataHub Quickstart Guide](https://docs.datahub.com/docs/quickstart) to run DataHub locally using [Docker](https://docker.com).

```bash
python3 -m pip install --upgrade acryl-datahub
datahub docker quickstart
```

**What you get:**

- ✅ DataHub GMS (backend metadata service)
- ✅ DataHub Frontend (React UI)
- ✅ Elasticsearch (search & analytics)
- ✅ MySQL (metadata storage)
- ✅ Kafka + Schema Registry (streaming)

> 💡 You can always try our [hosted demo](https://demo.datahub.com/) - Explore DataHub with sample data, no installation needed!

## Trusted by Industry Leaders

DataHub powers data discovery and governance at some of the world's most data-driven organizations.

**Featured Adopters:** LinkedIn, Expedia Group, Coursera, Klarna, Optum, CVS Health, Udemy, Peloton, N26, ThoughtWorks, Wikimedia, Viasat, Wealthsimple, ClassDojo, SpotHero

[See full list of 40+ companies →](https://datahub.com/resources/?2004611554=dh-stories)

## Community

Join our [Slack workspace](https://datahub.com/slack?utm_source=github&utm_medium=readme&utm_campaign=github_readme) for discussions and important announcements. You can also find out more about our upcoming [town hall meetings](docs/townhalls.md) and view past recordings.

## Contributing

We welcome contributions from the community. Please refer to our [Contributing Guidelines](docs/CONTRIBUTING.md) for more details. If you need help getting started, feel free to reach out to the DataHub team in the [#contribute-code](https://datahubspace.slack.com/archives/C017W0NTZHR) channel in our Slack community.

If you're looking to build & modify DataHub, please take a look at our [Development Guide](https://docs.datahub.com/docs/developers).

## DataHub Cloud

Looking for a fully managed solution? **DataHub Cloud** provides enterprise-grade data catalog with zero infrastructure management.

☁️ **[Request Demo](https://datahub.com/demo/)** | **[Why Cloud?](https://datahub.com/products/why-datahub-cloud/)** | **[Cloud vs Core](https://datahub.com/products/cloud-vs-core/)**

## Source Code and Repositories

- [datahub-project/datahub](https://github.com/datahub-project/datahub): This repository contains the complete source code for DataHub's metadata model, metadata services, integration connectors and the web application.
- [acryldata/datahub-actions](https://github.com/acryldata/datahub-actions): DataHub Actions is a framework for responding to changes to your DataHub Metadata Graph in real time.
- [acryldata/datahub-helm](https://github.com/acryldata/datahub-helm): Helm charts for deploying DataHub on a Kubernetes cluster
- [dbt-impact-action](https://github.com/acryldata/dbt-impact-action): A github action for commenting on your PRs with a summary of the impact of changes within a dbt project.
- [datahub-tools](https://github.com/makenotion/datahub-tools): Additional python tools to interact with the DataHub GraphQL endpoints, built by Notion.
- [business-glossary-sync-action](https://github.com/acryldata/business-glossary-sync-action): A github action that opens PRs to update your business glossary yaml file.
- [mcp-server-datahub](https://github.com/acryldata/mcp-server-datahub): A [Model Context Protocol](https://modelcontextprotocol.io/) server implementation for DataHub.
- [cs-acryl-example-scripts](https://github.com/acryldata/cs-acryl-example-scripts): Example scripts shared publically for using DataHub Cloud.

## 📖 More Information

- **Releases:** [Release notes](https://github.com/datahub-project/datahub/releases)
- **Security:** [Security stance & policies](docs/SECURITY_STANCE.md) - Information on DataHub's security posture
- **Architecture:** [System design docs](docs/architecture/architecture.md) - Understand how DataHub is built

## License

[Apache License 2.0](./LICENSE).
