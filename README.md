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

# DataHub: The Data Discovery Platform for the Modern Data Stack

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
  <a href="https://x.com/datahubproject">
    <img alt="X (formerly Twitter) Follow" src="https://img.shields.io/badge/follow-datahubproject-red.svg?style=flat&logo=x&labelColor=133554&color=1890ff" /></a>
</div>

---

### 🏠 Docs: [docs.datahub.com](https://docs.datahub.com/)

[Quickstart](https://docs.datahub.com/docs/quickstart) |
[Features](https://docs.datahub.com/docs/features) |
[Adoption](https://datahub.com/resources/?2004611554=dh-stories) |
[Demo](https://demo.datahub.com/) |
[Town Hall](https://docs.datahub.com/docs/townhalls)


## What is DataHub?

**DataHub is an enterprise-grade, real-time metadata platform** that enables data discovery, observability, and governance across your entire data ecosystem. Built by LinkedIn and proven at massive scale (100,000+ datasets), DataHub provides a unified catalog where data engineers, analysts, and scientists can find, understand, and trust their data.


**The Challenge:** Modern data stacks are fragmented across dozens of tools—warehouses, lakes, BI platforms, ML systems, orchestration engines. Finding the right data, understanding its lineage, and ensuring governance is like searching through a maze blindfolded.

**The DataHub Solution:** DataHub acts as a real-time metadata graph that continuously streams metadata from all your data tools, creating a single source of truth. Unlike batch-based catalogs that are always outdated, DataHub keeps your metadata fresh and actionable.

## Why DataHub?

- **Built for Scale**: Proven at LinkedIn managing 100,000+ datasets, 10M+ daily queries
- **Real-Time Streaming**: Metadata updates in seconds, not hours or days
- **Universal Connectors**: [100+ integrations](https://docs.datahub.com/integrations) for warehouses, databases, BI, ML, orchestration
- **Developer-First**: Rich APIs (GraphQL, REST), Python SDK, CLI tools
- Enterprise Ready: Battle-tested security, authentication, authorization, and audit trails
- **Open Source**: [Apache 2.0 licensed](./LICENSE), vendor-neutral, community-driven

## Common Use Cases


| Use Case | Description | Learn More |
|----------|-------------|------------|
| 🔍 **Data Discovery** | Help users find the right data for analytics and ML | [Guide](https://docs.datahub.com/docs/features) |
| 📊 **Impact Analysis** | Understand downstream impact before making changes | [Lineage Docs](https://docs.datahub.com/docs/lineage) |
| 🏛️ **Data Governance** | Enforce policies, classify PII, manage access | [Governance Guide](https://docs.datahub.com/docs/governance) |
| 🔔 **Data Quality** | Monitor freshness, volumes, schema changes | [Quality Checks](https://docs.datahub.com/docs/tests) |
| 📚 **Documentation** | Centralize data documentation and knowledge | [Docs Features](https://docs.datahub.com/docs/documentation) |
| 👥 **Collaboration** | Foster data culture with discussions and ownership | [Collaboration](https://docs.datahub.com/docs/features) |


## Quickstart

Please follow the [DataHub Quickstart Guide](https://docs.datahub.com/docs/quickstart) to run DataHub locally using [Docker](https://docker.com).

```
python3 -m pip install --upgrade acryl-datahub
datahub docker quickstart
```

What you get:
- ✅ DataHub GMS (backend metadata service)
- ✅ DataHub Frontend (React UI)
- ✅ Elasticsearch (search & analytics)
- ✅ MySQL (metadata storage)
- ✅ Kafka + Schema Registry (streaming)
- ✅ Sample data 


 > You can alwasy try our [hosted demo]((https://demo.datahub.com/)) - Explore DataHub with sample data, no installation needed!


## Trusted by Industry Leaders
DataHub powers data discovery and governance at some of the world's most data-driven organizations.

[Here are the companies](https://datahub.com/resources/?2004611554=dh-stories) that have officially adopted DataHub. Please feel free to add yours to the list if we missed it.



## Community

Join our [Slack workspace](https://datahub.com/slack?utm_source=github&utm_medium=readme&utm_campaign=github_readme) for discussions and important announcements. You can also find out more about our upcoming [town hall meetings](docs/townhalls.md) and view past recordings.


## Contributing

We welcome contributions from the community. Please refer to our [Contributing Guidelines](docs/CONTRIBUTING.md) for more details. We also have a [contrib](contrib) directory for incubating experimental features.

If you're looking to build & modify datahub please take a look at our [Development Guide](https://docs.datahub.com/docs/developers).


## DataHub Cloud

Looking for a fully managed solution? **DataHub Cloud** provides enterprise-grade data catalog with zero infrastructure management.

**☁️ [Request Demo](https://datahub.com/demo/)** | **[Why Cloud?](https://datahub.com/products/why-datahub-cloud/)** | **[Cloud vs Core](https://datahub.com/products/cloud-vs-core/)** | **[Pricing](https://www.acryldata.io/pricing)**


## Source Code and Repositories

- [datahub-project/datahub](https://github.com/datahub-project/datahub): This repository contains the complete source code for DataHub's metadata model, metadata services, integration connectors and the web application.
- [acryldata/datahub-actions](https://github.com/acryldata/datahub-actions): DataHub Actions is a framework for responding to changes to your DataHub Metadata Graph in real time.
- [acryldata/datahub-helm](https://github.com/acryldata/datahub-helm): Helm charts for deploying DataHub on a Kubernetes cluster
- [acryldata/meta-world](https://github.com/acryldata/meta-world): A repository to store recipes, custom sources, transformations and other things to make your DataHub experience magical.
- [dbt-impact-action](https://github.com/acryldata/dbt-impact-action): A github action for commenting on your PRs with a summary of the impact of changes within a dbt project.
- [datahub-tools](https://github.com/makenotion/datahub-tools): Additional python tools to interact with the DataHub GraphQL endpoints, built by Notion.
- [business-glossary-sync-action](https://github.com/acryldata/business-glossary-sync-action): A github action that opens PRs to update your business glossary yaml file.
- [mcp-server-datahub](https://github.com/acryldata/mcp-server-datahub): A [Model Context Protocol](https://modelcontextprotocol.io/) server implementation for DataHub.

