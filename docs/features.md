

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

### Get Started Now

Run the following command to get started with DataHub:

```shell
python3 -m pip install --upgrade pip wheel setuptools
python3 -m pip install --upgrade acryl-datahub
datahub docker quickstart
```

- [Quickstart with DataHub Open Source](/docs/quickstart)
- [Try DataHub Cloud](https://datahub.com/get-datahub-cloud/)

## Key Features

- **[Data Discovery](/docs/how/search)** — Search your entire data ecosystem, including dashboards, datasets, ML models, and raw files.
- **[Data Governance](https://medium.com/datahub-project/the-3-must-haves-of-metadata-management-part-2-35a649f2e2fb)** — Define ownership and track PII.
- **[Data Quality & Observability](/docs/features/feature-guides/observe)** — Detect and resolve quality issues before they impact production. Automated anomaly detection, assertions, and data contracts keep data reliable.
- **[UI-based Ingestion](/docs/ui-ingestion)** — Easily set up integrations in minutes using DataHub's intuitive UI-based ingestion feature.
- **[APIs and SDKs](/docs/api/datahub-apis)** — For users who prefer programmatic control, DataHub offers a comprehensive set of APIs and SDKs.
- **[Vibrant Community](/docs/slack)** — Our community provides support through office hours, workshops, and a Slack channel.

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
