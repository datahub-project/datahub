---
title: "Features"
---

# DataHub Features Overview

DataHub is a modern data catalog built to enable end-to-end data discovery, data observability, and data governance. This extensible metadata platform is built for developers to tame the complexity of their rapidly evolving data ecosystems, and for data practitioners to leverage the full value of data within their organization.

Here’s an overview of DataHub’s current functionality. Curious about what’s to come? Check out our [roadmap](https://feature-requests.datahubproject.io/roadmap).

## End-to-end Search and Discovery

### Search for assets across databases, datalakes, BI platforms, ML feature stores, workflow orchestration, and more

Here’s an example of searching for assets related to the term `health`: we see results spanning Looker dashboards, BigQuery datasets, and DataHub Tags & Users, and ultimately navigate to the “DataHub Health” Looker dashboard overview ([view in demo site](https://demo.datahubproject.io/dashboard/urn:li:dashboard:(looker,dashboards.11)/Documentation?is_lineage_mode=false))

![](./imgs/feature-search-across-all-entities.gif)

### Easily understand the end-to-end journey of data by tracing lineage across platforms, datasets, pipelines, charts, and dashboards

Let’s dig into the dependency chain of the “DataHub Health” Looker dashboard. Using the lineage view, we can navigate all upstream dependencies of the Dashboard including Looker Charts, Snowflake and s3 Datasets, and Airflow Pipelines ([view in demo site](https://demo.datahubproject.io/dashboard/urn:li:dashboard:(looker,dashboards.11)/Documentation?is_lineage_mode=true))

![](./imgs/feature-navigate-lineage-vis.gif)

### Quickly gain context about related entities as you navigate the lineage graph

As you explore the relationships between entities, it’s easy to view documentation, usage stats, ownership, and more without leaving the lineage graph

![](./imgs/feature-view-entitiy-details-via-lineage-vis.gif)

### Gain confidence in the accuracy and relevance of datasets

DataHub provides dataset profiling and usage statistics for popular data warehousing platforms, making it easy for data practitioners to understand the shape of the data and how it has evolved over time. Query stats give context into how often (and by whom) the data is queried which can act as a strong signal of the trustworthiness of a dataset

![](./imgs/feature-table-usage-and-stats.gif)

## Robust Documentation and Tagging

### Capture and maintain institutional knowledge via API and/or the DataHub UI

DataHub makes it easy to update and maintain documentation as definitions and use cases evolve. In addition to managing documentation via GMS, DataHub offers rich documentation and support for external links via the UI. 

![](./imgs/feature-rich-documentation.gif)

### Create and define new tags via API and/or the DataHub UI

Create and add tags to any type of entity within DataHub via the GraphQL API, or allow your end users to create and define new tags within the UI as use cases evolve over time

![](./imgs/feature-create-new-tag.gif)

### Browse and search specific tags to fast-track discovery across entities

Seamlessly browse entities associated with a tag or filter search results for a specific tag to find the entities that matter most

![](./imgs/feature-tag-browse.gif)

## Data Governance at your fingertips 

### Quickly assign asset ownership to users and/or user groups 

![](./imgs/feature-add-owners.gif)

### Manage Fine-Grained Access Control with Policies

DataHub admins can create Policies to define who can perform what action against which resource(s). When you create a new Policy, you will be able to define the following:

* **Policy Type Platform** (top-level DataHub Platform privileges, i.e. managing users, groups, and policies) or Metadata (ability to manipulate ownership, tags, documentation, & more)
* **Resource Type** - Specify the type of resource, such as Datasets, Dashboards, Pipelines, etc.
* **Privileges** - Choose the set of permissions, such as Edit Owners, Edit Documentation, Edit Links
* **Users and/or Groups** - Assign relevant Users and/or Groups; you can also assign the Policy to Resource Owners, regardless of which Group they belong to

![](./imgs/feature-create-policy.gif)

## Metadata quality & usage analytics

Gain a deeper understanding of the health of metadata within DataHub and how end-users are interacting with the platform. The Analytics view provides a snapshot of volume of assets and percentage with assigned ownership, weekly active users, and most common searches & actions ([view in demo site](https://demo.datahubproject.io/analytics)).

![](./imgs/feature-datahub-analytics.png)

## DataHub is a Platform for Developers

DataHub is an API- and stream-first platform, empowering developers to implement an instance tailored to their specific data stack. Our growing set of flexible integration models allow for push and pull metadata ingestion, as well as no-code metadata model extensions to quickly get up and running. 

### Dataset Sources
| Source | Status |
|---|:---:|
| Athena | Supported |
| BigQuery | Supported |
| Delta Lake | Planned |
| Druid | Supported |
| Elasticsearch | Supported |
| Hive | Supported |
| Hudi | Planned |
| Iceberg | Planned |
| Kafka Metadata | Supported |
| MongoDB | Supported |
| Microsoft SQL Server | Supported |
| MySQL | Supported |
| Oracle | Supported |
| PostgreSQL | Supported |
| Redshift | Supported |
| s3 | Supported |
| Snowflake | Supported |
| Spark/Databricks | Partially Supported |
| Trino FKA Presto | Supported |

### BI Tools
| Source | Status |
|---|:---:|
| Business Glossary | Supported |
| Looker | Supported |
| Redash | Supported |
| Superset | Supported |
| Tableau | Planned |
| Grafana | Partially Supported |

### ETL / ELT
| Source | Status |
|---|:---:|
| dbt | Supported |
| Glue | Supported |

### Workflow Orchestration
| Source | Status |
|---|:---:|
| Airflow | Supported |
| Prefect | Planned |

### Data Observability
| Source | Status |
|---|:---:|
| Great Expectations | Planned |

### ML Platform
| Source | Status |
|---|:---:|
| Feast | Supported |
| Sagemaker | Supported |

### Identity Management
| Source | Status |
|---|:---:|
| Azure AD | Supported |
| LDAP | Supported |
| Okta | Supported |