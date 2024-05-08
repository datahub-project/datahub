# DataHub Roadmap

## [The DataHub Roadmap has a new home!](https://feature-requests.datahubproject.io/roadmap)

Please refer to the [new DataHub Roadmap](https://feature-requests.datahubproject.io/roadmap) for the most up-to-date details of what we are working on!

_If you have suggestions about what we should consider in future cycles, feel free to submit a [feature request](https://feature-requests.datahubproject.io/) and/or upvote existing feature requests so we can get a sense of level of importance!_


## Historical Roadmap

_This following represents the progress made on historical roadmap items as of January 2022. For incomplete roadmap items, we have created Feature Requests to gauge current community interest & impact to be considered in future cycles. If you see something that is still of high-interest to you, please up-vote via the Feature Request portal link and subscribe to the post for updates as we progress through the work in future cycles._

### Q4 2021 [Oct - Dec 2021]

#### Data Lake Ecosystem Integration
- [ ] Spark Delta Lake - [View in Feature Reqeust Portal](https://feature-requests.datahubproject.io/b/feedback/p/spark-delta-lake)
- [ ] Apache Iceberg - [Included in Q1 2022 Roadmap - Community-Driven Metadata Ingestion Sources](https://feature-requests.datahubproject.io/roadmap/540)
- [ ] Apache Hudi - [View in Feature Request Portal](https://feature-requests.datahubproject.io/b/feedback/p/apachi-hudi-ingestion-support)

#### Metadata Trigger Framework
[View in Feature Request Portal](https://feature-requests.datahubproject.io/b/User-Experience/p/ability-to-subscribe-to-an-entity-to-receive-notifications-when-something-changes)
- [ ] Stateful sensors for Airflow
- [ ] Receive events for you to send alerts, email
- [ ] Slack integration

#### ML Ecosystem
- [x] Features (Feast)
- [x] Models (Sagemaker)
- [ ] Notebooks - View in Feature Request Portal](https://feature-requests.datahubproject.io/admin/p/jupyter-integration)

#### Metrics Ecosystem
[View in Feature Request Portal](https://feature-requests.datahubproject.io/b/User-Experience/p/ability-to-define-metrics-and-attach-them-to-entities)
- [ ] Measures, Dimensions
- [ ] Relationships to Datasets and Dashboards

#### Data Mesh oriented features
- [ ] Data Product modeling
- [ ] Analytics to enable Data Meshification

#### Collaboration
[View in Feature Reqeust Portal](https://feature-requests.datahubproject.io/b/User-Experience/p/collaboration-within-datahub-ui)
- [ ] Conversations on the platform
- [ ] Knowledge Posts (Gdocs, Gslides, Gsheets)

### Q3 2021 [Jul - Sept 2021]

#### Data Profiling and Dataset Previews
Use Case: See sample data for a dataset and statistics on the shape of the data (column distribution, nullability etc.)
- [x] Support for data profiling and preview extraction through ingestion pipeline (column samples, not rows)

#### Data Quality
Included in Q1 2022 Roadmap - [Display Data Quality Checks in the UI](https://feature-requests.datahubproject.io/roadmap/544)
- [x] Support for data profiling and time-series views
- [ ] Support for data quality visualization
- [ ] Support for data health score based on data quality results and pipeline observability
- [ ] Integration with systems like Great Expectations, AWS deequ, dbt test etc. 

#### Fine-grained Access Control for Metadata
- [x] Support for role-based access control to edit metadata
- Scope: Access control on entity-level, aspect-level and within aspects as well.

#### Column-level lineage
Included in Q1 2022 Roadmap - [Column Level Lineage](https://feature-requests.datahubproject.io/roadmap/541)
- [ ] Metadata Model
- [ ] SQL Parsing

#### Operational Metadata
- [ ] Partitioned Datasets - - [View in Feature Request Portal](https://feature-requests.datahubproject.io/b/User-Experience/p/advanced-dataset-schema-properties-partition-support)
- [x] Support for operational signals like completeness, freshness etc.

### Q2 2021 (Apr - Jun 2021)

#### Cloud Deployment
- [X] Production-grade Helm charts for Kubernetes-based deployment
- [ ] How-to guides for deploying DataHub to all the major cloud providers 
  - [x] AWS
  - [ ] Azure
  - [x] GCP

#### Product Analytics for DataHub
- [x] Helping you understand how your users are interacting with DataHub
- [x] Integration with common systems like Google Analytics etc.

#### Usage-Based Insights
- [x] Display frequently used datasets, etc.
- [ ] Improved search relevance through usage data

#### Role-based Access Control
- Support for fine-grained access control for metadata operations (read, write, modify)
- Scope: Access control on entity-level, aspect-level and within aspects as well.
- This provides the foundation for Tag Governance, Dataset Preview access control etc.

#### No-code Metadata Model Additions
Use Case: Developers should be able to add new entities and aspects to the metadata model easily
- [x] No need to write any code (in Java or Python) to store, retrieve, search and query metadata
- [ ] No need to write any code (in GraphQL or UI) to visualize metadata

### Q1 2021 [Jan - Mar 2021]

#### React UI
- [x] Build a new UI based on React
- [x] Deprecate open-source support for Ember UI

#### Python-based Metadata Integration
- [x] Build a Python-based Ingestion Framework
- [x] Support common people repositories (LDAP)
- [x] Support common data repositories (Kafka, SQL databases, AWS Glue, Hive)
- [x] Support common transformation sources (dbt, Looker)
- [x] Support for push-based metadata emission from Python (e.g. Airflow DAGs)

#### Dashboards and Charts
- [x] Support for dashboard and chart entity page
- [x] Support browse, search and discovery

#### SSO for Authentication
- [x] Support for Authentication (login) using OIDC providers (Okta, Google etc)

#### Tags
Use-Case: Support for free-form global tags for social collaboration and aiding discovery
- [x] Edit / Create new tags
- [x] Attach tags to relevant constructs (e.g. datasets, dashboards, users, schema\_fields)
- [x] Search using tags (e.g. find all datasets with this tag, find all entities with this tag)

#### Business Glossary
- [x] Support for business glossary model (definition + storage)
- [ ] Browse taxonomy
- [x] UI support for attaching business terms to entities and fields

#### Jobs, Flows / Pipelines
Use case: Search and Discover your Pipelines (e.g. Airflow DAGs) and understand data lineage with datasets
- [x] Support for Metadata Models + Backend Implementation
- [x] Metadata Integrations with systems like Airflow.

#### Data Profiling and Dataset Previews
Use Case: See sample data for a dataset and statistics on the shape of the data (column distribution, nullability etc.)
- [ ] Support for data profiling and preview extraction through ingestion pipeline
- Out of scope for Q1: Access control of data profiles and sample data
