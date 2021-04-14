# DataHub Roadmap

Here is DataHub's roadmap for the next six months (starting Jan 2021).

We publish only a short roadmap, because we are evolving very fast and want to adapt to the community's needs. We will be checking off against this roadmap as we make progress over the next few months.

**Caveat**: ETA-s are subject to change. Do let us know before you commit to your stakeholders about deploying these capabilities at your company.

If you would like to suggest new items or request timeline changes to the existing items, please submit your request through this [form](https://docs.google.com/forms/d/1znDv7_CXXvUDcUsqzq92PgGqPSh_1yeYC3cl2xgizSE/) or submit a GitHub [feature request](https://github.com/linkedin/datahub/issues/new?assignees=&labels=feature-request&template=--feature-request.md&title=A+short+description+of+the+feature+request).

Of course, you always have access to our community through [Slack](https://slack.datahubproject.io) or our [town halls](townhalls.md) to chat with us live!

## Q1 2021 [Jan - Mar 2021]

### React UI
- [x] Build a new UI based on React
- [x] Deprecate open-source support for Ember UI

### Python-based Metadata Integration
- [x] Build a Python-based Ingestion Framework
- [x] Support common people repositories (LDAP)
- [x] Support common data repositories (Kafka, SQL databases, AWS Glue, Hive)
- [ ] Support common transformation sources (dbt, Looker)
- [x] Support for push-based metadata emission from Python (e.g. Airflow DAGs)

### Dashboards and Charts
- [x] Support for dashboard and chart entity page
- [x] Support browse, search and discovery

### SSO for Authentication
- [x] Support for Authentication (login) using OIDC providers (Okta, Google etc)

### Tags 
Use-Case: Support for free-form global tags for social collaboration and aiding discovery
- [x] Edit / Create new tags
- [x] Attach tags to relevant constructs (e.g. datasets, dashboards, users, schema\_fields)
- [x] Search using tags (e.g. find all datasets with this tag, find all entities with this tag)

### Business Glossary
- [ ] Support for business glossary model (definition + storage)
- [ ] Browse taxonomy
- [ ] UI support for attaching business terms to entities and fields

### Jobs, Flows / Pipelines
Use case: Search and Discover your Pipelines (e.g. Airflow DAGs) and understand lineage with datasets
- [x] Support for Metadata Models + Backend Implementation
- [ ] Metadata Integrations with systems like Airflow.

### Data Profiling and Dataset Previews
Use Case: See sample data for a dataset and statistics on the shape of the data (column distribution, nullability etc.)
- [ ] Support for data profiling and preview extraction through ingestion pipeline
- Out of scope for Q1: Access control of data profiles and sample data

## Q2 2021 (Apr - Jun 2021)

### Cloud Deployment
- [ ] Production-grade Helm charts for Kubernetes-based deployment
- [ ] How-to guides for deploying DataHub to all the major cloud providers (AWS, Azure, GCP)


### Data Quality
- Support for data quality visualization
- Support for data health score based on data quality results and pipeline observability
- Integration with systems like Great Expectations, AWS deequ etc.

### Product Analytics for DataHub
- Helping you understand how your users are interacting with DataHub
- Integration with common systems like Google Analytics etc.

### Usage-Based Insights
- Display frequently used datasets, dashboards
- Improved search relevance through usage data

### Role-based Access Control
- Support for fine-grained access control for metadata operations (read, write, modify)
- Scope: Access control on entity-level, aspect-level and within aspects as well.
- This provides the foundation for Tag Governance, Dataset Preview access control etc.

### No-code Metadata Model Additions
Use Case: Developers should be able to add new entities and aspects to the metadata model easily
- No need to write any code (in Java or Python) to store, retrieve, search and query metadata

## Beyond the horizon

### Let us know what you want!
- Submit requests [here](https://docs.google.com/forms/d/1znDv7_CXXvUDcUsqzq92PgGqPSh_1yeYC3cl2xgizSE/) or
- Submit a GitHub [feature request](https://github.com/linkedin/datahub/issues/new?assignees=&labels=feature-request&template=--feature-request.md&title=A+short+description+of+the+feature+request). 



