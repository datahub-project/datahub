# DataHub Features

DataHub is made up of a [generic backend](what/gma.md) and a [React-based UI](../datahub-web-react/README.md).
Original DataHub [blog post](https://engineering.linkedin.com/blog/2019/data-hub) talks about the design extensively and mentions some of the features of DataHub.
Our open sourcing [blog post](https://engineering.linkedin.com/blog/2020/open-sourcing-datahub--linkedins-metadata-search-and-discovery-p) also provides a comparison of some features between LinkedIn production DataHub vs open source DataHub. Below is a list of the latest features that are available in DataHub, as well as ones that will soon become available.

## Entities 

### Datasets
 - **Search**: full-text & advanced search, search ranking
 - **Browse**: browsing through a configurable hierarchy
 - **Schema**: table & document schema in tabular and JSON format
 - **Coarse grain lineage**: support for lineage at the dataset level, tabular & graphical visualization of downstreams/upstreams
 - **Ownership**: surfacing owners of a dataset, viewing datasets you own
 - **Dataset life-cycle management**: deprecate/undeprecate, surface removed datasets and tag it with "removed"
 - **Institutional knowledge**: support for adding free form doc to any dataset
 - **Fine grain lineage**: support for lineage at the field level [*coming soon*]
 - **Social actions**: likes, follows, bookmarks [*coming soon*]
 - **Compliance management**: field level tag based compliance editing [*coming soon*]
 - **Top users**: frequent users of a dataset [*coming soon*]
 
### Users & Groups
 - **Search**: full-text & advanced search, search ranking
 - **Browse**: browsing through a configurable hierarchy [*coming soon*]
 - **Profile editing**: LinkedIn style professional profile editing such as summary, skills

### Dashboards & Charts 
- **Search**: full-text & advanced search, search ranking
- **Basic information**: ownership, location. Link to external service for viewing the dashboard.
- **Institutional knowledge**: support for adding free form doc to any dashboards [*coming soon*]

### Tasks & Pipelines
 - **Search**: full-text & advanced search, search ranking
 - **Browse**: browsing through a configurable hierarchy
 - **Basic information**: 
 - **Execution history**: Executions and their status. Link to external service for viewing full info.

### Tags
- **Globally defined**: Tags provided a standardized set of labels that can be shared across all your entities
- **Supports entities and schemas**: Tags can be applied at the entity level or for datasets, attached to schema fields.
- **Searchable** Entities can be searched and filtered by tag

### Schemas [*coming soon*]
- **Search**: full-text & advanced search, search ranking
- **Browse**: browsing through a configurable hierarchy
- **Schema history**: view and diff historic versions of schemas
- **GraphQL**: visualization of GraphQL schemas


### Metrics [*coming soon*]
 - **Search**: full-text & advanced search, search ranking
 - **Browse**: browsing through a configurable hierarchy
 - **Basic information**: ownershp, dimensions, formula, input & output datasets, dashboards
 - **Institutional knowledge**: support for adding free form doc to any metric

## Fine-Grained Access Controls

DataHub also provides mechanisms to control *who* has access to *which* metadata entities via UI & API. Using this functionality,
admins of DataHub can define policies such as 

- Dataset Owners should be able to update Documentation, but not Tags, for all datasets. 
- A specific Data Steward should be able to add tags to any Dataset, but edit nothing else. 
- Data Platform team should have all privileges for DataHub, including manging policies & viewing platform analytics.

For an in-depth introduction into Fine-Grained Access Control, check out [Fine-Grained Access Policies](./policies.md) and 
the August 2021 [Town Hall demo](https://www.youtube.com/watch?v=3joZINi3ti4).

## Metadata Sources

We have a [Metadata Ingestion Framework](../metadata-ingestion/README.md) which supports a variety of popular connectors, like

- BigQuery
- Snowflake
- Redshift 
- Postgres
- Kafka
- MySQL
- Hive
- Looker
- MongoDB 

and many more. 