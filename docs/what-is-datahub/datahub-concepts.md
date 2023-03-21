# DataHub Concepts

Explore key concepts of DataHub to take full advantage of its capabilities in managing your data.

## General Concepts

### URN (Uniform Resource Name)
URN (Uniform Resource Name) is the chosen scheme of URI to uniquely define any resource in DataHub. It has the following form.
```
urn:<Namespace>:<Entity Type>:<ID>
```
> * [What is URN?](/docs/what/urn.md)

### Entity
An entity is the primary node in the metadata graph. For example, an instance of a Dataset or a CorpUser is an Entity. 

> * [How does DataHub model metadata?](/docs/modeling/metadata-model.md)

### Aspect
An aspect is a collection of attributes that describes a particular facet of an entity. 
Aspects can be shared across entities, for example "Ownership" is an aspect that is re-used across all the Entities that have owners. 

> * [What is a metadata aspect?](/docs/what/aspect.md)
> * [How does DataHub model metadata?](/docs/modeling/metadata-model.md)

### Policy
Access policies in DataHub define who can do what to which resources. 

> * [Policies Guide](/docs/authorization/policies.md)
> * [Developer Guides: DataHubPolicy](/docs/generated/metamodel/entities/dataHubPolicy.md)
> * [Feature Guides: About DataHub Access Policies](/docs/authorization/access-policies-guide.md)

### Role
DataHub provides the ability to use Roles to manage permissions.

> * [About DataHub Roles](/docs/authorization/roles.md)
> * [Developer Guides: DataHubRole](/docs/generated/metamodel/entities/dataHubRole.md)


### Relationships 
A relationship represents a named edge between 2 entities. They are declared via foreign key attributes within Aspects along with a custom annotation (@Relationship). 

> * [What is a relationship?](/docs/what/relationship.md)
> * [How does DataHub model metadata?](/docs/modeling/metadata-model.md)

## Metadata Models

### Data platform 
Data Platforms are systems or tools that contain Datasets, Dashboards, Charts, and all other kinds of data assets modeled in the metadata graph.
Examples of data platforms are redshift, hive, bigquery, looker, tableau etc.

> * [Developer Guides: Data Platform](/docs/generated/metamodel/entities/dataPlatform.md)

### Dataset
Datasets represent collections of data that are typically represented as Tables or Views in a database (e.g. BigQuery, Snowflake, Redshift etc.), Streams in a stream-processing environment (Kafka, Pulsar etc.), bundles of data found as Files or Folders in data lake systems (S3, ADLS, etc.).

> * [Developer Guides: Dataset](/docs/generated/metamodel/entities/dataset.md)

### Chart
A single data vizualization derived from a Dataset. A single Chart can be a part of multiple Dashboards. Charts can have tags, owners, links, glossary terms, and descriptions attached to them. Examples include a Superset or Looker Chart.

> * [Developer Guides: Chart](/docs/generated/metamodel/entities/chart.md)


### Dashboard
A collection of Charts for visualization. Dashboards can have tags, owners, links, glossary terms, and descriptions attached to them. Examples include a Superset or Mode Dashboard.

> * [Developer Guides: Dashboard](/docs/generated/metamodel/entities/dashboard.md)


### Data Job 
An executable job that processes data assets, where "processing" implies consuming data, producing data, or both. 
Examples include an Airflow Task.

> * [Developer Guides: Data Job](/docs/generated/metamodel/entities/dataJob.md)


### Data Flow
An executable collection of Data Jobs with dependencies among them, or a DAG. Data Jobs can have tags, owners, links, glossary terms, and descriptions attached to them. Examples include an Airflow DAG.

> * [Developer Guides: Data Flow](/docs/generated/metamodel/entities/dataFlow.md)

### Glossary Term 
Shared vocabulary within the data ecosystem.

> * [Feature Guides: Glossary](/docs/glossary/business-glossary.md) 
> * [Developer Guides: GlossaryTerm](/docs/generated/metamodel/entities/glossaryTerm.md)

### Tag 
Tags are informal, loosely controlled labels that help in search & discovery. They can be added to datasets, dataset schemas, or containers, for an easy way to label or categorize entities – without having to associate them to a broader business glossary or vocabulary.

> * [Feature Guides: About DataHub Tags](/docs/tags.md)
> * [Developer Guides: Tags](/docs/generated/metamodel/entities/tag.md)


### Domain
Domains are curated, top-level folders or categories where related assets can be explicitly grouped.

> * [Feature Guides: About DataHub Domains](/docs/domains.md)
> * [Developer Guides: Domain](/docs/generated/metamodel/entities/domain.md)


### CorpUser
CorpUser represents an identity of a person (or an account) in the enterprise.

> * [Developer Guides: CorpUser](/docs/generated/metamodel/entities/corpuser.md)

### CorpGroup
CorpGroup represents an identity of a group of users in the enterprise.

> * [Developer Guides: CorpGroup](/docs/generated/metamodel/entities/corpGroup.md)
