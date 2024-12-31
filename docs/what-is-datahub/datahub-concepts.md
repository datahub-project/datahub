# DataHub Concepts

Explore key concepts of DataHub to take full advantage of its capabilities in managing your data.

## General Concepts

### URN (Uniform Resource Name)

URN (Uniform Resource Name) is the chosen scheme of URI to uniquely define any resource in DataHub. It has the following form.

```
urn:<Namespace>:<Entity Type>:<ID>
```

Examples include `urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)`, `urn:li:corpuser:jdoe`.

> - [What is URN?](/docs/what/urn.md)

### Policy

Access policies in DataHub define who can do what to which resources.

> - [Authorization: Policies Guide](/docs/authorization/policies.md)
> - [Developer Guides: DataHubPolicy](/docs/generated/metamodel/entities/dataHubPolicy.md)
> - [Feature Guides: About DataHub Access Policies](/docs/authorization/access-policies-guide.md)

### Role

DataHub provides the ability to use Roles to manage permissions.

> - [Authorization: About DataHub Roles](/docs/authorization/roles.md)
> - [Developer Guides: DataHubRole](/docs/generated/metamodel/entities/dataHubRole.md)

### Access Token (Personal Access Token)

Personal Access Tokens, or PATs for short, allow users to represent themselves in code and programmatically use DataHub's APIs in deployments where security is a concern.
Used along-side with [authentication-enabled metadata service](/docs/authentication/introducing-metadata-service-authentication.md), PATs add a layer of protection to DataHub where only authorized users are able to perform actions in an automated way.

> - [Authentication: About DataHub Personal Access Tokens](/docs/authentication/personal-access-tokens.md)
> - [Developer Guides: DataHubAccessToken](/docs/generated/metamodel/entities/dataHubAccessToken.md)

### View

Views allow you to save and share sets of filters for reuse when browsing DataHub. A view can either be public or personal.

> - [DataHubView](/docs/generated/metamodel/entities/dataHubView.md)

### Deprecation

Deprecation is an aspect that indicates the deprecation status of an entity. Typically it is expressed as a Boolean value.

> - [Deprecation of a dataset](/docs/generated/metamodel/entities/dataset.md#deprecation)

### Ingestion Source

Ingestion sources refer to the data systems that we are extracting metadata from. For example, we have sources for BigQuery, Looker, Tableau and many others.

> - [Sources](/metadata-ingestion/README.md#sources)
> - [DataHub Integrations](https://datahubproject.io/integrations)

### Container

A container of related data assets.

> - [Developer Guides: Container](/docs/generated/metamodel/entities/container.md)

### Data Platform

Data Platforms are systems or tools that contain Datasets, Dashboards, Charts, and all other kinds of data assets modeled in the metadata graph.

<details><summary>
List of Data Platforms
</summary>

- Azure Data Lake (Gen 1)
- Azure Data Lake (Gen 2)
- Airflow
- Ambry
- ClickHouse
- Couchbase
- External Source
- HDFS
- SAP HANA
- Hive
- Iceberg
- AWS S3
- Kafka
- Kafka Connect
- Kusto
- Mode
- MongoDB
- MySQL
- MariaDB
- OpenAPI
- Oracle
- Pinot
- PostgreSQL
- Presto
- Tableau
- Vertica

Reference : [data_platforms.yaml](https://github.com/datahub-project/datahub/blob/master/metadata-service/configuration/src/main/resources/bootstrap_mcps/data-platforms.yaml)

</details>

> - [Developer Guides: Data Platform](/docs/generated/metamodel/entities/dataPlatform.md)

### Dataset

Datasets represent collections of data that are typically represented as Tables or Views in a database (e.g. BigQuery, Snowflake, Redshift etc.), Streams in a stream-processing environment (Kafka, Pulsar etc.), bundles of data found as Files or Folders in data lake systems (S3, ADLS, etc.).

> - [Developer Guides: Dataset](/docs/generated/metamodel/entities/dataset.md)

### Chart

A single data vizualization derived from a Dataset. A single Chart can be a part of multiple Dashboards. Charts can have tags, owners, links, glossary terms, and descriptions attached to them. Examples include a Superset or Looker Chart.

> - [Developer Guides: Chart](/docs/generated/metamodel/entities/chart.md)

### Dashboard

A collection of Charts for visualization. Dashboards can have tags, owners, links, glossary terms, and descriptions attached to them. Examples include a Superset or Mode Dashboard.

> - [Developer Guides: Dashboard](/docs/generated/metamodel/entities/dashboard.md)

### Data Job

An executable job that processes data assets, where "processing" implies consuming data, producing data, or both.
In orchestration systems, this is sometimes referred to as an individual "Task" within a "DAG". Examples include an Airflow Task.

> - [Developer Guides: Data Job](/docs/generated/metamodel/entities/dataJob.md)

### Data Flow

An executable collection of Data Jobs with dependencies among them, or a DAG.
Sometimes referred to as a "Pipeline". Examples include an Airflow DAG.

> - [Developer Guides: Data Flow](/docs/generated/metamodel/entities/dataFlow.md)

### Glossary Term

Shared vocabulary within the data ecosystem.

> - [Feature Guides: Glossary](/docs/glossary/business-glossary.md)
> - [Developer Guides: GlossaryTerm](/docs/generated/metamodel/entities/glossaryTerm.md)

### Glossary Term Group

Glossary Term Group is similar to a folder, containing Terms and even other Term Groups to allow for a nested structure.

> - [Feature Guides: Term & Term Group](/docs/glossary/business-glossary.md#terms--term-groups)

### Tag

Tags are informal, loosely controlled labels that help in search & discovery. They can be added to datasets, dataset schemas, or containers, for an easy way to label or categorize entities – without having to associate them to a broader business glossary or vocabulary.

> - [Feature Guides: About DataHub Tags](/docs/tags.md)
> - [Developer Guides: Tags](/docs/generated/metamodel/entities/tag.md)

### Domain

Domains are curated, top-level folders or categories where related assets can be explicitly grouped.

> - [Feature Guides: About DataHub Domains](/docs/domains.md)
> - [Developer Guides: Domain](/docs/generated/metamodel/entities/domain.md)

### Owner

Owner refers to the users or groups that has ownership rights over entities. For example, owner can be acceessed to dataset or a column or a dataset.

> - [Getting Started : Adding Owners On Datasets/Columns](/docs/api/tutorials/owners.md#add-owners)

### Users (CorpUser)

CorpUser represents an identity of a person (or an account) in the enterprise.

> - [Developer Guides: CorpUser](/docs/generated/metamodel/entities/corpuser.md)

### Groups (CorpGroup)

CorpGroup represents an identity of a group of users in the enterprise.

> - [Developer Guides: CorpGroup](/docs/generated/metamodel/entities/corpGroup.md)

## Metadata Model

### Entity

An entity is the primary node in the metadata graph. For example, an instance of a Dataset or a CorpUser is an Entity.

> - [How does DataHub model metadata?](/docs/modeling/metadata-model.md)

### Aspect

An aspect is a collection of attributes that describes a particular facet of an entity.
Aspects can be shared across entities, for example "Ownership" is an aspect that is re-used across all the Entities that have owners.

> - [What is a metadata aspect?](/docs/what/aspect.md)
> - [How does DataHub model metadata?](/docs/modeling/metadata-model.md)

### Relationships

A relationship represents a named edge between 2 entities. They are declared via foreign key attributes within Aspects along with a custom annotation (@Relationship).

> - [What is a relationship?](/docs/what/relationship.md)
> - [How does DataHub model metadata?](/docs/modeling/metadata-model.md)
