# Features of DataHub

DataHub is composed of a [generic backend infra](what/gma.md) and a [Ember-based UI](../datahub-web). Original DataHub 
[blog post](https://engineering.linkedin.com/blog/2019/data-hub) extensively talks about the design and mentions some of
the features of DataHub. Our open sourcing [blog post](https://engineering.linkedin.com/blog/2020/open-sourcing-datahub--linkedins-metadata-search-and-discovery-p) 
also provides a comparison of some features between LinkedIn production DataHub vs open source DataHub. Although, these
are good references, we'll list down all available (also WIP) features of DataHub.

## Data Constructs (Entities)
Currently, open source DataHub only supports datasets, users and groups data constructs.

### Datasets
 - **Search**: full-text & advanced search, search ranking
 - **Browse**: browsing through a fixed hierarchy
 - **Schema**: table & document schema in tabular and JSON format
 - **Coarse grain lineage**: support for lineage at the dataset level, tabular & graphical visualization of downstreams/upstreams
 - **Ownership**: surfacing owners of a dataset, viewing datasets you own
 - **Dataset life-cycle management**: deprecate/undeprecate, surface removed datasets and tag it with "removed"
 - **Institutional knowledge**: support for adding free form doc to any dataset
 - **Fine grain lineage**: support for lineage at the field level [*Not available yet*]
 - **Social actions**: likes, follows, bookmarks [*Not available yet*]
 - **Compliance management**: field level tag based compliance editing [*Not available yet*]
 - **Top users**: frequent users of a dataset [*Not available yet*]
 
### Users
 - **Search**: full-text & advanced search, search ranking
 - **Profile editing**: LinkedIn style professional profile editing such as summary, skills
 
## Metadata Sources
You can integrate any data platform to DataHub easily. As long as you have a way of *E*xtracting metadata from the platform and
*T*ransform that into our standard [MCE](what/mxe.md) format, you're free to *L*oad/ingest metadata to DataHub from any available platform.
We have provided [ETL ingestion](architecture/metadata-ingestion.md) pipelines for:
 - Hive
 - Kafka
 - RDBMS
 - MySQL
 - LDAP