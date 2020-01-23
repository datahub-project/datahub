# What is GMA search index?

Each [search document](search-document.md) type (or [entity](entity.md) type) will be mapped to an independent search index in Elasticsearch. 
Beyond the standard search engine features (analyzer, tokenizer, filter queries, faceting, sharding, etc), 
GMA also supports the following specific features:
* Partial update of indexed documents
* Membership testing on multi-value fields
* Zero downtime switch between indices

Check out [Search DAO](../architecture/metadata-serving.md#search-dao) for search query abstraction in GMA.

## Search Automation (TBD)

We aim to automate the index creation, schema evolution, and reindexing such that the team will only need to focus on the search document model and their custom [Index Builder](../architecture/metadata-ingestion.md#search-index-builders) logic. 
As the logic changes, a new version of the index will be created and populated from historic MAEs. 
Once it’s fully populated, the team can switch to the new version through a simple config change from their [GMS](gms.md). 
They can also rollback to an older version of index whenever needed.