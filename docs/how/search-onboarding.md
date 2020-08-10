# How to onboard to GMA search?

> Refer to [this doc](./search-over-new-field.md) if you're only interested in indexing a new field of an existing entity 

## 1. Define search document model for the entity
Modeling is the most important and crucial part of your design. 
[Search document] model contains a list of fields that need to be indexed along with the names and their data types. 
Check [here][Search document] to learn more about search document model.
Please note that all fields in the search document model (except the `urn`) are `optional`. 
This is because we want to support partial updates to search documents.

[Search document]: ../what/search-document.md

## 2. Create the search index, define its mappings and settings

A [mapping] is created using the information of search document model. 
It defines how a document, and the fields it contains, are stored and indexed by various [tokenizers], [analyzers] and data type for the fields. 
For certain fields, sub-fields are created using different analyzers. 
The analyzers are chosen depending on the needs for each field. 
This is currently created manually using [curl] commands, and we plan to [automate](../what/search-index.md#search-automation-tbd) the process in the near future. 
Check index [mappings & settings](../../docker/elasticsearch/dataset-index-config.json) for `dataset` search index.

## 3. Ingestion into search index
The actual indexing process for each [entity] is powered by [index builders]. 
The builders register the metadata [aspects] of their interest against [MAE Consumer Job] and will be invoked whenever an [MAE] of same interest is received. 
Index builders should be extended from [BaseIndexBuilder]. Check [DatasetIndexBuilder] as an example. 
For the consumer job to consume those MAEs, you should add your index builder to the [index builder registry].

## 4. Search query configs
Once you have the [search index] built, it's ready to be queried! 
The search query is constructed and executed through [Search DAO]. 
The raw search hits are retrieved and extracted using the base model. 
Besides the regular full text search, run time aggregation and relevance are provided in the search queries as well. 

[ESSearchDAO] is the implementation for the [BaseSearchDAO] for Elasticsearch.
It's still a generic class which can be used for a specific [entity] and configured using [BaseSearchConfig]. 

BaseSearchConfig is the abstraction for all query related configurations such as query templates, default field to execute autocomplete on etc.

```java
public abstract class BaseSearchConfig<DOCUMENT extends RecordTemplate> {

  public abstract Set<String> getFacetFields();

  public String getIndexName() {
    return getSearchDocument().getSimpleName().toLowerCase();
  }

  public abstract Class<DOCUMENT> getSearchDocument();

  public abstract String getDefaultAutocompleteField();

  public abstract String getSearchQueryTemplate();

  public abstract String getAutocompleteQueryTemplate();
}
```

[DatasetSearchConfig] is the implementation of search config for `dataset` entity.

Search query templates for various entities can be found [here](https://github.com/linkedin/datahub/tree/master/gms/impl/src/main/resources). 

## 5. Add search query endpoints to GMS
Finally, you need to create [rest.li](https://rest.li) APIs to serve your search queries. 
[BaseSearchableEntityResource] provides an abstract implementation of search and autocomplete APIs.
Any top level rest.li resource implementation could extend it and easily add search and autocomplete [ACTION](https://linkedin.github.io/rest.li/user_guide/restli_server#action) methods.
Refer to [CorpUsers] rest.li resource implementation as an example.


[mapping]: https://www.elastic.co/guide/en/elasticsearch/reference/5.6/mapping.html
[tokenizer]: https://www.elastic.co/guide/en/elasticsearch/reference/5.6/analysis-tokenizers.html
[analyzer]: https://www.elastic.co/guide/en/elasticsearch/reference/5.6/analysis-analyzers.html
[curl]: https://en.wikipedia.org/wiki/CURL
[entity]: ../what/entity.md
[index builder]: ../architecture/metadata-ingestion.md#search-and-graph-index-builders
[aspect]: ../what/aspect.md
[mae consumer job]: ../architecture/metadata-ingestion.md#mae-consumer-job
[mae]: ../what/mxe.md#metadata-audit-event-mae
[baseindexbuilder]: ../../metadata-builders/src/main/java/com/linkedin/metadata/builders/search/BaseIndexBuilder.java
[datasetindexbuilder]: ../../metadata-builders/src/main/java/com/linkedin/metadata/builders/search/DatasetIndexBuilder.java
[index builder registry]: ../../metadata-builders/src/main/java/com/linkedin/metadata/builders/search/RegisteredIndexBuilders.java
[search index]: ../what/search-index.md
[search dao]: ../architecture/metadata-serving.md#search-dao
[essearchdao]: ../../metadata-dao-impl/elasticsearch-dao/src/main/java/com/linkedin/metadata/dao/search/ESSearchDAO.java
[basesearchdao]: ../../metadata-dao/src/main/java/com/linkedin/metadata/dao/BaseSearchDAO.java
[basesearchconfig]: ../../metadata-dao-impl/elasticsearch-dao/src/main/java/com/linkedin/metadata/dao/search/BaseSearchConfig.java
[datasetsearchconfig]: ../../gms/impl/src/main/java/com/linkedin/metadata/configs/DatasetSearchConfig.java
[basesearchableentityresource]: ../../metadata-restli-resource/src/main/java/com/linkedin/metadata/restli/BaseSearchableEntityResource.java
[corpusers]: ../../gms/impl/src/main/java/com/linkedin/metadata/resources/identity/CorpUsers.java
