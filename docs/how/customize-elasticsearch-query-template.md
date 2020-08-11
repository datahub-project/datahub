# How to customize the search experience of an entity?

> Assume you have all the fields needed for your query, otherwise, refer to [this doc](./search-over-new-field.md) if you're only interested in indexing a new field of an existing entity 

## 1. Default query template for each type of search document
The search query is constructed and executed through [Search DAO]. Each search document is associated with a customizable query template, which can found [here](https://github.com/linkedin/datahub/tree/master/gms/impl/src/main/resources). 
Take [dataset search query template] for example, it supports a few features:

The general search accepts a query and at run time, [ES Search DAO] will replace $INPUT in the query template and generate the query for Elasticsearch.
For example, in search bar, user can type in "test" to find all the related datasets with its name matching the rules defined in query template.
 ```
   {
     "query_string": {
       "query": "$INPUT",
       "analyzer": "whitespace_lowercase",
       "boost": 0.125,
       "default_field": "name.ngram",
       "default_operator": "AND"
     }
   }...
 ```  
The advanced search for dataset in DataHub is implemented by [query string query]. 
This query uses a [syntax] to parse and split the provided query string based on operators, such as AND or NOT. The query then analyzes each split text independently before returning matching documents.
You can use the query_string query to create a complex search that includes wildcard characters, searches across multiple fields, and more. While versatile, the query is strict and returns an error if the query string includes any invalid syntax.
For example, in search bar, user can type in "platform:kafka" to get all kafka datasets or "owners:foo AND platform:hdfs" to get all the hdfs datasets owned by foo.

Relevance is achieved with [function score query] and [boost].
For example, the template sets high scores for those that have owners, and set lower scores for those that are deprecated.  
```   
 "functions": [
     {
       "filter": {
         "term": {
           "hasOwners": true
         }
       },
       "weight": 2
     },
     {
       "filter": {
         "term": {
           "deprecated": true
         }
       },
       "weight": 0.5
     }, ...
```

## 2. Elasticsearch full text queries
Elasticsearch provides a full Query DSL (Domain Specific Language) based on JSON to define queries. If the default query template does not suit your business need, you may want to explore more about [Elasticsearch full text queries]
Here are several popular ones: 
[match query] accepts text/numerics/dates, analyzes them, and constructs a query.
[multi_match query] builds on the match query to allow multi-field queries
[query_string_query] is a query that uses a query parser in order to parse its content.

## 3. Analyzers
Text analysis enables Elasticsearch to perform full-text search, where the search returns all relevant results rather than just exact matches.
Text analysis is performed by an [analyzer], a set of rules that govern the entire process. Elasticsearch includes a default analyzer, called the [standard analyzer], which works well for most use cases right out of the box.
If you want to tailor your search experience, you can choose a different built-in analyzer or even configure a custom one. A custom analyzer gives you control over each step of the analysis process.  

You can check a list of custom analyzers in settings in [dataset index config]
Analyzers can be specified per-query, per-field or per-index. Read more about analyzer [here][analyzer].

## 4. Language support
You can apply one of the [language analyzers] supported by Elasticsearch. 
For languages such as Chinese and Japanese, you would need to install [Smart Chinese Analysis plugin] or
[Japanese Analysis plugin] first. 

## 5. Test raw queries against Elasticsearch
You can first run the raw query to check if the results are expected. Below are some sample queries:
```
curl http://localhost:9200/datasetdocument/_search? -d '{"query": {"match": {"name": "test_datasetname"}}}'
curl http://localhost:9200/datasetdocument/_search? -d '{"query":{"query_string":{"query":"name:test_datasetname"}}}'
```

If the results look good, you can move on to the next step:
## 6. Test search rest API
Example query: 
```
curl 'http://localhost:8080/datasets?q=search&input=test_datasetname' 
```
If the results look good, you can move on to the next step if needed. 
## 7. Test end to end
Search via data hub UI. Debugging into mid-tier or UI if the results look different. 


[Search DAO]: ../architecture/metadata-serving.md#search-dao
[ES Search DAO]: ../../metadata-dao-impl/elasticsearch-dao/src/main/java/com/linkedin/metadata/dao/search/ESSearchDAO.java
[query string query]: https://www.elastic.co/guide/en/elasticsearch/reference/5.6/query-dsl-query-string-query.html
[dataset search query template]:../../gms/impl/src/main/resources/datasetESSearchQueryTemplate.json
[syntax]: https://www.elastic.co/guide/en/elasticsearch/reference/5.6/query-dsl-query-string-query.html#query-string-syntax
[function score query]: https://www.elastic.co/guide/en/elasticsearch/reference/5.6/query-dsl-function-score-query.html
[boost]: https://www.elastic.co/guide/en/elasticsearch/reference/5.6/mapping-boost.html
[Elasticsearch full text queries]:https://www.elastic.co/guide/en/elasticsearch/reference/5.6/full-text-queries.html
[match query]: https://www.elastic.co/guide/en/elasticsearch/reference/5.6/query-dsl-match-query.html
[multi_match query]: https://www.elastic.co/guide/en/elasticsearch/reference/5.6/query-dsl-multi-match-query.html
[query_string_query]: https://www.elastic.co/guide/en/elasticsearch/reference/5.6/query-dsl-query-string-query.html
[standard analyzer]:https://www.elastic.co/guide/en/elasticsearch/reference/5.6/analysis-standard-analyzer.html
[analyzer]: https://www.elastic.co/guide/en/elasticsearch/reference/5.6/analyzer.html
[language analyzers]:https://www.elastic.co/guide/en/elasticsearch/reference/5.6/analysis-lang-analyzer.html
[Smart Chinese Analysis plugin]: https://www.elastic.co/guide/en/elasticsearch/plugins/5.6/analysis-smartcn.html
[Japanese Analysis plugin]: https://www.elastic.co/guide/en/elasticsearch/plugins/5.6/analysis-kuromoji.html
[dataset index config]: ../../docker/elasticsearch-setup/dataset-index-config.json