# Elasticsearch upgrade from 5.6.8 to 7.9.3: Summary of changes

### Search index mapping & settings
- Removal of mapping types (as mentioned [here](https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html))
- Specify the maximum allowed difference between `min_gram` and `max_gram` for NGramTokenizer and NGramTokenFilter by adding property `max_ngram_diff` in index settings, particularly if the difference is greater than 1 (as mentioned [here](https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html))


### Search query
The following parameters are/were `optional` and hence automatically populated in the search query. Some tests that expect a certain search query to be sent to ES will change with the ES upgrade.
- `disable_coord` parameter of the `bool` and `common_terms` queries has been removed (as mentioned [here](https://www.elastic.co/guide/en/elasticsearch/reference/6.8/breaking-changes-6.0.html))
- `auto_generate_synonyms_phrase_query` parameter in `match` query is added with a default value of `true` (as mentioned [here](https://www.elastic.co/guide/en/elasticsearch/reference/7.x/query-dsl-match-query.html))

### Java High Level Rest Client
- In 7.9.3, Java High Level Rest Client instance needs a REST low-level client builder to be built. In 5.6.8, the same instance needs REST low-level client
- Document APIs such as the Index API, Delete API, etc no longer takes the doc `type` as an input
