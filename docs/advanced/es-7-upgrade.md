# Elasticsearch upgrade from 5.6.8 to 7.9.3

## Summary of changes
Checkout the list of breaking changes for [Elasticsearch 6](https://www.elastic.co/guide/en/elasticsearch/reference/6.8/breaking-changes-6.0.html) and [Elasticsearch 7](https://www.elastic.co/guide/en/elasticsearch/reference/7.x/breaking-changes-7.0.html). Following is the summary of changes that impact Datahub. 

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

## Migration strategy

As mentioned in the docs, indices created in Elasticsearch 5.x are not readable by Elasticsearch 7.x. Running the upgraded elasticsearch container on the existing esdata volume will fail. 

For local development, our recommendation is to run the `docker/nuke.sh` script to remove the existing esdata volume before starting up the containers. Note, all data will be lost. 

To migrate without losing data, please refer to the python script and Dockerfile in `contrib/elasticsearch/es7-upgrade`. The script takes source and destination elasticsearch cluster URL and SSL configuration (if applicable) as input. It ports the mappings and settings for all indices in the source cluster to the destination cluster making the necessary changes stated above. Then it transfers all documents in the source cluster to the destination cluster. 

You can run the script in a docker container as follows
```
docker build -t migrate-es-7 .
docker run migrate-es-7 -s SOURCE -d DEST [--disable-source-ssl]
                   [--disable-dest-ssl] [--cert-file CERT_FILE]
                   [--key-file KEY_FILE] [--ca-file CA_FILE] [--create-only]
                   [-i INDICES] [--name-override NAME_OVERRIDE]
```

## Plan

We will create an "elasticsearch-5-legacy" branch with the version of master prior to the elasticsearch 7 upgrade. However, we will not be supporting this branch moving forward and all future development will be done using elasticsearch 7.9.3