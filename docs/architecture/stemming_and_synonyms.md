import FeatureAvailability from '@site/src/components/FeatureAvailability';

# About DataHub [Stemming and Synonyms Support]

<FeatureAvailability/>

This feature adds features to our current search implementation in an effort to make search results more relevant. Included improvements are:

* Stemming - Using a multi-language stemmer to allow better partial matching based on lexicographical roots i.e. "log" resolves from logs, logging. logger etc.
* Urn matching - Both partial and full Urns previously did not give desirable behavior in search results, these are now properly indexed and queried to give better matching results
* Word breaks across special characters - Previously when typing in a query like "logging_events", autocomplete would fail to resolve results after typing in the underscore until at least "logging_eve" had been typed and the same would occur with spaces. This has been resolved.
* Synonyms - A list of synonyms that will match across search results, currently static, has been added. We will be evolving this list over time to improve matching jargon versions of words to their full word equivalent. For example, typing in staging in a query can resolve datasets with stg in their name.


<!-- TODO: ADD RELEASE VERSION -->

## [Stemming and Synonyms Support] Setup, Prerequisites, and Permissions

A reindex is required for this feature to work as it modifies non-dynamic mappings and settings in the index. This reindex is carried out as a part of the bootstrapping process by
DataHub Upgrade which has been added to the helm charts and docker-compose files as a required component with default configurations that should work for most deployments.
The job uses existing credentials and permissions for ElasticSearch to achieve this. During the reindex, writes to ElasticSearch will fail, so it is recommended to schedule an outage during this time. If doing a rolling update, old versions of GMS should still be able to serve queries, but at minimum ingestion traffic needs to be stopped. Estimated downtime for instances on the order of a few million records is ~30 minutes. Larger instances may require several hours though.
Once the reindex has succeeded, a message will be sent to new GMS and MCL/MAE Consumer instances that the state is ready for them to start up. Until this time they will hold off on starting using an exponential backoff to check for readiness.

Relevant configuration for the Upgrade Job:

### Helm Values

```yaml
global:
  elasticsearch:
    ## The following section controls when and how reindexing of elasticsearch indices are performed
    index:
      ## Enable reindexing when mappings change based on the data model annotations
      enableMappingsReindex: false

      ## Enable reindexing when static index settings change.
      ## Dynamic settings which do not require reindexing are not affected
      ## Primarily this should be enabled when re-sharding is necessary for scaling/performance.
      enableSettingsReindex: false

      ## Index settings can be overridden for entity indices or other indices on an index by index basis
      ## Some index settings, such as # of shards, requires reindexing while others, i.e. replicas, do not
      ## Non-Entity indices do not require the prefix
      # settingsOverrides: '{"graph_service_v1":{"number_of_shards":"5"},"system_metadata_service_v1":{"number_of_shards":"5"}}'
      ## Entity indices do not require the prefix or suffix
      # entitySettingsOverrides: '{"dataset":{"number_of_shards":"10"}}'

      ## The amount of delay between indexing a document and having it returned in queries
      ## Increasing this value can improve performance when ingesting large amounts of data
      # refreshIntervalSeconds: 1

      ## The following options control settings for datahub-upgrade job when creating or reindexing indices
      upgrade:
        enabled: true

        ## When reindexing is required, this option will clone the existing index as a backup
        ## The clone indices are not currently managed.
        # cloneIndices: true

        ## Typically when reindexing the document counts between the original and destination indices should match.
        ## In some cases reindexing might not be able to proceed due to incompatibilities between a document in the
        ## orignal index and the new index's mappings. This document could be dropped and re-ingested or restored from
        ## the SQL database.
        ##
        ## This setting allows continuing if and only if the cloneIndices setting is also enabled which
        ## ensures a complete backup of the original index is preserved.
        # allowDocCountMismatch: false
```

### Docker Environment Variables

* ELASTICSEARCH_INDEX_BUILDER_MAPPINGS_REINDEX - Controls whether to perform a reindex for mappings mismatches
* ELASTICSEARCH_INDEX_BUILDER_SETTINGS_REINDEX - Controls whether to perform a reindex for settings mismatches
* ELASTICSEARCH_BUILD_INDICES_ALLOW_DOC_COUNT_MISMATCH - Used in conjunction with ELASTICSEARCH_BUILD_INDICES_CLONE_INDICES to allow users to skip passed document count mismatches when reindexing. Count mismatches may indicate dropped records during the reindex, so to prevent data loss this is only allowed if cloning is enabled.
* ELASTICSEARCH_BUILD_INDICES_CLONE_INDICES - Enables creating a clone of the current index to prevent data loss, default true
* ELASTICSEARCH_BUILD_INDICES_INITIAL_BACK_OFF_MILLIS - Controls the GMS and MCL Consumer backoff for checking if the reindex process has completed during start up. It is recommended to leave the defaults which will result in waiting up to ~5 minutes before killing the start-up process, allowing a new pod to attempt to start up in orchestrated deployments.
* ELASTICSEARCH_BUILD_INDICES_MAX_BACK_OFFS
* ELASTICSEARCH_BUILD_INDICES_BACK_OFF_FACTOR
* ELASTICSEARCH_BUILD_INDICES_WAIT_FOR_BUILD_INDICES - Controls whether to require waiting for the Build Indices job to finish. Defaults to true. It is not recommended to change this as it will allow GMS and MCL Consumers to start up in an error state.

## Using [Stemming and Synonyms Support]

### Stemming

Stemming uses the root of a word without suffixes to match across intent of the search when a user is not quite sure of the precise name of a resource.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/elasticsearch_optimization/eso-stemming_1.png"/>
</p>

In this first image stemming is shown in the results. Even though the query is "event", the results contain instances with "events."

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/elasticsearch_optimization/eso-stemming_2.png"/>
</p>

The second image exemplifies stemming on a query. The query is for "events", but the results show resources containing "event" as well.

### Urn Matching

Previously queries were not properly parsing out and tokenizing the expected portions of Urn types. Changes have been made on the index mapping and query side to support various partial and full Urn matching.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/elasticsearch_optimizations/eso-exact_match.png"/>
</p>

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/elasticsearch_optimizations/eso-partial_urn_1.png"/>
</p>

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/elasticsearch_optimizations/eso-partial_urn_2.png"/>
</p>


### Synonyms

Synonyms includes a static list of equivalent terms that are baked into the index at index creation time. This allows for efficient indexing of related terms. It is possible to add these to the query side as well to
allow for dynamic synonyms, but this is unsupported at this time and has performance implications.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/elasticsearch_optimizations/eso-synonyms_1.png"/>
</p>

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/elasticsearch_optimizations/eso-synonyms_2.png"/>
</p>

### Autocomplete improvements

Improvements were made to autocomplete handling around special characters like underscores and spaces.

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/elasticsearch_optimizations/eso-autocomplete_1.png"/>
</p>

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/elasticsearch_optimizations/eso-autocomplete_2.png"/>
</p>

<p align="center">
  <img width="70%"  src="https://raw.githubusercontent.com/datahub-project/static-assets/main/imgs/elasticsearch_optimizations/eso-autocomplete_3.png"/>
</p>

## Additional Resources

### Videos


**DataHub TownHall: Search Improvements Preview**

<p align="center">
<iframe width="560" height="315" src="https://www.youtube.com/embed/ECxIMbKwuOY?start=1529" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" allowfullscreen></iframe>
</p>

## FAQ and Troubleshooting

*Need more help? Join the conversation in [Slack](http://slack.datahubproject.io)!*
