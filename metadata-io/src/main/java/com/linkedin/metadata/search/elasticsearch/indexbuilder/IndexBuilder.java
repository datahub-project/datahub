package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.ReindexRequest;


@Slf4j
@RequiredArgsConstructor
public class IndexBuilder {

  private final RestHighLevelClient searchClient;
  private final String indexName;
  private final Map<String, Object> mappings;
  private final Map<String, Object> settings;

  private static final int NUM_RETRIES = 3;

  public void buildIndex() throws IOException {
    // Check if index exists
    boolean exists = searchClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);

    // If index doesn't exist, create index
    if (!exists) {
      createIndex(indexName, mappings, settings);
      return;
    }

    Map<String, Object> oldMappings = searchClient.indices()
        .getMapping(new GetMappingsRequest().indices(indexName), RequestOptions.DEFAULT)
        .mappings()
        .values()
        .stream()
        .findFirst()
        .get()
        .getSourceAsMap();

    MapDifference<String, Object> mappingsDiff = Maps.difference(mappings, oldMappings);

    Settings oldSettings = searchClient.indices()
        .getSettings(new GetSettingsRequest().indices(indexName), RequestOptions.DEFAULT)
        .getIndexToSettings()
        .valuesIt()
        .next();

    // If there are no updates to mappings, return
    if (mappingsDiff.areEqual() && equals(settings, oldSettings)) {
      log.info("No updates to index {}", indexName);
      return;
    }

    if (!mappingsDiff.areEqual()) {
      log.info("There's diff between new mappings (left) and old mappings (right): {}", mappingsDiff.toString());
    } else {
      log.info("There's an update to settings");
    }

    String tempIndexName = indexName + "_" + System.currentTimeMillis();
    createIndex(tempIndexName, mappings, settings);
    try {
      searchClient.reindex(new ReindexRequest().setSourceIndices(indexName).setDestIndex(tempIndexName),
          RequestOptions.DEFAULT);
    } catch (Exception e) {
      log.info("Failed to reindex {} to {}: Exception {}", indexName, tempIndexName, e.toString());
      searchClient.indices().delete(new DeleteIndexRequest().indices(tempIndexName), RequestOptions.DEFAULT);
      throw e;
    }

    // Check whether reindex succeeded by comparing document count
    // There can be some delay between the reindex finishing and count being fully up to date, so try multiple times
    long originalCount = 0;
    long reindexedCount = 0;
    for (int i = 0; i < NUM_RETRIES; i++) {
      // Check if reindex succeeded by comparing document counts
      originalCount = getCount(indexName);
      reindexedCount = getCount(tempIndexName);
      if (originalCount == reindexedCount) {
        break;
      }
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    if (originalCount != reindexedCount) {
      log.info("Post-reindex document count is different, source_doc_count: {} reindex_doc_count: {}", originalCount,
          reindexedCount);
      searchClient.indices().delete(new DeleteIndexRequest().indices(tempIndexName), RequestOptions.DEFAULT);
      throw new RuntimeException(String.format("Reindex from %s to %s failed", indexName, tempIndexName));
    }

    log.info("Reindex from {} to {} succeeded", indexName, tempIndexName);
    String indexNamePattern = indexName + "_*";
    // Check if the original index is aliased or not
    GetAliasesResponse aliasesResponse = searchClient.indices()
        .getAlias(new GetAliasesRequest(indexName).indices(indexNamePattern), RequestOptions.DEFAULT);
    // If not aliased, delete the original index
    if (aliasesResponse.getAliases().isEmpty()) {
      searchClient.indices().delete(new DeleteIndexRequest().indices(indexName), RequestOptions.DEFAULT);
    } else {
      searchClient.indices()
          .delete(new DeleteIndexRequest().indices(aliasesResponse.getAliases().keySet().toArray(new String[0])),
              RequestOptions.DEFAULT);
    }

    // Add alias for the new index
    AliasActions removeAction = AliasActions.remove().alias(indexName).index(indexNamePattern);
    AliasActions addAction = AliasActions.add().alias(indexName).index(tempIndexName);
    searchClient.indices()
        .updateAliases(new IndicesAliasesRequest().addAliasAction(removeAction).addAliasAction(addAction),
            RequestOptions.DEFAULT);
    log.info("Finished setting up {}", indexName);
  }

  private long getCount(@Nonnull String indexName) throws IOException {
    return searchClient.count(new CountRequest(indexName).query(QueryBuilders.matchAllQuery()), RequestOptions.DEFAULT)
        .getCount();
  }

  private void createIndex(String indexName, Map<String, Object> mappings, Map<String, Object> settings)
      throws IOException {
    log.info("Index {} does not exist. Creating", indexName);
    CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
    createIndexRequest.mapping(mappings);
    createIndexRequest.settings(settings);
    searchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
    log.info("Created index {}", indexName);
  }

  private boolean equals(Map<String, Object> newSettings, Settings oldSettings) {
    if (!newSettings.containsKey("index") || !((Map<String, Object>) newSettings.get("index")).containsKey(
        "analysis")) {
      return true;
    }
    Map<String, Object> newAnalysis =
        (Map<String, Object>) ((Map<String, Object>) newSettings.get("index")).get("analysis");
    Settings oldAnalysis = oldSettings.getByPrefix("index.analysis.");
    return equalsGroup(newAnalysis, oldAnalysis);
  }

  private boolean equalsGroup(Map<String, Object> newSettings, Settings oldSettings) {
    if (!newSettings.keySet().equals(oldSettings.names())) {
      return false;
    }

    for (String key : newSettings.keySet()) {
      if (newSettings.get(key) instanceof Map) {
        if (!equalsGroup((Map<String, Object>) newSettings.get(key), oldSettings.getByPrefix(key + "."))) {
          return false;
        }
      } else if (newSettings.get(key) instanceof List) {
        if (!newSettings.get(key).equals(oldSettings.getAsList(key))) {
          return false;
        }
      } else {
        if (!newSettings.get(key).toString().equals(oldSettings.get(key))) {
          return false;
        }
      }
    }
    return true;
  }
}
