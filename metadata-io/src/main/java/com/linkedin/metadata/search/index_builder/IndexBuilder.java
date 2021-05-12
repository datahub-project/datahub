package com.linkedin.metadata.search.index_builder;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.IndexSetting;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.IndexType;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
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
  private final EntitySpec entitySpec;
  private final String indexName;

  public void buildIndex() throws IOException {
    log.info("Setting up index: {}", indexName);
    Map<String, Object> mappings = MappingsBuilder.getMappings(entitySpec);
    Map<String, Object> settings = SettingsBuilder.getSettings(getMaxNgramDiff());

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
    Settings oldSettings = searchClient.indices()
        .getSettings(new GetSettingsRequest().indices(indexName), RequestOptions.DEFAULT)
        .getIndexToSettings()
        .values()
        .toArray(Settings.class)[0].getAsSettings("index");
    MapDifference<String, Object> mappingsDiff = Maps.difference(mappings, oldMappings);
    // If there are no updates to mappings, return
    if (mappingsDiff.areEqual()) {
      log.info("No updates to index {}", indexName);
      return;
    }

    log.info("There's diff between new mappings (left) and old mappings (right): {}", mappingsDiff.toString());
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

    // Check if reindex succeeded by comparing document counts
    long originalCount = getCount(indexName);
    long reindexedCount = getCount(tempIndexName);
    if (originalCount != reindexedCount) {
      log.info("Post-reindex document count is different, source_doc_count: {} reindex_doc_count: {}", originalCount,
          reindexedCount);
      searchClient.indices().delete(new DeleteIndexRequest().indices(tempIndexName), RequestOptions.DEFAULT);
      throw new RuntimeException(String.format("Reindex from %s to %s failed", indexName, tempIndexName));
    }

    log.info("Reindex from {} to {} succeeded", indexName, tempIndexName);
    // Check if the original index is aliased or not
    GetAliasesResponse aliasesResponse =
        searchClient.indices().getAlias(new GetAliasesRequest(indexName), RequestOptions.DEFAULT);
    // If not aliased, delete the original index
    if (aliasesResponse.getAliases().isEmpty()) {
      searchClient.indices().delete(new DeleteIndexRequest().indices(indexName), RequestOptions.DEFAULT);
    } else {
      searchClient.indices()
          .delete(new DeleteIndexRequest().indices(aliasesResponse.getAliases().keySet().toArray(new String[0])),
              RequestOptions.DEFAULT);
    }

    // Add alias for the new index
    AliasActions removeAction = AliasActions.remove().alias(indexName).index("*");
    AliasActions addAction = AliasActions.add().alias(indexName).index(tempIndexName);
    searchClient.indices()
        .updateAliases(new IndicesAliasesRequest().addAliasAction(removeAction).addAliasAction(addAction),
            RequestOptions.DEFAULT);
    log.info("Finished setting up {}", indexName);
  }

  private long getCount(String indexName) throws IOException {
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

  // Get maximum diff between max_gram and min_gram, which are only set for partial filters
  private Optional<Integer> getMaxNgramDiff() {
    Set<IndexType> allIndexTypes = entitySpec.getSearchableFieldSpecs()
        .stream()
        .map(SearchableFieldSpec::getIndexSettings)
        .flatMap(List::stream)
        .map(IndexSetting::getIndexType)
        .collect(Collectors.toSet());
    if (allIndexTypes.contains(IndexType.PARTIAL_LONG)) {
      // max_gram: 50, min_gram: 3
      return Optional.of(47);
    }
    if (allIndexTypes.contains(IndexType.PARTIAL_SHORT)) {
      // max_gram: 20, min_gram: 1
      return Optional.of(19);
    }
    if (allIndexTypes.contains(IndexType.PARTIAL) || allIndexTypes.contains(IndexType.PARTIAL_PATTERN)) {
      // max_gram: 20, min_gram: 3
      return Optional.of(17);
    }
    return Optional.empty();
  }
}
