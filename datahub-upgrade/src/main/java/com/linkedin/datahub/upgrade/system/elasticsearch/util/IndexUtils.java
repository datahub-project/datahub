package com.linkedin.datahub.upgrade.system.elasticsearch.util;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.client.GetAliasesResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;

@Slf4j
public class IndexUtils {
  public static final String INDEX_BLOCKS_WRITE_SETTING = "index.blocks.write";
  public static final int INDEX_BLOCKS_WRITE_RETRY = 4;
  public static final int INDEX_BLOCKS_WRITE_WAIT_SECONDS = 10;

  private IndexUtils() {}

  private static List<ReindexConfig> _reindexConfigs = new ArrayList<>();

  public static List<ReindexConfig> getAllReindexConfigs(
      List<ElasticSearchIndexed> elasticSearchIndexedList,
      Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties)
      throws IOException {
    // Avoid locking & reprocessing
    List<ReindexConfig> reindexConfigs = new ArrayList<>(_reindexConfigs);
    if (reindexConfigs.isEmpty()) {
      for (ElasticSearchIndexed elasticSearchIndexed : elasticSearchIndexedList) {
        reindexConfigs.addAll(elasticSearchIndexed.buildReindexConfigs(structuredProperties));
      }
      _reindexConfigs = new ArrayList<>(reindexConfigs);
    }

    return reindexConfigs;
  }

  public static boolean validateWriteBlock(
      RestHighLevelClient esClient, String indexName, boolean expectedState)
      throws IOException, InterruptedException {
    final String finalIndexName = resolveAlias(esClient, indexName);

    GetSettingsRequest request =
        new GetSettingsRequest()
            .indices(finalIndexName)
            .names(INDEX_BLOCKS_WRITE_SETTING)
            .includeDefaults(true);

    int count = INDEX_BLOCKS_WRITE_RETRY;
    while (count > 0) {
      GetSettingsResponse response =
          esClient.indices().getSettings(request, RequestOptions.DEFAULT);
      if (response
          .getSetting(finalIndexName, INDEX_BLOCKS_WRITE_SETTING)
          .equals(String.valueOf(expectedState))) {
        return true;
      }
      count = count - 1;

      if (count != 0) {
        Thread.sleep(INDEX_BLOCKS_WRITE_WAIT_SECONDS * 1000);
      }
    }

    return false;
  }

  public static String resolveAlias(RestHighLevelClient esClient, String indexName)
      throws IOException {
    String finalIndexName = indexName;

    GetAliasesResponse aliasResponse =
        esClient.indices().getAlias(new GetAliasesRequest(indexName), RequestOptions.DEFAULT);

    if (!aliasResponse.getAliases().isEmpty()) {
      Set<String> indices = aliasResponse.getAliases().keySet();
      if (indices.size() != 1) {
        throw new NotImplementedException(
            String.format(
                "Clone not supported for %s indices in alias %s. Indices: %s",
                indices.size(), indexName, String.join(",", indices)));
      }
      finalIndexName = indices.stream().findFirst().get();
      log.info("Alias {} resolved to index {}", indexName, finalIndexName);
    }

    return finalIndexName;
  }
}
