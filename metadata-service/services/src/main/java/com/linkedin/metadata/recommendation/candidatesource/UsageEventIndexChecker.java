package com.linkedin.metadata.recommendation.candidatesource;

import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;

/**
 * Checks whether the DataHub usage event index exists, caching the result per index name for the
 * lifetime of the process so recommendation candidate sources don't each re-issue the same index
 * check. Keyed by index name to stay correct across tenants/index prefixes.
 */
@Slf4j
public class UsageEventIndexChecker {

  private static final String DATAHUB_USAGE_INDEX = "datahub_usage_event";

  private static final int MAX_CACHE_SIZE = 1000;

  private static final ConcurrentHashMap<String, Boolean> USAGE_INDEX_EXISTS_CACHE =
      new ConcurrentHashMap<>();

  private UsageEventIndexChecker() {}

  public static boolean usageIndexExists(
      @Nonnull OperationContext opContext,
      @Nonnull SearchClientShim<?> searchClient,
      @Nonnull IndexConvention indexConvention) {
    String indexName = indexConvention.getIndexName(opContext, DATAHUB_USAGE_INDEX);
    Boolean cached = USAGE_INDEX_EXISTS_CACHE.get(indexName);
    if (cached != null) {
      return cached;
    }
    try {
      boolean exists =
          searchClient.indexExists(
              opContext, new GetIndexRequest(indexName), RequestOptions.DEFAULT);
      if (USAGE_INDEX_EXISTS_CACHE.size() < MAX_CACHE_SIZE) {
        USAGE_INDEX_EXISTS_CACHE.put(indexName, exists);
      }
      return exists;
    } catch (IOException e) {
      log.error("Failed to check whether DataHub usage index exists");
      return false;
    }
  }
}
