package com.linkedin.metadata.recommendation.candidatesource;

import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;

/**
 * Checks whether the DataHub usage event index exists, caching the result for the lifetime of the
 * process so recommendation candidate sources don't each re-issue the same index check.
 */
@Slf4j
public class UsageEventIndexChecker {

  private static final String DATAHUB_USAGE_INDEX = "datahub_usage_event";

  private static volatile Boolean USAGE_INDEX_EXISTS = null;

  private UsageEventIndexChecker() {}

  public static boolean usageIndexExists(
      @Nonnull OperationContext opContext,
      @Nonnull SearchClientShim<?> searchClient,
      @Nonnull IndexConvention indexConvention) {
    Boolean cached = USAGE_INDEX_EXISTS;
    if (cached != null) {
      return cached;
    }
    try {
      boolean exists =
          searchClient.indexExists(
              opContext,
              new GetIndexRequest(indexConvention.getIndexName(opContext, DATAHUB_USAGE_INDEX)),
              RequestOptions.DEFAULT);
      USAGE_INDEX_EXISTS = exists;
      return exists;
    } catch (IOException e) {
      log.error("Failed to check whether DataHub usage index exists");
      return false;
    }
  }
}
