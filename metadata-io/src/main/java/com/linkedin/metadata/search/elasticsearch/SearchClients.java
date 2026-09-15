package com.linkedin.metadata.search.elasticsearch;

import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.action.search.SearchRequest;

/**
 * Picks the search client for a request from {@link SearchClusterAccess} on the operation, not from
 * a DAO field. Keyword V2 vs V3 is decided by {@link EntitySearchIndexResolver}; the client follows
 * that component (or the request's entity index names).
 */
public final class SearchClients {

  private SearchClients() {}

  /** Component that serves keyword entity search for the current cutover flags. */
  @Nonnull
  public static SearchComponent keywordComponent(@Nullable ElasticSearchConfiguration config) {
    return keywordComponent(config == null ? null : config.getEntityIndex());
  }

  @Nonnull
  public static SearchComponent keywordComponent(@Nullable EntityIndexConfiguration entityIndex) {
    return EntitySearchIndexResolver.shouldReadV3(entityIndex)
        ? SearchComponent.SEARCH_V3
        : SearchComponent.SEARCH_V2;
  }

  /**
   * Route from the request's target indices. Cutover flags are unused here: the index names already
   * encode V2 vs V3 vs semantic.
   */
  @Nonnull
  public static SearchClientShim<?> forEntityIndices(
      @Nonnull OperationContext opContext, @Nonnull SearchRequest searchRequest) {
    return forEntityIndices(opContext, searchRequest.indices());
  }

  /**
   * Route from resolved entity index names. Prefer {@link #forEntityIndices(OperationContext,
   * SearchRequest)} when a request is already built.
   */
  @Nonnull
  public static SearchClientShim<?> forEntityIndices(
      @Nonnull OperationContext opContext, @Nullable String... indices) {
    return forEntityIndices(opContext, (EntityIndexConfiguration) null, indices);
  }

  @Nonnull
  public static SearchClientShim<?> forEntityIndices(
      @Nonnull OperationContext opContext,
      @Nullable ElasticSearchConfiguration config,
      @Nullable String... indices) {
    return forEntityIndices(opContext, config == null ? null : config.getEntityIndex(), indices);
  }

  @Nonnull
  public static SearchClientShim<?> forEntityIndices(
      @Nonnull OperationContext opContext,
      @Nullable EntityIndexConfiguration entityIndex,
      @Nullable String... indices) {
    SearchClusterAccess access = opContext.getSearchContext().requireSearchClusterAccess();
    if (indices == null || indices.length == 0) {
      return access.clientFor(keywordComponent(entityIndex));
    }
    SearchComponent family =
        SearchClusterAccess.componentForEntityIndicesOrNull(
            opContext.getSearchContext().getIndexConvention(), indices);
    if (family == null) {
      throw new IllegalArgumentException(
          "Index list is not a Search V2, V3, or semantic entity family: "
              + Arrays.toString(indices)
              + ". Unrecognized names are a setup error; do not route them to the keyword cluster.");
    }
    return access.clientFor(family);
  }

  @Nonnull
  public static SearchClientShim<?> forComponent(
      @Nonnull OperationContext opContext, @Nonnull SearchComponent component) {
    return opContext.getSearchContext().requireSearchClusterAccess().clientFor(component);
  }
}
