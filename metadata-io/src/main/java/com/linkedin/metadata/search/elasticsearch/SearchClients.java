package com.linkedin.metadata.search.elasticsearch;

import static com.linkedin.metadata.Constants.DATAHUB_USAGE_EVENT_INDEX;

import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.EntitySearchIndexResolver;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
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
   * True when {@code indexName} is the usage-event index, either as the unprefixed constant or as
   * the name resolved through this operation's {@link IndexConvention}.
   */
  public static boolean isUsageIndex(
      @Nonnull OperationContext opContext, @Nullable String indexName) {
    return isUsageIndex(opContext, opContext.getSearchContext().getIndexConvention(), indexName);
  }

  public static boolean isUsageIndex(
      @Nonnull OperationContext opContext,
      @Nonnull IndexConvention convention,
      @Nullable String indexName) {
    if (indexName == null || indexName.isEmpty()) {
      return false;
    }
    if (indexName.equals(DATAHUB_USAGE_EVENT_INDEX)) {
      return true;
    }
    return indexName.equals(convention.getIndexName(opContext, DATAHUB_USAGE_EVENT_INDEX));
  }

  /**
   * Component that owns a DataHub-managed index: entity families, usage, graph, or system-metadata
   * (including zero-downtime backing names that share the concrete prefix).
   */
  @Nonnull
  public static SearchComponent componentForManagedIndex(
      @Nonnull OperationContext opContext, @Nonnull String indexOrPattern) {
    return componentForManagedIndex(
        opContext, opContext.getSearchContext().getIndexConvention(), indexOrPattern);
  }

  @Nonnull
  public static SearchComponent componentForManagedIndex(
      @Nonnull OperationContext opContext,
      @Nonnull IndexConvention convention,
      @Nonnull String indexOrPattern) {
    SearchComponent entityFamily =
        SearchClusterAccess.tryComponentForEntityIndex(convention, indexOrPattern);
    if (entityFamily != null) {
      return entityFamily;
    }
    if (isUsageIndex(opContext, convention, indexOrPattern)) {
      return SearchComponent.USAGE;
    }
    String graphIndex = convention.getIndexName(opContext, ElasticSearchGraphService.INDEX_NAME);
    String systemMetadataIndex =
        convention.getIndexName(opContext, ElasticSearchSystemMetadataService.INDEX_NAME);
    if (matchesOwnedIndex(indexOrPattern, graphIndex)) {
      return SearchComponent.GRAPH;
    }
    if (matchesOwnedIndex(indexOrPattern, systemMetadataIndex)) {
      return SearchComponent.SYSTEM_METADATA;
    }
    throw new IllegalArgumentException(
        "Index '"
            + indexOrPattern
            + "' is not a recognized search, graph, system-metadata, or usage index. Unrecognized"
            + " names are a setup error; refusing to use the primary cluster.");
  }

  /**
   * Route a request that may target usage or entity indices. When the request has no indices (PIT
   * follow-up search), {@code config} cutover flags pick keyword V2 vs V3.
   */
  @Nonnull
  public static SearchClientShim<?> forSearchRequest(
      @Nonnull OperationContext opContext, @Nonnull SearchRequest searchRequest) {
    return forSearchRequest(opContext, searchRequest, null);
  }

  @Nonnull
  public static SearchClientShim<?> forSearchRequest(
      @Nonnull OperationContext opContext,
      @Nonnull SearchRequest searchRequest,
      @Nullable ElasticSearchConfiguration config) {
    String[] indices = searchRequest.indices();
    if (indices != null && indices.length > 0) {
      boolean anyUsage = false;
      boolean anyNonUsage = false;
      for (String index : indices) {
        if (index == null) {
          continue;
        }
        if (isUsageIndex(opContext, index)) {
          anyUsage = true;
        } else {
          anyNonUsage = true;
        }
      }
      if (anyUsage && anyNonUsage) {
        throw new IllegalArgumentException(
            "Search request mixes usage indices with other indices: "
                + Arrays.toString(indices)
                + "; those components may be on different clusters");
      }
      if (anyUsage) {
        return forComponent(opContext, SearchComponent.USAGE);
      }
    }
    return forEntityIndices(opContext, searchRequest, config);
  }

  /**
   * Route one resolved index name. Usage is classified first; everything else must be an entity
   * family (or empty, in which case {@code config} picks the keyword family).
   */
  @Nonnull
  public static SearchClientShim<?> forIndex(
      @Nonnull OperationContext opContext, @Nullable String indexName) {
    return forIndex(opContext, (ElasticSearchConfiguration) null, indexName);
  }

  @Nonnull
  public static SearchClientShim<?> forIndex(
      @Nonnull OperationContext opContext,
      @Nullable ElasticSearchConfiguration config,
      @Nullable String indexName) {
    if (isUsageIndex(opContext, indexName)) {
      return forComponent(opContext, SearchComponent.USAGE);
    }
    return forEntityIndices(opContext, config, indexName);
  }

  /**
   * Route from the request's target indices. When the request has no indices (PIT follow-up
   * search), {@code entityIndex} cutover flags pick keyword V2 vs V3.
   */
  @Nonnull
  public static SearchClientShim<?> forEntityIndices(
      @Nonnull OperationContext opContext, @Nonnull SearchRequest searchRequest) {
    return forEntityIndices(opContext, (EntityIndexConfiguration) null, searchRequest.indices());
  }

  @Nonnull
  public static SearchClientShim<?> forEntityIndices(
      @Nonnull OperationContext opContext,
      @Nonnull SearchRequest searchRequest,
      @Nullable ElasticSearchConfiguration config) {
    return forEntityIndices(opContext, config, searchRequest.indices());
  }

  /**
   * Route from resolved entity index names. Prefer {@link #forEntityIndices(OperationContext,
   * SearchRequest)} or {@link #forEntityIndices(OperationContext, SearchRequest,
   * ElasticSearchConfiguration)} when a request is already built.
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

  private static boolean matchesOwnedIndex(
      @Nonnull String nameOrPattern, @Nonnull String concreteName) {
    return nameOrPattern.equals(concreteName) || nameOrPattern.startsWith(concreteName + "_");
  }
}
