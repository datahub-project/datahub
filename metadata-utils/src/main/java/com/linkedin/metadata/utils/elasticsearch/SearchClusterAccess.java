package com.linkedin.metadata.utils.elasticsearch;

import com.linkedin.metadata.config.search.SearchComponent;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;

/**
 * Resolves which search HTTP client serves a subsystem or an entity index family.
 *
 * <p>This is a lookup, not a connection: implementations typically wrap the process-wide cluster
 * registry. Live clients must not be stored as "the" client on {@code OperationContext}; callers
 * ask for a {@link SearchComponent} or a resolved entity index name.
 *
 * <p>Graph, timeseries, system-metadata and usage indices are <i>not</i> entity families. Those
 * call sites must use {@link #clientFor(SearchComponent)}. {@link #clientForIndex} only accepts
 * Search V2 / V3 / semantic names (including zero-downtime-update backing indices).
 */
public interface SearchClusterAccess {

  /** Client for a named subsystem, following {@code elasticsearch.componentCluster}. */
  @Nonnull
  SearchClientShim<?> clientFor(@Nonnull SearchComponent component);

  /**
   * Client that owns a resolved entity or semantic index name. Throws if the name is not an entity
   * family — use {@link #clientFor(SearchComponent)} for graph / timeseries / usage.
   */
  @Nonnull
  default SearchClientShim<?> clientForIndex(
      @Nonnull IndexConvention convention, @Nonnull String indexName) {
    return clientFor(componentForEntityIndex(convention, indexName));
  }

  @Nonnull
  default SearchClientShim.SearchEngineType engineTypeFor(@Nonnull SearchComponent component) {
    return clientFor(component).getEngineType();
  }

  /**
   * Client for a request whose indices all belong to one entity family. Mixed V2/V3 in one request
   * is rejected: those families can live on different clusters.
   */
  @Nonnull
  default SearchClientShim<?> clientForEntityIndices(
      @Nonnull IndexConvention convention, @Nonnull String[] indexNames) {
    SearchComponent component = componentForEntityIndicesOrNull(convention, indexNames);
    if (component == null) {
      throw new IllegalArgumentException(
          "Index list is not a Search V2, V3, or semantic entity family. Use clientFor("
              + "SearchComponent) for graph, timeseries, system-metadata, or usage.");
    }
    return clientFor(component);
  }

  /**
   * Shared family when every name is a V2 / V3 / semantic entity index (or a wildcard of one).
   * {@code null} when the list is empty, or when none of the names are entity families. Mixed
   * recognized families, or entity names mixed with non-entity names, throw. Callers that received
   * a non-empty list must treat {@code null} as an error rather than routing to keyword.
   */
  @Nullable
  static SearchComponent componentForEntityIndicesOrNull(
      @Nonnull IndexConvention convention, @Nullable String[] indexNames) {
    if (indexNames == null || indexNames.length == 0) {
      return null;
    }
    SearchComponent resolved = null;
    boolean unrecognized = false;
    for (String indexName : indexNames) {
      SearchComponent next = tryComponentForEntityIndex(convention, indexName);
      if (next == null) {
        unrecognized = true;
        continue;
      }
      if (resolved == null) {
        resolved = next;
      } else if (resolved != next) {
        throw new IllegalArgumentException(
            "Search request mixes index families "
                + resolved
                + " and "
                + next
                + "; those components may be on different clusters");
      }
    }
    if (resolved != null && unrecognized) {
      throw new IllegalArgumentException(
          "Search request mixes entity index families with non-entity indices; those may be on "
              + "different clusters");
    }
    return resolved;
  }

  /**
   * Entity-family component, or {@code null} when the name is not V2 / V3 / semantic. Wildcards
   * that still carry a family marker ({@code *index_v3*}) resolve to that family.
   */
  @Nullable
  static SearchComponent tryComponentForEntityIndex(
      @Nonnull IndexConvention convention, @Nonnull String indexName) {
    try {
      return componentForEntityIndex(convention, indexName);
    } catch (IllegalArgumentException ignored) {
      if (IndexConvention.matchesSemanticEntityIndexFamily(indexName)) {
        return SearchComponent.SEMANTIC;
      }
      if (IndexConvention.matchesV3EntityIndexFamily(indexName)) {
        return SearchComponent.SEARCH_V3;
      }
      if (IndexConvention.matchesV2EntityIndexFamily(indexName)) {
        return SearchComponent.SEARCH_V2;
      }
      return null;
    }
  }

  /**
   * Maps a resolved entity index name to SEARCH_V2, SEARCH_V3 or SEMANTIC. Other index families
   * (graph, timeseries, usage, system-metadata) are not classified as V2.
   */
  @Nonnull
  static SearchComponent componentForEntityIndex(
      @Nonnull IndexConvention convention, @Nonnull String indexName) {
    if (convention.isSemanticEntityIndexType(indexName)) {
      return SearchComponent.SEMANTIC;
    }
    if (convention.isV3EntityIndexOrBackingType(indexName)) {
      return SearchComponent.SEARCH_V3;
    }
    if (convention.isV2EntityIndexOrBackingType(indexName)) {
      return SearchComponent.SEARCH_V2;
    }
    throw new IllegalArgumentException(
        "Index '"
            + indexName
            + "' is not a Search V2, V3, or semantic entity index. Use clientFor(SearchComponent) "
            + "for graph, timeseries, system-metadata, or usage.");
  }

  /**
   * Single-cluster stub for tests: every component and entity index resolves to the same client.
   * Production wiring uses the cluster registry instead.
   */
  @Nonnull
  static SearchClusterAccess fixed(@Nonnull SearchClientShim<?> client) {
    return new FixedSearchClusterAccess(client);
  }

  @EqualsAndHashCode
  final class FixedSearchClusterAccess implements SearchClusterAccess {
    private final SearchClientShim<?> client;

    private FixedSearchClusterAccess(@Nonnull SearchClientShim<?> client) {
      this.client = client;
    }

    @Override
    @Nonnull
    public SearchClientShim<?> clientFor(@Nonnull SearchComponent component) {
      return client;
    }
  }
}
