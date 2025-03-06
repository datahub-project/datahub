package com.linkedin.metadata.search.client;

import static com.linkedin.metadata.search.client.CachingEntitySearchService.*;

import com.linkedin.common.urn.Urn;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.javatuples.Octet;
import org.javatuples.Septet;

class UrnCacheKeyMatcher implements CacheKeyMatcher {
  private final List<Urn> urns;
  private final Set<String> entityTypes;

  final List<String> SUPPORTED_CACHE_NAMES =
      Arrays.asList(
          ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME, ENTITY_SEARCH_SERVICE_SCROLL_CACHE_NAME);

  UrnCacheKeyMatcher(List<Urn> urns) {
    this.urns = urns;
    this.entityTypes = new HashSet<>();
    urns.forEach(
        urn -> {
          this.entityTypes.add(urn.getEntityType());
        });
  }

  @Override
  public boolean supportsCache(String cacheName) {
    return SUPPORTED_CACHE_NAMES.contains(cacheName);
  }

  @Override
  public boolean match(String cacheName, Object key) {
    switch (cacheName) {
      case ENTITY_SEARCH_SERVICE_SEARCH_CACHE_NAME:
        return matchSearchServiceCacheKey(key);
      case ENTITY_SEARCH_SERVICE_SCROLL_CACHE_NAME:
        return matchSearchServiceScrollCacheKey(key);
    }
    return false;
  }

  private boolean matchSearchServiceScrollCacheKey(Object key) {
    Octet<?, List<String>, String, String, ?, ?, List<String>, ?> cacheKey =
        (Octet<?, List<String>, String, String, ?, ?, List<String>, ?>) key;
    // For reference - cache key contents
    //       @Nonnull OperationContext opContext,
    //       @Nonnull List<String> entities,
    //       @Nonnull String query,
    //       @Nullable Filter filters,
    //       List<SortCriterion> sortCriteria,
    //       @Nullable String scrollId,
    //       @Nonnull List<String> facets
    //       int size,
    List<String> entitiesInCacheKey = (List<String>) cacheKey.getValue(1);
    String filter = (String) cacheKey.getValue(3);
    String query = (String) cacheKey.getValue(2);
    List<String> facets = (List<String>) cacheKey.getValue(6);

    if (filter == null) {
      filter = "";
    }
    filter += " " + String.join(" ", facets);
    // Facets may contain urns. Since the check for urns in filters is similar, can append it to the
    // filter.
    return isKeyImpactedByEntity(entitiesInCacheKey, query, filter);
  }

  private boolean matchSearchServiceCacheKey(Object key) {
    Septet<?, List<String>, ?, String, ?, ?, ?> cacheKey =
        (Septet<?, List<String>, ?, String, ?, ?, ?>) key;
    // For reference
    //      @Nonnull OperationContext opContext,
    //      @Nonnull List<String> entityNames,
    //      @Nonnull String query,
    //      @Nullable Filter filters,
    //      List<SortCriterion> sortCriteria,
    //      @Nonnull List<String> facets
    //      querySize

    List<String> entitiesInCacheKey = (List<String>) cacheKey.getValue(1);
    String filter = (String) cacheKey.getValue(3);
    String query = (String) cacheKey.getValue(2);
    List<String> facets = (List<String>) cacheKey.getValue(5);

    // Facets may contain urns. Since the check for urns in filters is similar, can append it to the
    // filter.
    if (filter == null) {
      filter = "";
    }
    filter += " " + String.join(" ", facets);

    return isKeyImpactedByEntity(entitiesInCacheKey, query, filter);
  }

  boolean isKeyImpactedByEntity(List<String> entitiesInCacheKey, String query, String filter) {
    boolean entityMatch = entitiesInCacheKey.stream().anyMatch(entityTypes::contains);
    if (!entityMatch) {
      return false;
    }

    // Ignoring query for now. A query could make this cache entry more targeted, but till there is
    // a quick way to evaluate if the entities that were updated are affected by this query,
    // ignoring it may mean some cache entries are invalidated even if they may not be a match,
    // and an uncached query result will still be fetched.

    boolean containsUrn = filter.contains("urn:li");
    if (!containsUrn) {
      return true; // Entity match, has a filter, but not on urn. this may be a suboptimal
    }

    return urns.stream()
        .anyMatch(
            urn ->
                filter.contains(
                    urn.toString())); // If we found an exact URN match, this is to be evicted. If

    // this entry was for some other urn, do not evict.
  }
}
