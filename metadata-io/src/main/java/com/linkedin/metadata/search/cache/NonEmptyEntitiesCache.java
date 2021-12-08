package com.linkedin.metadata.search.cache;

import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;


@RequiredArgsConstructor
public class NonEmptyEntitiesCache {
  private static final String NON_EMPTY_ENTITIES_CACHE_NAME = "nonEmptyEntities";

  private final EntityRegistry _entityRegistry;
  private final EntitySearchService _entitySearchService;
  private final CacheManager _cacheManager;

  @WithSpan
  public List<String> getNonEmptyEntities() {
    Cache.ValueWrapper cachedResult =
        _cacheManager.getCache(NON_EMPTY_ENTITIES_CACHE_NAME).get(NON_EMPTY_ENTITIES_CACHE_NAME);

    if (cachedResult != null) {
      return (List<String>) cachedResult.get();
    }

    List<String> nonEmptyEntities = _entityRegistry.getEntitySpecs()
        .keySet()
        .stream()
        .filter(entity -> _entitySearchService.docCount(entity) > 0)
        .collect(Collectors.toList());
    _cacheManager.getCache(NON_EMPTY_ENTITIES_CACHE_NAME).put(NON_EMPTY_ENTITIES_CACHE_NAME, nonEmptyEntities);
    return nonEmptyEntities;
  }
}
