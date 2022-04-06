package com.linkedin.metadata.search.cache;

import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;


@RequiredArgsConstructor
public class EntityDocCountCache {
  private static final String CACHE_NAME = "entityDocCount";

  private final EntityRegistry _entityRegistry;
  private final EntitySearchService _entitySearchService;
  private final CacheManager _cacheManager;

  @WithSpan
  public Map<String, Long> getEntityDocCount(boolean skipCache) {
    Cache.ValueWrapper cachedResult = _cacheManager.getCache(CACHE_NAME).get(CACHE_NAME);

    if (!skipCache && cachedResult != null) {
      return (Map<String, Long>) cachedResult.get();
    }

    Map<String, Long> docCountPerEntity = _entityRegistry.getEntitySpecs()
        .keySet()
        .stream()
        .collect(Collectors.toMap(Function.identity(), _entitySearchService::docCount));
    _cacheManager.getCache(CACHE_NAME).put(CACHE_NAME, docCountPerEntity);
    return docCountPerEntity;
  }

  public List<String> getNonEmptyEntities() {
    return getEntityDocCount(false).entrySet()
        .stream()
        .filter(entry -> entry.getValue() > 0)
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }
}
