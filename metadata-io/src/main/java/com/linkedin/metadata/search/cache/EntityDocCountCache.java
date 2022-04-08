package com.linkedin.metadata.search.cache;

import com.google.common.base.Suppliers;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;


public class EntityDocCountCache {
  private final EntityRegistry _entityRegistry;
  private final EntitySearchService _entitySearchService;
  private final Supplier<Map<String, Long>> entityDocCount;

  public EntityDocCountCache(EntityRegistry entityRegistry, EntitySearchService entitySearchService) {
    _entityRegistry = entityRegistry;
    _entitySearchService = entitySearchService;
    entityDocCount = Suppliers.memoizeWithExpiration(this::fetchEntityDocCount, 1, TimeUnit.MINUTES);
  }

  private Map<String, Long> fetchEntityDocCount() {
    return _entityRegistry.getEntitySpecs()
        .keySet()
        .stream()
        .collect(Collectors.toMap(Function.identity(), _entitySearchService::docCount));
  }

  @WithSpan
  public Map<String, Long> getEntityDocCount() {
    return entityDocCount.get();
  }

  public List<String> getNonEmptyEntities() {
    return getEntityDocCount().entrySet()
        .stream()
        .filter(entry -> entry.getValue() > 0)
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }
}
