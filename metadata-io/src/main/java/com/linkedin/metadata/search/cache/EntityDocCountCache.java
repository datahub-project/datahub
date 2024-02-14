package com.linkedin.metadata.search.cache;

import com.google.common.base.Suppliers;
import com.linkedin.metadata.config.cache.EntityDocCountCacheConfiguration;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.utils.ConcurrencyUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class EntityDocCountCache {
  private final OperationContext opContext;
  private final EntityRegistry _entityRegistry;
  private final EntitySearchService _entitySearchService;
  private final Supplier<Map<String, Long>> entityDocCount;

  public EntityDocCountCache(
      @Nonnull OperationContext opContext,
      EntityRegistry entityRegistry,
      EntitySearchService entitySearchService,
      EntityDocCountCacheConfiguration config) {
    this.opContext = opContext;
    _entityRegistry = entityRegistry;
    _entitySearchService = entitySearchService;
    entityDocCount =
        Suppliers.memoizeWithExpiration(
            this::fetchEntityDocCount, config.getTtlSeconds(), TimeUnit.SECONDS);
  }

  private Map<String, Long> fetchEntityDocCount() {
    return ConcurrencyUtils.transformAndCollectAsync(
        _entityRegistry.getEntitySpecs().keySet(),
        Function.identity(),
        Collectors.toMap(Function.identity(), v -> _entitySearchService.docCount(v, null)));
  }

  @WithSpan
  public Map<String, Long> getEntityDocCount() {
    return entityDocCount.get();
  }

  public List<String> getNonEmptyEntities() {
    return getEntityDocCount().entrySet().stream()
        .filter(entry -> entry.getValue() > 0)
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }
}
