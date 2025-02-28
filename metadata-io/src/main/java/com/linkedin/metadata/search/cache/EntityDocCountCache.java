package com.linkedin.metadata.search.cache;

import com.google.common.base.Suppliers;
import com.linkedin.metadata.config.cache.EntityDocCountCacheConfiguration;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.utils.ConcurrencyUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

public class EntityDocCountCache {
  private final EntityRegistry entityRegistry;
  private final EntitySearchService entitySearchService;
  private final EntityDocCountCacheConfiguration config;
  private final Map<EntityDocCountsKey, Supplier<Map<String, Long>>> entityDocCounts;

  @AllArgsConstructor
  @EqualsAndHashCode
  private static final class EntityDocCountsKey {
    private final String searchContextId;
    private final Filter filter;
  }

  public EntityDocCountCache(
      EntityRegistry entityRegistry,
      EntitySearchService entitySearchService,
      EntityDocCountCacheConfiguration config) {
    this.config = config;
    this.entityRegistry = entityRegistry;
    this.entitySearchService = entitySearchService;
    this.entityDocCounts = new ConcurrentHashMap<>();
  }

  private Map<String, Long> fetchEntityDocCount(
      @Nonnull OperationContext opContext, @Nullable Filter filter) {
    return ConcurrencyUtils.transformAndCollectAsync(
        entityRegistry.getEntitySpecs().keySet(),
        Function.identity(),
        Collectors.toMap(
            Function.identity(), v -> entitySearchService.docCount(opContext, v, filter)));
  }

  @WithSpan
  public Map<String, Long> getEntityDocCount(@Nonnull OperationContext opContext) {
    return getEntityDocCount(opContext, null);
  }

  @WithSpan
  public Map<String, Long> getEntityDocCount(
      @Nonnull OperationContext opContext, @Nullable Filter filter) {
    return entityDocCounts
        .computeIfAbsent(
            new EntityDocCountsKey(opContext.getSearchContextId(), filter),
            k -> buildSupplier(opContext, filter))
        .get();
  }

  public List<String> getNonEmptyEntities(@Nonnull OperationContext opContext) {
    return getEntityDocCount(opContext).entrySet().stream()
        .filter(entry -> entry.getValue() > 0)
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }

  private Supplier<Map<String, Long>> buildSupplier(
      @Nonnull OperationContext opContext, @Nullable Filter filter) {
    return Suppliers.memoizeWithExpiration(
        () -> fetchEntityDocCount(opContext, filter), config.getTtlSeconds(), TimeUnit.SECONDS);
  }
}
