package com.linkedin.datahub.graphql;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;

/**
 * The purpose of this class is to avoid loading 42+ dataLoaders when many of the graphql queries do
 * not use all of them.
 */
@Slf4j
public class LazyDataLoaderRegistry extends DataLoaderRegistry {
  private final QueryContext queryContext;
  private final Map<String, Function<QueryContext, DataLoader<?, ?>>> dataLoaderSuppliers;
  private volatile Context otelContext = null;

  public LazyDataLoaderRegistry(
      QueryContext queryContext,
      Map<String, Function<QueryContext, DataLoader<?, ?>>> dataLoaderSuppliers) {
    super();
    this.queryContext = queryContext;
    this.dataLoaderSuppliers = new ConcurrentHashMap<>(dataLoaderSuppliers);
  }

  /**
   * Called by {@link
   * com.linkedin.datahub.graphql.instrumentation.OtelContextCaptureInstrumentation} at the start of
   * each GraphQL execution, once the OTel operation span is active. Stores the OTel {@link Context}
   * (not the DataHub {@code OperationContext}) so that DataLoader creation can re-activate it.
   */
  public void setOtelContext(Context ctx) {
    this.otelContext = ctx;
  }

  @Override
  public <K, V> DataLoader<K, V> getDataLoader(String key) {
    return super.computeIfAbsent(
        key,
        k -> {
          Function<QueryContext, DataLoader<?, ?>> supplier = dataLoaderSuppliers.get(key);
          if (supplier == null) {
            throw new IllegalArgumentException("No DataLoader registered for key: " + key);
          }
          // Make the OTel operation context current while the DataLoader is created so that
          // createDataLoader can capture it via Context.current() for batch span parenting.
          Context ctx = otelContext;
          if (ctx != null) {
            try (Scope ignored = ctx.makeCurrent()) {
              return supplier.apply(queryContext);
            }
          }
          return supplier.apply(queryContext);
        });
  }

  @Override
  public Set<String> getKeys() {
    return Stream.concat(dataLoaders.keySet().stream(), dataLoaderSuppliers.keySet().stream())
        .collect(Collectors.toSet());
  }

  @Override
  public DataLoaderRegistry combine(DataLoaderRegistry registry) {
    throw new UnsupportedOperationException();
  }
}
