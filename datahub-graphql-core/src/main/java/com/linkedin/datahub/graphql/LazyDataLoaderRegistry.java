package com.linkedin.datahub.graphql;

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

  public LazyDataLoaderRegistry(
      QueryContext queryContext,
      Map<String, Function<QueryContext, DataLoader<?, ?>>> dataLoaderSuppliers) {
    super();
    this.queryContext = queryContext;
    this.dataLoaderSuppliers = new ConcurrentHashMap<>(dataLoaderSuppliers);
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
